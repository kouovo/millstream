import {
  type Features,
  type Logger,
  Message,
  type Publisher,
  PubSubClosedError,
  Semaphore,
  type Subscriber,
  anySignal,
  isAbortError,
  newUUID,
  nopLogger,
  producerSubscription,
  sleep,
} from '@millstream/core';
import {
  Consumer,
  type ConsumeOptions,
  type ConsumerOptions,
  type Message as KafkaRecord,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  Producer,
  type ProducerOptions,
} from '@platformatic/kafka';

/** Default header carrying the message uuid. */
export const UUID_HEADER = '_millstream_message_uuid';
/** The header Go Watermill's Kafka marshaler uses — pass it as `uuidHeader` to interoperate. */
export const WATERMILL_UUID_HEADER = '_watermill_message_uuid';

type Bytes = Buffer;

export interface KafkaInfo {
  topic: string;
  partition: number;
  /** Decimal string (Kafka offsets are int64). */
  offset: string;
  key?: Buffer;
  timestamp: bigint;
}

const infos = new WeakMap<Message, KafkaInfo>();

/** Kafka coordinates of a consumed message (in-process only, not part of metadata). */
export function kafkaInfo(msg: Message): KafkaInfo | undefined {
  return infos.get(msg);
}

// ---------------------------------------------------------------- Publisher

/**
 * Sends are serialised per producer: with several idempotent sends in flight, one that
 * overtakes another's retry can land out of order or be appended twice
 * (platformatic/kafka#472, open as of 2.15). Earlier versions (< 2.3, #326) even dropped
 * concurrent sends silently. Keyed by producer so publishers sharing one producer are
 * serialised too. Batch messages into one `publish()` call for throughput.
 */
const sendChains = new WeakMap<object, Promise<unknown>>();
function serialized<T>(producer: object, fn: () => Promise<T>): Promise<T> {
  const run = (sendChains.get(producer) ?? Promise.resolve()).then(fn, fn);
  sendChains.set(
    producer,
    run.catch(() => {}),
  );
  return run;
}

export interface KafkaPublisherOptions {
  bootstrapBrokers: string[];
  clientId?: string;
  /** Extra producer options (acks, compression, …). `idempotent` defaults to true. */
  producer?: Partial<ProducerOptions<Bytes, Bytes, Bytes, Bytes>>;
  /**
   * Partition key. Messages with the same key go to the same partition and stay ordered.
   * Default: none (round-robin) — set it for anything that needs per-entity ordering.
   */
  partitionKey?: (topic: string, msg: Message) => string | Buffer | undefined;
  /** Header for the message uuid. Default `UUID_HEADER`; `WATERMILL_UUID_HEADER` to talk to Go Watermill. */
  uuidHeader?: string;
}

/** Publishes payload as the record value; metadata + uuid as headers. */
export class KafkaPublisher implements Publisher {
  readonly #producer: Producer<Bytes, Bytes, Bytes, Bytes>;
  readonly #ownsProducer: boolean;
  readonly #partitionKey?: KafkaPublisherOptions['partitionKey'];
  readonly #uuidHeader: Buffer;
  #closed = false;

  constructor(
    opts:
      | KafkaPublisherOptions
      | ({ producer: Producer<Bytes, Bytes, Bytes, Bytes> } & Pick<
          KafkaPublisherOptions,
          'partitionKey' | 'uuidHeader'
        >),
  ) {
    if ('bootstrapBrokers' in opts) {
      this.#producer = new Producer<Bytes, Bytes, Bytes, Bytes>({
        clientId: opts.clientId ?? 'millstream',
        bootstrapBrokers: opts.bootstrapBrokers,
        // A partially failed Produce request is retried as a whole, including partitions the broker
        // already acknowledged; without idempotence those records are appended again (observed on
        // 2.15 right after topic creation). The broker dedupes idempotent resends.
        idempotent: true,
        ...opts.producer,
      } as ProducerOptions<Bytes, Bytes, Bytes, Bytes>);
      this.#ownsProducer = true;
    } else {
      this.#producer = opts.producer;
      this.#ownsProducer = false;
    }
    this.#partitionKey = opts.partitionKey;
    this.#uuidHeader = Buffer.from(opts.uuidHeader ?? UUID_HEADER);
  }

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('KafkaPublisher');
    if (!messages.length) return;
    const records = messages.map((m) => {
      const key = this.#partitionKey?.(topic, m);
      const headers = new Map<Buffer, Buffer>([[this.#uuidHeader, Buffer.from(m.uuid)]]);
      for (const [k, v] of Object.entries(m.metadata)) headers.set(Buffer.from(k), Buffer.from(v));
      return {
        topic,
        value: m.payload,
        headers,
        ...(key === undefined ? {} : { key: typeof key === 'string' ? Buffer.from(key) : key }),
      };
    });
    await serialized(this.#producer, () => this.#producer.send({ messages: records }));
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#ownsProducer) await this.#producer.close();
  }
}

// ---------------------------------------------------------------- Subscriber

export interface KafkaSubscriberOptions {
  bootstrapBrokers: string[];
  /** Kafka consumer group. Default: a fresh unique group per `subscribe()` (every subscription sees everything). */
  consumerGroup?: string;
  clientId?: string;
  /** Where a group without committed offsets starts. Default `'earliest'` (don't miss what was published before). */
  fallbackMode?: 'earliest' | 'latest';
  /**
   * Partitions processed in parallel within one subscription. Each partition is still
   * strictly sequential (commit order = offset order). Default 1: fully sequential.
   */
  partitionConcurrency?: number;
  /** Wait before redelivering a nacked message. Default 100ms. */
  nackResendMs?: number;
  /** No answer within this time → redeliver (like a nack). Default `null`: wait forever. */
  ackDeadlineMs?: number | null;
  /** Extra consumer options (session timeout, rack, SASL, …). */
  consumer?: Partial<ConsumerOptions<Bytes, Bytes, Bytes, Bytes>>;
  /** Extra `consume()` options (maxWaitTime, highWaterMark, …). */
  consume?: Partial<ConsumeOptions<Bytes, Bytes, Bytes, Bytes>>;
  /** Header carrying the message uuid. Default `UUID_HEADER`; `WATERMILL_UUID_HEADER` to read Go Watermill's. */
  uuidHeader?: string;
  logger?: Logger;
}

/**
 * Kafka subscriber (Watermill semantics):
 * - **Ack → commit** `offset + 1` for that partition.
 * - **Nack → in-place redelivery** of a fresh copy after `nackResendMs`. Kafka has no per-message
 *   nack, so a poison message blocks its partition: put `poisonQueue` / `retry` on the Router.
 * - Unanswered messages are never committed; on restart/rebalance they are delivered again
 *   (at-least-once — dedupe downstream by `msg.uuid`).
 */
export class KafkaSubscriber implements Subscriber {
  static readonly features: Features = {
    ordered: true, // per partition
    persistent: true,
    consumerGroups: true,
    newSubscriberReceivesOldMessages: true, // with fallbackMode 'earliest'
    exactlyOnceWithTx: false,
  };

  readonly #opts: KafkaSubscriberOptions;
  readonly #logger: Logger;
  readonly #closing = new AbortController();

  constructor(opts: KafkaSubscriberOptions) {
    this.#opts = opts;
    this.#logger = opts.logger ?? nopLogger;
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('KafkaSubscriber');
    return producerSubscription(
      (deliver, stop) => this.#run(topic, deliver, stop),
      anySignal(signal, this.#closing.signal),
    );
  }

  async close(): Promise<void> {
    this.#closing.abort();
  }

  async #run(
    topic: string,
    deliver: (msg: Message, deadline?: number | null) => Promise<string>,
    stop: AbortSignal,
  ): Promise<void> {
    const o = this.#opts;
    const consumer = new Consumer<Bytes, Bytes, Bytes, Bytes>({
      clientId: o.clientId ?? 'millstream',
      groupId: o.consumerGroup ?? `millstream-${newUUID()}`,
      bootstrapBrokers: o.bootstrapBrokers,
      ...o.consumer,
    } as ConsumerOptions<Bytes, Bytes, Bytes, Bytes>);
    try {
      const stream = await consumer.consume({
        topics: [topic],
        autocommit: false,
        mode: MessagesStreamModes.COMMITTED,
        fallbackMode:
          o.fallbackMode === 'latest' ? MessagesStreamFallbackModes.LATEST : MessagesStreamFallbackModes.EARLIEST,
        ...o.consume,
      });
      const onAbort = () => void stream.close().catch(() => {});
      stop.addEventListener('abort', onAbort, { once: true });
      if (stop.aborted) onAbort();

      const handle = (record: KafkaRecord<Bytes, Bytes, Bytes, Bytes>) => this.#handle(record, deliver, stop);
      const concurrency = o.partitionConcurrency ?? 1;
      try {
        if (concurrency <= 1) {
          for await (const record of stream) {
            if (stop.aborted || !(await handle(record))) break;
          }
        } else {
          await this.#runLanes(stream, handle, concurrency, stop);
        }
      } finally {
        stop.removeEventListener('abort', onAbort);
        await stream.close().catch(() => {});
      }
    } catch (error) {
      if (!stop.aborted && !isAbortError(error)) throw error;
    } finally {
      // `force`: also tear down streams still open; the overload with `force` alone is callback-style.
      await new Promise<void>((resolve) =>
        consumer.close(true, (error) => {
          if (error) this.#logger.error('kafka consumer close failed', { error });
          resolve();
        }),
      );
    }
  }

  /** One sequential lane per partition; at most `concurrency` records buffered across lanes. */
  async #runLanes(
    stream: AsyncIterable<KafkaRecord<Bytes, Bytes, Bytes, Bytes>>,
    handle: (r: KafkaRecord<Bytes, Bytes, Bytes, Bytes>) => Promise<boolean>,
    concurrency: number,
    stop: AbortSignal,
  ): Promise<void> {
    const slots = new Semaphore(concurrency);
    const lanes = new Map<number, Promise<boolean>>();
    for await (const record of stream) {
      if (stop.aborted) break;
      await slots.acquire();
      const previous = lanes.get(record.partition) ?? Promise.resolve(true);
      // A lane that stopped (abort) must not process later records of its partition.
      const next = previous.then((ok) => (ok && !stop.aborted ? handle(record) : false)).finally(() => slots.release());
      lanes.set(record.partition, next);
    }
    await Promise.allSettled(lanes.values());
  }

  /** Deliver until acked; returns false when the subscription is stopping. */
  async #handle(
    record: KafkaRecord<Bytes, Bytes, Bytes, Bytes>,
    deliver: (msg: Message, deadline?: number | null) => Promise<string>,
    stop: AbortSignal,
  ): Promise<boolean> {
    let msg = toMessage(record, this.#opts.uuidHeader ?? UUID_HEADER);
    for (;;) {
      const r = await deliver(msg, this.#opts.ackDeadlineMs ?? null);
      if (r === 'ack') {
        try {
          await record.commit();
        } catch (error) {
          // Typically a rebalance took the partition away: the new owner redelivers it.
          this.#logger.error('kafka commit failed', { topic: record.topic, partition: record.partition, error });
        }
        return true;
      }
      if (r === 'aborted' || stop.aborted) return false;
      if (r === 'timeout') this.#logger.error('ack deadline exceeded, redelivering', { uuid: msg.uuid });
      try {
        await sleep(this.#opts.nackResendMs ?? 100, stop);
      } catch {
        return false;
      }
      msg = msg.copy();
      infos.set(msg, infoOf(record));
    }
  }
}

function infoOf(r: KafkaRecord<Bytes, Bytes, Bytes, Bytes>): KafkaInfo {
  return {
    topic: r.topic,
    partition: r.partition,
    offset: r.offset.toString(),
    timestamp: r.timestamp,
    ...(r.key && r.key.length ? { key: r.key } : {}),
  };
}

function toMessage(r: KafkaRecord<Bytes, Bytes, Bytes, Bytes>, uuidHeader: string): Message {
  const metadata: Record<string, string> = {};
  let uuid: string | undefined;
  for (const [k, v] of r.headers) {
    const key = k.toString();
    if (key === uuidHeader) uuid = v.toString();
    else metadata[key] = v?.toString() ?? '';
  }
  // Records not produced by millstream: a deterministic id keeps downstream dedup working.
  const msg = new Message(uuid ?? `kafka:${r.topic}:${r.partition}:${r.offset}`, r.value ?? Buffer.alloc(0), metadata);
  infos.set(msg, infoOf(r));
  return msg;
}
