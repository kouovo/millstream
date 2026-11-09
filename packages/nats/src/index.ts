import {
  type Deliver,
  type Features,
  type Logger,
  Message,
  type Publisher,
  PubSubClosedError,
  Semaphore,
  type Subscriber,
  anySignal,
  nopLogger,
  producerSubscription,
  sleep,
} from '@millstream/core';
import {
  AckPolicy,
  type ConsumerConfig,
  type ConsumerMessages,
  DeliverPolicy,
  type JetStreamClient,
  JetStreamApiError,
  type JetStreamManager,
  type JetStreamPublishOptions,
  type JsMsg,
  type StreamConfig,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { type MsgHdrs, type NatsConnection, headers as newHeaders, nanos } from '@nats-io/nats-core';

/** NATS header JetStream deduplicates on; millstream sets it to the message uuid. */
export const MSG_ID_HEADER = 'Nats-Msg-Id';
/** The header Go Watermill's NATS marshaler uses for the uuid — pass it as `uuidHeader` to interoperate. */
export const WATERMILL_UUID_HEADER = '_watermill_message_uuid';

/** `.`, `*`, `>`, whitespace, `/` and `\` are not allowed in stream and consumer names. */
export function sanitizeName(name: string): string {
  return name.replace(/[.*>\s/\\]/g, '_');
}

// ---------------------------------------------------------------- streams

export interface NatsStreamOptions {
  connection: NatsConnection;
  /** JetStream stream holding a topic (subject). Default: the topic, sanitized (`orders.created` → `orders_created`). */
  stream?: (topic: string) => string;
  /**
   * Create the stream on first use if it doesn't exist, with `subjects: [topic]` — like Watermill's
   * `AutoProvision`. Pass a function to add stream settings (retention, max_age, replicas, …).
   * Default `true`. Set `false` when streams are managed elsewhere (e.g. one `ORDERS` stream for `orders.>`).
   */
  autoProvision?: boolean | ((topic: string) => Partial<StreamConfig>);
  /** JetStream domain / API prefix and request timeouts. */
  jetStreamOptions?: Parameters<typeof jetstreamManager>[1];
}

class Streams {
  readonly #opts: NatsStreamOptions;
  readonly #ensured = new Map<string, Promise<string>>();
  #jsm?: Promise<JetStreamManager>;

  constructor(opts: NatsStreamOptions) {
    this.#opts = opts;
  }

  jsm(): Promise<JetStreamManager> {
    this.#jsm ??= jetstreamManager(this.#opts.connection, this.#opts.jetStreamOptions);
    this.#jsm.catch(() => (this.#jsm = undefined));
    return this.#jsm;
  }

  /** Stream name for a topic, created first when auto-provisioning. */
  ensure(topic: string): Promise<string> {
    let s = this.#ensured.get(topic);
    if (!s) {
      s = this.#provision(topic);
      s.catch(() => this.#ensured.delete(topic));
      this.#ensured.set(topic, s);
    }
    return s;
  }

  async #provision(topic: string): Promise<string> {
    const name = this.#opts.stream?.(topic) ?? sanitizeName(topic);
    const auto = this.#opts.autoProvision ?? true;
    if (auto === false) return name;
    const jsm = await this.jsm();
    try {
      await jsm.streams.info(name);
    } catch (error) {
      if (!(error instanceof JetStreamApiError && error.code === 10059)) throw error; // stream not found
      const extra = typeof auto === 'function' ? auto(topic) : {};
      await jsm.streams.add({ name, subjects: [topic], ...extra }).catch(async (e) => {
        await jsm.streams.info(name).catch(() => Promise.reject(e)); // lost a creation race: fine
      });
    }
    return name;
  }
}

// ---------------------------------------------------------------- marshaling

export interface NatsInfo {
  stream: string;
  /** Sequence of the message in its stream. */
  streamSequence: number;
  /** Delivery attempt (1 = first delivery). */
  deliveryCount: number;
  subject: string;
}

const infos = new WeakMap<Message, NatsInfo>();

/** JetStream coordinates of a consumed message (in-process only). */
export function natsInfo(msg: Message): NatsInfo | undefined {
  return infos.get(msg);
}

function toHeaders(msg: Message, uuidHeader: string | undefined): MsgHdrs {
  const h = newHeaders();
  for (const [k, v] of Object.entries(msg.metadata)) h.set(k, v);
  if (uuidHeader && uuidHeader !== MSG_ID_HEADER) h.set(uuidHeader, msg.uuid);
  return h;
}

function toMessage(m: JsMsg, uuidHeader: string): Message {
  const metadata: Record<string, string> = {};
  let uuid: string | undefined;
  for (const [k, values] of m.headers ?? []) {
    const v = values[values.length - 1] ?? '';
    if (k === uuidHeader) uuid = v;
    else if (k !== MSG_ID_HEADER) metadata[k] = v;
  }
  // Published without an id (other clients): the stream sequence is stable.
  const msg = new Message(uuid || `nats:${m.info.stream}:${m.seq}`, Buffer.from(m.data), metadata);
  infos.set(msg, {
    stream: m.info.stream,
    streamSequence: m.seq,
    deliveryCount: m.info.deliveryCount,
    subject: m.subject,
  });
  return msg;
}

// ---------------------------------------------------------------- Publisher

export interface NatsPublisherOptions extends NatsStreamOptions {
  /** Also write the uuid to this header (it is always the `Nats-Msg-Id`). */
  uuidHeader?: string;
  /** Extra publish options per message (expect, ttl, timeout, …). */
  publishOptions?: (topic: string, msg: Message) => Partial<JetStreamPublishOptions>;
}

/**
 * Publishes to JetStream and waits for every PubAck. `Nats-Msg-Id` = `msg.uuid`, so the stream drops
 * a re-published message within its `duplicate_window` (2 min by default). Metadata = headers.
 * The connection belongs to the caller: `close()` does not close it.
 */
export class NatsPublisher implements Publisher {
  readonly #opts: NatsPublisherOptions;
  readonly #streams: Streams;
  readonly #js: JetStreamClient;
  #closed = false;

  constructor(opts: NatsPublisherOptions) {
    this.#opts = opts;
    this.#streams = new Streams(opts);
    this.#js = jetstream(opts.connection, opts.jetStreamOptions);
  }

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('NatsPublisher');
    if (!messages.length) return;
    await this.#streams.ensure(topic);
    // Sent in order on one connection, so the stream keeps the order; acks are awaited together.
    await Promise.all(
      messages.map((m) =>
        this.#js.publish(topic, m.payload, {
          msgID: m.uuid,
          headers: toHeaders(m, this.#opts.uuidHeader),
          ...this.#opts.publishOptions?.(topic, m),
        }),
      ),
    );
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}

// ---------------------------------------------------------------- Subscriber

export interface NatsSubscriberOptions extends NatsStreamOptions {
  /**
   * Durable consumer shared by every subscriber of this group: each group gets every message,
   * subscribers of one group compete for them, and progress survives restarts.
   * Default: an ephemeral consumer per `subscribe()` that starts at the beginning of the stream.
   */
  consumerGroup?: string;
  /** Durable consumer name of a group. Default `<group>_<topic>`, sanitized. */
  durableName?: (topic: string, consumerGroup: string) => string;
  /** Where a new consumer starts. Default `'all'` (don't miss what was published before). */
  deliverPolicy?: 'all' | 'new';
  /** Unacked messages per consumer (`max_ack_pending`) — i.e. messages handled in parallel. Default 1. */
  concurrency?: number;
  /** Server redelivers an unacked message after this long; extended with `working()` while it's handled. Default 30s. */
  ackWaitMs?: number;
  /** `nak` delay for a nacked message. Default 100ms. */
  nackResendMs?: number;
  /** No answer within this time → treated as a nack. Default: wait forever. */
  ackDeadlineMs?: number | null;
  /** Extra consumer settings (max_deliver, backoff, filter_subjects, …). */
  consumerConfig?: Partial<ConsumerConfig>;
  /** Read the uuid from this header. Default `Nats-Msg-Id`. */
  uuidHeader?: string;
  /** Wait before re-subscribing after a failure. Default 1000ms. */
  reconnectMs?: number;
  logger?: Logger;
}

/**
 * Pull consumer per subscription. Ack → `+ACK` (confirmed). Nack → `-NAK` with `nackResendMs` delay.
 * Abort while holding a message → `-NAK` right away. `max_ack_pending = concurrency`, so with the
 * default of 1 a subscription is strictly ordered and holds the next message until the current one is acked.
 */
export class NatsSubscriber implements Subscriber {
  static readonly features: Features = {
    ordered: true, // per consumer, with concurrency 1
    persistent: true,
    consumerGroups: true,
    newSubscriberReceivesOldMessages: true, // with deliverPolicy 'all'
    exactlyOnceWithTx: false,
  };

  readonly #opts: NatsSubscriberOptions;
  readonly #streams: Streams;
  readonly #js: JetStreamClient;
  readonly #logger: Logger;
  readonly #closing = new AbortController();

  constructor(opts: NatsSubscriberOptions) {
    if (opts.consumerGroup !== undefined && !opts.consumerGroup) throw new Error('consumer group must be non-empty');
    this.#opts = opts;
    this.#streams = new Streams(opts);
    this.#js = jetstream(opts.connection, opts.jetStreamOptions);
    this.#logger = opts.logger ?? nopLogger;
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('NatsSubscriber');
    return producerSubscription(
      (deliver, stop) => this.#run(topic, deliver, stop),
      anySignal(signal, this.#closing.signal),
    );
  }

  async close(): Promise<void> {
    this.#closing.abort();
  }

  async #run(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    while (!stop.aborted) {
      try {
        await this.#consume(topic, deliver, stop);
      } catch (error) {
        if (stop.aborted) return;
        if (this.#opts.connection.isClosed()) throw error;
        this.#logger.error('nats subscription failed, re-subscribing', { topic, error });
        await sleep(this.#opts.reconnectMs ?? 1000, stop).catch(() => {});
      }
    }
  }

  async #consume(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const o = this.#opts;
    const stream = await this.#streams.ensure(topic);
    const jsm = await this.#streams.jsm();
    const concurrency = Math.max(1, o.concurrency ?? 1);
    const ackWaitMs = o.ackWaitMs ?? 30_000;
    const group = o.consumerGroup;
    const config: Partial<ConsumerConfig> = {
      ack_policy: AckPolicy.Explicit,
      deliver_policy: o.deliverPolicy === 'new' ? DeliverPolicy.New : DeliverPolicy.All,
      filter_subject: topic,
      ack_wait: nanos(ackWaitMs),
      max_ack_pending: concurrency,
      ...(group === undefined
        ? { inactive_threshold: nanos(Math.max(60_000, ackWaitMs * 2)) } // left over by a crash: cleaned up
        : { durable_name: sanitizeName(o.durableName?.(topic, group) ?? `${group}_${topic}`) }),
      ...o.consumerConfig,
    };
    const info = await jsm.consumers.add(stream, config).catch(async (error) => {
      // A durable consumer created with other settings (e.g. a new `concurrency`): update what can be.
      if (!(error instanceof JetStreamApiError && error.code === 10148 && config.durable_name)) throw error;
      const {
        durable_name,
        ack_policy: _a,
        deliver_policy: _d,
        opt_start_seq: _s,
        opt_start_time: _t,
        ...mutable
      } = config;
      return jsm.consumers.update(stream, durable_name, mutable);
    });
    const ephemeral = group === undefined;
    let messages: ConsumerMessages | undefined;
    const onAbort = () => void messages?.close().catch(() => {});
    stop.addEventListener('abort', onAbort, { once: true });
    try {
      const consumer = await this.#js.consumers.get(stream, info.name);
      messages = await consumer.consume({ max_messages: concurrency });
      if (stop.aborted) onAbort();
      const slots = new Semaphore(concurrency);
      const inflight = new Set<Promise<void>>();
      try {
        for await (const m of messages) {
          await slots.acquire();
          const h = this.#handle(m, deliver, stop, ackWaitMs)
            .catch((error) => this.#logger.error('nats settle failed', { error }))
            .finally(() => slots.release());
          inflight.add(h);
          void h.finally(() => inflight.delete(h));
          if (concurrency === 1) await h;
        }
      } finally {
        await Promise.allSettled(inflight);
      }
      if (!stop.aborted) throw new Error(`consumer ${info.name} stopped`);
    } finally {
      stop.removeEventListener('abort', onAbort);
      await messages?.close().catch(() => {});
      if (ephemeral) await jsm.consumers.delete(stream, info.name).catch(() => {});
    }
  }

  async #handle(m: JsMsg, deliver: Deliver, stop: AbortSignal, ackWaitMs: number): Promise<void> {
    const o = this.#opts;
    // Keep the server from redelivering while the message is held (also while it waits to be pulled).
    const keepAlive = setInterval(() => m.working(), Math.max(100, ackWaitMs / 2));
    try {
      if (stop.aborted) return m.nak();
      const r = await deliver(toMessage(m, o.uuidHeader ?? MSG_ID_HEADER), o.ackDeadlineMs ?? null);
      if (r === 'ack') {
        await m.ackAck(); // a lost ack would mean a redelivery
        return;
      }
      if (r === 'aborted' || stop.aborted) return m.nak();
      if (r === 'timeout') this.#logger.error('ack deadline exceeded', { seq: m.seq });
      m.nak(o.nackResendMs ?? 100);
    } finally {
      clearInterval(keepAlive);
    }
  }
}
