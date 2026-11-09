import {
  type Deliver,
  type Features,
  type Logger,
  Message,
  type Publisher,
  PubSubClosedError,
  type Subscriber,
  anySignal,
  newUUID,
  nopLogger,
  producerSubscription,
  sleep,
} from '@millstream/core';
import {
  type Channel,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Options,
  type SocketOptions,
  connect,
} from 'amqplib';

/** The header Go Watermill's AMQP marshaler uses for the uuid — pass it as `uuidHeader` to interoperate. */
export const WATERMILL_UUID_HEADER = '_watermill_message_uuid';

// ---------------------------------------------------------------- connection

/** Connect with a URL (the publisher / subscriber owns and closes the connection) or share your own. */
export type RabbitMQConnection =
  | { url: string | Options.Connect; socketOptions?: SocketOptions; connection?: never }
  | { connection: ChannelModel; url?: never; socketOptions?: never };

/** A lazily opened connection: owned (reopened after a failure) or borrowed (used as is). */
class ConnectionHolder {
  readonly #cfg: RabbitMQConnection;
  readonly #logger: Logger;
  #conn?: Promise<ChannelModel>;

  constructor(cfg: RabbitMQConnection, logger: Logger) {
    this.#cfg = cfg;
    this.#logger = logger;
  }

  get(): Promise<ChannelModel> {
    if (this.#cfg.connection) return Promise.resolve(this.#cfg.connection);
    if (!this.#conn) {
      const conn = connect(this.#cfg.url!, this.#cfg.socketOptions).then((c) => {
        c.on('error', (error) => this.#logger.error('rabbitmq connection error', { error }));
        c.on('close', () => {
          if (this.#conn === conn) this.#conn = undefined; // reconnect on next use
        });
        return c;
      });
      conn.catch(() => {
        if (this.#conn === conn) this.#conn = undefined;
      });
      this.#conn = conn;
    }
    return this.#conn;
  }

  async close(): Promise<void> {
    const conn = this.#conn;
    this.#conn = undefined;
    if (conn) await (await conn.catch(() => undefined))?.close().catch(() => {});
  }
}

// ---------------------------------------------------------------- topology

export class InvalidConsumerGroupError extends Error {
  constructor(group: string) {
    super(`invalid consumer group "${group}": must be non-empty`);
    this.name = 'InvalidConsumerGroupError';
  }
}

/** Naming and declaration of exchanges / queues, shared by publisher and subscriber. */
export interface RabbitMQTopology {
  /** Exchange a topic is published to. Default: the topic itself. */
  exchangeName?: (topic: string) => string;
  /** Exchange type. Default `'fanout'`: every bound queue (consumer group) gets every message. */
  exchangeType?: 'fanout' | 'topic' | 'direct' | 'headers' | (string & {});
  /** `assertExchange` options. Default `{ durable: true }`. */
  exchangeOptions?: Options.AssertExchange;
  /** Queue of a topic and consumer group. Default: `<topic>` without a group, `<topic>.<group>` with one. */
  queueName?: (topic: string, consumerGroup?: string) => string;
  /**
   * `assertQueue` options (quorum queues, dead-lettering, TTL, …), merged over `{ durable: true }`.
   * Publisher and subscribers must agree, or RabbitMQ rejects the declaration (PRECONDITION_FAILED).
   */
  queueOptions?: (topic: string, consumerGroup?: string) => Options.AssertQueue;
  /** Binding key between exchange and queue (ignored by fanout). Default: the topic. */
  bindingKey?: (topic: string, consumerGroup?: string) => string;
}

export function defaultQueueName(topic: string, consumerGroup?: string): string {
  if (consumerGroup === undefined) return topic;
  if (!consumerGroup) throw new InvalidConsumerGroupError(consumerGroup);
  return `${topic}.${consumerGroup}`;
}

async function declare(
  ch: Channel,
  t: RabbitMQTopology,
  topic: string,
  groups: ReadonlyArray<string | undefined>,
): Promise<{ exchange: string; queues: string[] }> {
  const exchange = t.exchangeName?.(topic) ?? topic;
  await ch.assertExchange(exchange, t.exchangeType ?? 'fanout', { durable: true, ...t.exchangeOptions });
  const queues: string[] = [];
  for (const group of groups) {
    const queue = t.queueName?.(topic, group) ?? defaultQueueName(topic, group);
    await ch.assertQueue(queue, { durable: true, ...t.queueOptions?.(topic, group) });
    await ch.bindQueue(queue, exchange, t.bindingKey?.(topic, group) ?? topic);
    queues.push(queue);
  }
  return { exchange, queues };
}

// ---------------------------------------------------------------- marshaling

export interface RabbitMQInfo {
  exchange: string;
  routingKey: string;
  deliveryTag: number;
  /** Set by RabbitMQ when the message was delivered before (nack/requeue, consumer crash). */
  redelivered: boolean;
}

const infos = new WeakMap<Message, RabbitMQInfo>();

/** AMQP delivery details of a consumed message (in-process only). */
export function rabbitmqInfo(msg: Message): RabbitMQInfo | undefined {
  return infos.get(msg);
}

function headerValue(v: unknown): string {
  if (typeof v === 'string') return v;
  if (Buffer.isBuffer(v)) return v.toString();
  if (v !== null && typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

function toMessage(raw: ConsumeMessage, uuidHeader: string | undefined): Message {
  const metadata: Record<string, string> = {};
  let uuid: string | undefined;
  for (const [k, v] of Object.entries(raw.properties.headers ?? {})) {
    if (v === undefined) continue;
    if (uuidHeader !== undefined && k === uuidHeader) uuid = headerValue(v);
    else metadata[k] = headerValue(v);
  }
  uuid ??=
    typeof raw.properties.messageId === 'string' && raw.properties.messageId ? raw.properties.messageId : undefined;
  // Messages published without an id (other clients): nothing stable to derive one from.
  const msg = new Message(uuid ?? `amqp:${newUUID()}`, raw.content, metadata);
  infos.set(msg, {
    exchange: raw.fields.exchange,
    routingKey: raw.fields.routingKey,
    deliveryTag: raw.fields.deliveryTag,
    redelivered: raw.fields.redelivered,
  });
  return msg;
}

// ---------------------------------------------------------------- Publisher

export type RabbitMQPublisherOptions = RabbitMQConnection &
  RabbitMQTopology & {
    /**
     * Queues declared (and bound) before the first publish to a topic, so messages published
     * before a subscriber exists are kept — RabbitMQ drops what no queue is bound for.
     *
     * - returns `undefined` (default) → the topic's shared queue (`queueName(topic)`);
     * - returns `['billing', …]` → one queue per consumer group (`queueName(topic, group)`);
     * - returns `[]` → declare nothing (plain pub/sub: only running subscribers' queues receive).
     */
    consumerGroups?: (topic: string) => readonly string[] | undefined;
    /** Routing key of a message. Default: the topic. */
    routingKey?: (topic: string, msg: Message) => string;
    /** Extra publish properties per message (priority, expiration, correlationId, …). */
    publishOptions?: (topic: string, msg: Message) => Options.Publish;
    /** Write the uuid to this header too (it is always the AMQP `messageId`). */
    uuidHeader?: string;
    logger?: Logger;
  };

/**
 * Publishes on a confirm channel: `publish()` resolves once RabbitMQ has taken responsibility
 * for every message (persisted for durable queues). Messages are persistent; uuid = `messageId`,
 * metadata = headers.
 */
export class RabbitMQPublisher implements Publisher {
  readonly #opts: RabbitMQPublisherOptions;
  readonly #conn: ConnectionHolder;
  readonly #logger: Logger;
  readonly #declared = new Map<string, Promise<string>>();
  #channel?: Promise<ConfirmChannel>;
  #closed = false;

  constructor(opts: RabbitMQPublisherOptions) {
    this.#opts = opts;
    this.#logger = opts.logger ?? nopLogger;
    this.#conn = new ConnectionHolder(opts, this.#logger);
  }

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('RabbitMQPublisher');
    if (!messages.length) return;
    const ch = await this.#getChannel();
    const exchange = await this.#declare(ch, topic);
    const confirms = messages.map(
      (m) =>
        new Promise<void>((resolve, reject) => {
          const headers: Record<string, string> = { ...m.metadata };
          if (this.#opts.uuidHeader) headers[this.#opts.uuidHeader] = m.uuid;
          ch.publish(
            exchange,
            this.#opts.routingKey?.(topic, m) ?? topic,
            m.payload,
            { persistent: true, messageId: m.uuid, headers, ...this.#opts.publishOptions?.(topic, m) },
            (err) =>
              err ? reject(err instanceof Error ? err : new Error(`message ${m.uuid} nacked by broker`)) : resolve(),
          );
        }),
    );
    await Promise.all(confirms);
  }

  #getChannel(): Promise<ConfirmChannel> {
    if (!this.#channel) {
      const channel = this.#conn.get().then(async (conn) => {
        const ch = await conn.createConfirmChannel();
        ch.on('error', (error) => this.#logger.error('rabbitmq publisher channel error', { error }));
        ch.on('close', () => {
          if (this.#channel === channel) this.#channel = undefined;
          this.#declared.clear(); // a new connection may face a reset broker
        });
        return ch;
      });
      channel.catch(() => {
        if (this.#channel === channel) this.#channel = undefined;
      });
      this.#channel = channel;
    }
    return this.#channel;
  }

  #declare(ch: Channel, topic: string): Promise<string> {
    let d = this.#declared.get(topic);
    if (!d) {
      const groups = this.#opts.consumerGroups?.(topic);
      d = declare(ch, this.#opts, topic, groups ? [...new Set(groups)] : [undefined]).then((r) => r.exchange);
      d.catch(() => this.#declared.delete(topic));
      this.#declared.set(topic, d);
    }
    return d;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const ch = await this.#channel?.catch(() => undefined);
    await ch?.close().catch(() => {});
    await this.#conn.close();
  }
}

// ---------------------------------------------------------------- Subscriber

export type RabbitMQSubscriberOptions = RabbitMQConnection &
  RabbitMQTopology & {
    /**
     * Consume the queue of this group (`queueName(topic, group)`), bound to the topic's exchange.
     * Each group gets every message; subscribers of the same group compete for them.
     * Default: the topic's shared queue (`queueName(topic)`).
     */
    consumerGroup?: string;
    /** Unacked messages per subscription (`prefetch`) — i.e. messages handled in parallel. Default 1. */
    concurrency?: number;
    /**
     * What a Nack (or ack-deadline timeout) does:
     * - `'requeue'` (default): back to the queue after `nackResendMs`; retry policy lives in Router middlewares;
     * - `'reject'`: rejected without requeue → dead-lettered when the queue has a `x-dead-letter-exchange`
     *   (set via `queueOptions`), otherwise **dropped**. Like Watermill AMQP's `NoRequeueOnNack`.
     */
    nack?: 'requeue' | 'reject';
    /** Wait before requeueing a nacked message. Default 100ms. */
    nackResendMs?: number;
    /**
     * No answer within this time → treated as a nack. Default: wait forever — but RabbitMQ's
     * `consumer_timeout` (30 min by default) closes the channel of a consumer that holds a message longer.
     */
    ackDeadlineMs?: number | null;
    /** Extra `consume` options (priority, exclusive, arguments, …). */
    consumeOptions?: Options.Consume;
    /** Read the uuid from this header (falling back to `messageId`). */
    uuidHeader?: string;
    /** Wait before re-subscribing after a channel / connection failure. Default 1000ms. */
    reconnectMs?: number;
    logger?: Logger;
  };

/**
 * One channel + consumer per subscription. Ack → `basic.ack`. Nack → requeue (or reject).
 * Abort while holding a message → requeued immediately. Messages unacked when the channel
 * or connection dies are requeued by RabbitMQ (at-least-once — dedupe by `msg.uuid`).
 * Channel / connection failures are logged and the subscription re-established.
 */
export class RabbitMQSubscriber implements Subscriber {
  static readonly features: Features = {
    ordered: true, // per queue, with one consumer and concurrency 1
    persistent: true,
    consumerGroups: true,
    newSubscriberReceivesOldMessages: true, // for queues already declared (see the publisher's consumerGroups)
    exactlyOnceWithTx: false,
  };

  readonly #opts: RabbitMQSubscriberOptions;
  readonly #conn: ConnectionHolder;
  readonly #logger: Logger;
  readonly #closing = new AbortController();
  readonly #runs = new Set<Promise<void>>();

  constructor(opts: RabbitMQSubscriberOptions) {
    if (opts.consumerGroup !== undefined) defaultQueueName('', opts.consumerGroup); // validate early
    this.#opts = opts;
    this.#logger = opts.logger ?? nopLogger;
    this.#conn = new ConnectionHolder(opts, this.#logger);
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('RabbitMQSubscriber');
    return producerSubscription(
      (deliver, stop) => {
        const run = this.#run(topic, deliver, stop);
        this.#runs.add(run);
        return run.finally(() => this.#runs.delete(run));
      },
      anySignal(signal, this.#closing.signal),
    );
  }

  async close(): Promise<void> {
    if (this.#closing.signal.aborted) return;
    this.#closing.abort();
    await Promise.allSettled(this.#runs);
    await this.#conn.close();
  }

  async #run(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    while (!stop.aborted) {
      try {
        await this.#consume(topic, deliver, stop);
      } catch (error) {
        if (stop.aborted) return;
        this.#logger.error('rabbitmq subscription failed, re-subscribing', { topic, error });
        // A borrowed connection that is gone can't come back.
        if (this.#opts.connection && isConnectionClosed(error)) throw error;
        await sleep(this.#opts.reconnectMs ?? 1000, stop).catch(() => {});
      }
    }
  }

  /** One channel's lifetime: resolves on abort, rejects when the channel / connection fails. */
  async #consume(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const o = this.#opts;
    const conn = await this.#conn.get();
    const ch = await conn.createChannel();
    const inflight = new Set<Promise<void>>();
    let fail: ((e: unknown) => void) | undefined;
    const failed = new Promise<never>((_, reject) => (fail = reject));
    failed.catch(() => {});
    ch.on('error', (error) => fail?.(error));
    ch.on('close', () => fail?.(new Error('channel closed')));
    try {
      const { queues } = await declare(ch, o, topic, [o.consumerGroup]);
      await ch.prefetch(Math.max(1, o.concurrency ?? 1));
      const consumer = await ch.consume(
        queues[0]!,
        (raw) => {
          if (!raw) return fail?.(new Error(`consumer cancelled by the broker (queue ${queues[0]} deleted?)`));
          const h = this.#handle(ch, raw, deliver, stop).catch((error) =>
            this.#logger.error('rabbitmq settle failed', { error }),
          );
          inflight.add(h);
          void h.finally(() => inflight.delete(h));
        },
        { noAck: false, ...o.consumeOptions },
      );
      const stopped = new Promise<void>((resolve) => {
        if (stop.aborted) resolve();
        else stop.addEventListener('abort', () => resolve(), { once: true });
      });
      await Promise.race([stopped, failed]);
      await ch.cancel(consumer.consumerTag).catch(() => {});
      await Promise.allSettled(inflight); // held messages are nacked (requeued) on abort
    } finally {
      await ch.close().catch(() => {});
    }
  }

  async #handle(ch: Channel, raw: ConsumeMessage, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const o = this.#opts;
    const r = await deliver(toMessage(raw, o.uuidHeader), o.ackDeadlineMs ?? null);
    if (r === 'ack') return ch.ack(raw);
    if (r === 'aborted' || stop.aborted) return ch.nack(raw, false, true);
    if (r === 'timeout') this.#logger.error('ack deadline exceeded', { messageId: raw.properties.messageId });
    if (o.nack === 'reject') return ch.nack(raw, false, false);
    await sleep(o.nackResendMs ?? 100, stop).catch(() => {});
    ch.nack(raw, false, true);
  }
}

function isConnectionClosed(error: unknown): boolean {
  return error instanceof Error && /connection (closed|ended)/i.test(error.message);
}
