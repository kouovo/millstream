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
import type { Redis } from 'ioredis';

/** Stream entry fields written by the publisher. */
export const FIELD_UUID = 'uuid';
export const FIELD_PAYLOAD = 'payload';
export const FIELD_METADATA = 'metadata';

export interface RedisStreamInfo {
  stream: string;
  /** Entry id (`<ms>-<seq>`). */
  id: string;
  /** Consumer group reading it (unset for ungrouped subscriptions). */
  consumerGroup?: string;
}

const infos = new WeakMap<Message, RedisStreamInfo>();

/** Stream coordinates of a consumed message (in-process only). */
export function redisStreamInfo(msg: Message): RedisStreamInfo | undefined {
  return infos.get(msg);
}

type RawEntry = [id: Buffer, fields: Buffer[] | null];

function toMessage(stream: string, [rawId, fields]: RawEntry, consumerGroup: string | undefined): Message {
  const id = rawId.toString();
  const f = new Map<string, Buffer>();
  if (fields) for (let i = 0; i + 1 < fields.length; i += 2) f.set(fields[i]!.toString(), fields[i + 1]!);
  let payload = f.get(FIELD_PAYLOAD);
  let metadata: Record<string, string> = {};
  const rawMeta = f.get(FIELD_METADATA);
  if (rawMeta) metadata = JSON.parse(rawMeta.toString()) as Record<string, string>;
  if (!payload) {
    // An entry added by another client: its fields become the payload (as JSON).
    payload = Buffer.from(JSON.stringify(Object.fromEntries([...f].map(([k, v]) => [k, v.toString()]))));
  }
  const msg = new Message(f.get(FIELD_UUID)?.toString() || `redis:${stream}:${id}`, payload, metadata);
  infos.set(msg, { stream, id, ...(consumerGroup === undefined ? {} : { consumerGroup }) });
  return msg;
}

// ---------------------------------------------------------------- Publisher

export interface RedisStreamPublisherOptions {
  /** ioredis client (yours: `close()` doesn't quit it). */
  client: Redis;
  /** Stream key of a topic. Default: the topic. */
  streamName?: (topic: string) => string;
  /**
   * Trim the stream to about this many entries on every add (`MAXLEN ~ n`). Default: no trimming.
   * Trimmed entries are gone even for consumer groups that haven't read them yet.
   */
  maxLen?: number | ((topic: string) => number | undefined);
}

/** `XADD` per message in one `MULTI`: entries land atomically and in order. Fields: uuid, payload, metadata (JSON). */
export class RedisStreamPublisher implements Publisher {
  readonly #opts: RedisStreamPublisherOptions;
  #closed = false;

  constructor(opts: RedisStreamPublisherOptions) {
    this.#opts = opts;
  }

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('RedisStreamPublisher');
    if (!messages.length) return;
    const stream = this.#opts.streamName?.(topic) ?? topic;
    const maxLen = typeof this.#opts.maxLen === 'function' ? this.#opts.maxLen(topic) : this.#opts.maxLen;
    const trim = maxLen === undefined ? [] : ['MAXLEN', '~', String(maxLen)];
    const tx = this.#opts.client.multi();
    for (const m of messages) {
      tx.call(
        'XADD',
        stream,
        ...trim,
        '*',
        FIELD_UUID,
        m.uuid,
        FIELD_PAYLOAD,
        m.payload,
        FIELD_METADATA,
        JSON.stringify(m.metadata),
      );
    }
    const results = await tx.exec();
    const failed = results?.find(([error]) => error);
    if (!results || failed) throw failed?.[0] ?? new Error('redis transaction aborted');
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}

// ---------------------------------------------------------------- Subscriber

export interface RedisStreamSubscriberOptions {
  /** ioredis client for acks / claims (yours). Each subscription also opens a `duplicate()` for blocking reads. */
  client: Redis;
  /** Stream key of a topic. Default: the topic. */
  streamName?: (topic: string) => string;
  /**
   * Read through this consumer group (`XREADGROUP`): each group gets every message, subscribers of one group
   * compete for them, and progress (the group's last id + pending entries) lives in Redis.
   * Default: no group — `XREAD` from `startId`, progress kept in memory (every subscription sees everything).
   */
  consumerGroup?: string;
  /** Consumer name within the group. Default: a fresh one per subscription. */
  consumerName?: string;
  /** Where a new group (or an ungrouped subscription) starts: `'0'` (everything, default) or `'$'` (only new entries). */
  startId?: '0' | '$';
  /** Entries handled in parallel (consumer groups only; ungrouped reads are sequential). Default 1. */
  concurrency?: number;
  /** `BLOCK` timeout of a read. Default 2000ms. */
  blockMs?: number;
  /**
   * Pending entries idle this long belong to a consumer presumed dead and are claimed (`XAUTOCLAIM`).
   * Entries being handled are kept fresh, so this only has to exceed a network hiccup. Default 30s.
   */
  claimIdleMs?: number;
  /** How often to look for claimable entries. Default `claimIdleMs / 2`. */
  claimIntervalMs?: number;
  /** Wait before redelivering a nacked message (in place). Default 100ms. */
  nackResendMs?: number;
  /** No answer within this time → redeliver (like a nack). Default `null`: wait forever. */
  ackDeadlineMs?: number | null;
  logger?: Logger;
}

/**
 * Redis Streams subscriber (Watermill `redisstream` semantics):
 * - **Ack → `XACK`**. **Nack → in-place redelivery** of a fresh copy after `nackResendMs`; the entry stays
 *   pending, so a poison message blocks its subscription — put `poisonQueue` / `retry` on the Router.
 * - Entries of crashed consumers are claimed after `claimIdleMs`; on abort, held entries are released
 *   right away (made claimable) for the next subscriber of the group.
 */
export class RedisStreamSubscriber implements Subscriber {
  static readonly features: Features = {
    ordered: true, // with concurrency 1 and one consumer per group (claims after a crash can reorder)
    persistent: true,
    consumerGroups: true,
    newSubscriberReceivesOldMessages: true, // with startId '0'
    exactlyOnceWithTx: false,
  };

  readonly #opts: RedisStreamSubscriberOptions;
  readonly #logger: Logger;
  readonly #closing = new AbortController();

  constructor(opts: RedisStreamSubscriberOptions) {
    if (opts.consumerGroup !== undefined && !opts.consumerGroup) throw new Error('consumer group must be non-empty');
    this.#opts = opts;
    this.#logger = opts.logger ?? nopLogger;
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('RedisStreamSubscriber');
    return producerSubscription(
      (deliver, stop) => this.#run(topic, deliver, stop),
      anySignal(signal, this.#closing.signal),
    );
  }

  async close(): Promise<void> {
    this.#closing.abort();
  }

  async #run(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const stream = this.#opts.streamName?.(topic) ?? topic;
    // Blocking reads get their own connection; aborting disconnects it, which ends a pending BLOCK.
    const reader = this.#opts.client.duplicate({ maxRetriesPerRequest: null });
    reader.on('error', (error) => {
      if (!stop.aborted) this.#logger.error('redis reader connection error', { error });
    });
    const onAbort = () => reader.disconnect();
    stop.addEventListener('abort', onAbort, { once: true });
    try {
      if (this.#opts.consumerGroup === undefined) await this.#runUngrouped(stream, reader, deliver, stop);
      else await this.#runGroup(stream, this.#opts.consumerGroup, reader, deliver, stop);
    } finally {
      stop.removeEventListener('abort', onAbort);
      reader.disconnect();
    }
  }

  /** Read, retrying transient failures until aborted. */
  async #read(reader: Redis, args: Array<string | number>, stop: AbortSignal): Promise<RawEntry[] | null> {
    try {
      const r = (await reader.callBuffer(...(args as [string, ...string[]]))) as Array<[Buffer, RawEntry[]]> | null;
      return r?.[0]?.[1] ?? [];
    } catch (error) {
      if (stop.aborted) return null;
      this.#logger.error('redis stream read failed', { error });
      await sleep(1000, stop).catch(() => {});
      return stop.aborted ? null : [];
    }
  }

  async #runUngrouped(stream: string, reader: Redis, deliver: Deliver, stop: AbortSignal): Promise<void> {
    let last = '0-0';
    if (this.#opts.startId === '$') {
      const tail = (await this.#opts.client.xrevrange(stream, '+', '-', 'COUNT', 1)) as Array<[string, string[]]>;
      last = tail[0]?.[0] ?? '0-0';
    }
    const block = this.#opts.blockMs ?? 2000;
    while (!stop.aborted) {
      const entries = await this.#read(reader, ['XREAD', 'COUNT', 100, 'BLOCK', block, 'STREAMS', stream, last], stop);
      if (!entries) return;
      for (const entry of entries) {
        if (!(await this.#deliverUntilAcked(stream, entry, undefined, deliver, stop))) return;
        last = entry[0].toString();
      }
    }
  }

  async #runGroup(stream: string, group: string, reader: Redis, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const o = this.#opts;
    const client = o.client;
    const consumer = o.consumerName ?? `millstream-${newUUID()}`;
    const claimIdleMs = o.claimIdleMs ?? 30_000;
    const claimIntervalMs = o.claimIntervalMs ?? claimIdleMs / 2;
    const concurrency = Math.max(1, o.concurrency ?? 1);
    const block = o.blockMs ?? 2000;

    await client.xgroup('CREATE', stream, group, o.startId ?? '0', 'MKSTREAM').catch((error: Error) => {
      if (!error.message.includes('BUSYGROUP')) throw error;
    });

    const inflight = new Set<Promise<void>>();
    let freed: (() => void) | undefined;
    const start = (entry: RawEntry) => {
      const h = this.#handleGroupEntry(stream, group, consumer, entry, claimIdleMs, deliver, stop)
        .catch((error) => this.#logger.error('redis stream settle failed', { stream, error }))
        .finally(() => {
          inflight.delete(h);
          freed?.();
        });
      inflight.add(h);
      return h;
    };

    let nextClaim = 0;
    let claimCursor = '0-0';
    try {
      while (!stop.aborted) {
        if (inflight.size >= concurrency) {
          await new Promise<void>((resolve) => (freed = resolve));
          continue;
        }
        const free = concurrency - inflight.size;
        let entries: RawEntry[] | null = [];
        if (Date.now() >= nextClaim) {
          // Entries left pending by a crashed (or stopped) consumer of the group.
          try {
            const r = (await client.callBuffer(
              'XAUTOCLAIM',
              stream,
              group,
              consumer,
              String(claimIdleMs),
              claimCursor,
              'COUNT',
              String(free),
            )) as [Buffer, RawEntry[], Buffer[]?];
            claimCursor = r[0].toString();
            entries = r[1];
            if (claimCursor === '0-0') nextClaim = Date.now() + claimIntervalMs;
          } catch (error) {
            if (stop.aborted) return;
            this.#logger.error('redis XAUTOCLAIM failed', { stream, error });
            nextClaim = Date.now() + claimIntervalMs;
          }
          // Redis < 7 returns deleted (trimmed) entries with null fields: drop them from the PEL.
          const gone = entries.filter(([, fields]) => !fields);
          if (gone.length) await client.xack(stream, group, ...gone.map(([id]) => id)).catch(() => {});
          entries = entries.filter(([, fields]) => fields);
        }
        if (!entries.length) {
          const wait = Math.max(1, Math.min(block, nextClaim - Date.now()));
          entries = await this.#read(
            reader,
            ['XREADGROUP', 'GROUP', group, consumer, 'COUNT', free, 'BLOCK', wait, 'STREAMS', stream, '>'],
            stop,
          );
          if (!entries) return;
        }
        for (const entry of entries) {
          const h = start(entry);
          if (concurrency === 1) await h; // strictly sequential
        }
      }
    } finally {
      await Promise.allSettled(inflight);
      // A consumer without pending entries is just noise in XINFO CONSUMERS.
      if (o.consumerName === undefined) {
        const pending = (await client.xpending(stream, group, '-', '+', 1, consumer).catch(() => [1])) as unknown[];
        if (!pending.length) await client.xgroup('DELCONSUMER', stream, group, consumer).catch(() => {});
      }
    }
  }

  async #handleGroupEntry(
    stream: string,
    group: string,
    consumer: string,
    entry: RawEntry,
    claimIdleMs: number,
    deliver: Deliver,
    stop: AbortSignal,
  ): Promise<void> {
    const client = this.#opts.client;
    const id = entry[0].toString();
    // Reset the idle time while held, so nobody claims an entry that is being worked on.
    const keepAlive = setInterval(
      () =>
        void client
          .xclaim(stream, group, consumer, 0, id, 'JUSTID')
          .catch((error) => this.#logger.error('redis XCLAIM keep-alive failed', { id, error })),
      Math.max(100, claimIdleMs / 3),
    );
    try {
      if (await this.#deliverUntilAcked(stream, entry, group, deliver, stop)) {
        await client.xack(stream, group, id);
      } else {
        // Stopped while holding it: make it claimable right away by the group's next subscriber.
        await client.xclaim(stream, group, consumer, 0, id, 'IDLE', claimIdleMs, 'JUSTID');
      }
    } finally {
      clearInterval(keepAlive);
    }
  }

  /** Deliver until acked; returns false when the subscription is stopping. */
  async #deliverUntilAcked(
    stream: string,
    entry: RawEntry,
    group: string | undefined,
    deliver: Deliver,
    stop: AbortSignal,
  ): Promise<boolean> {
    let msg = toMessage(stream, entry, group);
    const info = infos.get(msg)!;
    for (;;) {
      const r = await deliver(msg, this.#opts.ackDeadlineMs ?? null);
      if (r === 'ack') return true;
      if (r === 'aborted' || stop.aborted) return false;
      if (r === 'timeout') this.#logger.error('ack deadline exceeded, redelivering', { uuid: msg.uuid });
      try {
        await sleep(this.#opts.nackResendMs ?? 100, stop);
      } catch {
        return false;
      }
      msg = msg.copy();
      infos.set(msg, info);
    }
  }
}
