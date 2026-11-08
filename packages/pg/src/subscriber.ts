import {
  AbortedError,
  type Deliver,
  type Executor,
  type Features,
  type Logger,
  Message,
  PubSubClosedError,
  type Subscriber,
  anySignal,
  isAbortError,
  nopLogger,
  producerSubscription,
  sleep,
} from '@millstream/core';
import { type SqlDriver, type SqlDriverLike, toSqlDriver } from './driver.js';
import type { PgNotifier } from './notifier.js';
import { initializeSchema, notifyChannel, queueNotifyChannel, tables, validateTopic } from './schema.js';

export interface PgSubscriberOptions {
  /** Wait between polls when nothing was found. Default 1000ms. */
  pollIntervalMs?: number;
  /** Wait before redelivering a nacked message. Default 100ms. */
  resendIntervalMs?: number;
  /** Wait after an unexpected error. Default 1000ms. */
  retryIntervalMs?: number;
  /**
   * Max time to wait for Ack/Nack; then the transaction is rolled back and the
   * message redelivered. `null` = forever (dangerous: holds the xmin watermark). Default 30s.
   */
  ackDeadlineMs?: number | null;
  /** Create tables on subscribe (outside any business transaction). Default false. */
  initializeSchema?: boolean;
  /** LISTEN/NOTIFY wake-ups on top of polling. */
  notifier?: PgNotifier;
  logger?: Logger;
}

type Row = { uuid: string; payload: Buffer; metadata: Record<string, string> | null } & Record<string, unknown>;

function isTransient(e: unknown): boolean {
  const code = (e as { code?: string } | undefined)?.code;
  return code === '40001' || code === '40P01' || /deadlock|could not serialize|concurrent update/i.test(String(e));
}

/**
 * Executor handed to handlers via `msg.tx`; revoked once the delivery is settled so a
 * late/slow handler can't touch a connection that was already released to the pool.
 */
function guardedExecutor(tx: Executor): { exec: Executor; revoke: () => void } {
  let valid = true;
  return {
    exec: {
      query: (text, params) =>
        valid
          ? tx.query(text, params)
          : Promise.reject(new Error('the transaction of this delivery is already finished')),
    },
    revoke: () => (valid = false),
  };
}

/** Thrown inside the driver transaction to roll it back (non-ack outcome). */
class Rollback extends Error {}

/**
 * Shared polling loop: one transaction per message (batch size 1, see Watermill §6.3).
 *
 *   BEGIN → select next row (locks progress) → SAVEPOINT → deliver → wait Ack/Nack
 *     Ack      → advance progress → COMMIT
 *     Nack     → ROLLBACK TO SAVEPOINT (undo the handler's tx writes) → redeliver a copy
 *     timeout / close → ROLLBACK (redelivered next round)
 *
 * The handler runs inside the subscriber's transaction and can join it via
 * `msg.tx` / `txFromContext()`: business writes + ack commit atomically.
 */
abstract class PgSubscriberBase implements Subscriber {
  protected readonly logger: Logger;
  readonly #closing = new AbortController();
  readonly #pollIntervalMs: number;
  readonly #resendIntervalMs: number;
  readonly #retryIntervalMs: number;
  readonly #ackDeadlineMs: number | null;
  readonly #initializeSchema: boolean;
  readonly #notifier?: PgNotifier;

  protected readonly driver: SqlDriver;

  constructor(driver: SqlDriverLike, opts: PgSubscriberOptions = {}) {
    this.driver = toSqlDriver(driver);
    this.logger = opts.logger ?? nopLogger;
    this.#pollIntervalMs = opts.pollIntervalMs ?? 1000;
    this.#resendIntervalMs = opts.resendIntervalMs ?? 100;
    this.#retryIntervalMs = opts.retryIntervalMs ?? 1000;
    this.#ackDeadlineMs = opts.ackDeadlineMs === undefined ? 30_000 : opts.ackDeadlineMs;
    this.#initializeSchema = opts.initializeSchema ?? false;
    this.#notifier = opts.notifier;
  }

  protected abstract channel(topic: string): string;
  protected abstract initSchema(topic: string): Promise<void>;
  protected abstract prepare(topic: string): Promise<void>;
  protected abstract selectNext(tx: Executor, topic: string): Promise<Row | undefined>;
  protected abstract markAcked(tx: Executor, topic: string, row: Row): Promise<void>;

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('PgSubscriber');
    validateTopic(topic);
    return producerSubscription(
      (deliver, stop) => this.#run(topic, deliver, stop),
      anySignal(signal, this.#closing.signal),
    );
  }

  async close(): Promise<void> {
    this.#closing.abort();
  }

  async #run(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const notifier = this.#notifier;
    const channel = this.channel(topic);
    await this.#withRetry(stop, async () => {
      if (this.#initializeSchema) await this.initSchema(topic);
      await this.prepare(topic);
    });
    await notifier?.listen(channel);

    while (!stop.aborted) {
      const generation = notifier?.generation(channel) ?? 0;
      let found = false;
      try {
        found = await this.#processOne(topic, deliver, stop);
      } catch (error) {
        if (isAbortError(error) || stop.aborted) return;
        if (isTransient(error)) continue;
        this.logger.error('pg subscriber query failed', { topic, error });
        await sleep(this.#retryIntervalMs, stop);
        continue;
      }
      if (!found) {
        if (notifier) await notifier.wait(channel, generation, this.#pollIntervalMs, stop);
        else await sleep(this.#pollIntervalMs, stop);
      }
    }
  }

  /** One transaction: returns whether a row was found. */
  async #processOne(topic: string, deliver: Deliver, stop: AbortSignal): Promise<boolean> {
    let found = false;
    try {
      await this.driver.transaction(async (tx) => {
        const row = await this.selectNext(tx, topic);
        if (!row) return;
        found = true;
        if ((await this.#deliver(tx, row, deliver, stop)) !== 'ack') throw new Rollback();
        await this.markAcked(tx, topic, row);
      });
    } catch (error) {
      if (!(error instanceof Rollback)) throw error;
    }
    return found;
  }

  async #deliver(tx: Executor, row: Row, deliver: Deliver, stop: AbortSignal): Promise<string> {
    for (;;) {
      await tx.query('SAVEPOINT millstream_delivery');
      const msg = new Message(row.uuid, row.payload, row.metadata ?? {});
      const guard = guardedExecutor(tx);
      msg.tx = guard.exec;
      const r = await deliver(msg, this.#ackDeadlineMs);
      guard.revoke();
      if (r === 'ack') return r;
      if (r === 'timeout') this.logger.error('ack deadline exceeded, rolling back', { uuid: msg.uuid });
      if (r !== 'nack') return r;
      await tx.query('ROLLBACK TO SAVEPOINT millstream_delivery');
      if (this.#resendIntervalMs) await sleep(this.#resendIntervalMs, stop);
    }
  }

  async #withRetry(stop: AbortSignal, fn: () => Promise<void>): Promise<void> {
    for (;;) {
      try {
        return await fn();
      } catch (error) {
        if (stop.aborted) throw new AbortedError();
        this.logger.error('pg subscriber setup failed, retrying', { error });
        await sleep(this.#retryIntervalMs, stop);
      }
    }
  }
}

export interface PgLogSubscriberOptions extends PgSubscriberOptions {
  /** Progress bookmark name. Same group = share progress (single active consumer). Default ''. */
  consumerGroup?: string;
}

/**
 * Log-type subscriber (Kafka-like, ordered, consumer groups).
 *
 * `bigserial` allocation order ≠ commit order, so reading `offset > last_acked`
 * would permanently skip a smaller offset committed later. Instead progress is
 * `(transaction_id, offset)` and only rows from transactions older than the
 * snapshot xmin (= guaranteed finished) are read: nothing can appear behind the cursor.
 */
export class PgSubscriber extends PgSubscriberBase {
  static readonly features: Features = {
    ordered: true,
    persistent: true,
    consumerGroups: true,
    newSubscriberReceivesOldMessages: true,
    exactlyOnceWithTx: true,
  };

  readonly consumerGroup: string;

  constructor(driver: SqlDriverLike, opts: PgLogSubscriberOptions = {}) {
    super(driver, opts);
    this.consumerGroup = opts.consumerGroup ?? '';
  }

  protected channel(topic: string): string {
    return notifyChannel(topic);
  }

  protected initSchema(topic: string): Promise<void> {
    return initializeSchema(this.driver, { logTopics: [topic] });
  }

  protected async prepare(topic: string): Promise<void> {
    await this.driver.query(
      `INSERT INTO ${tables.offsets(topic)} (consumer_group) VALUES ($1) ON CONFLICT DO NOTHING`,
      [this.consumerGroup],
    );
  }

  protected async selectNext(tx: Executor, topic: string): Promise<Row | undefined> {
    // 1) Lock this group's bookmark: one active consumer per group, the rest wait here.
    const { rows: off } = await tx.query(
      `SELECT offset_acked::text AS offset_acked, last_processed_transaction_id::text AS last_tx
       FROM ${tables.offsets(topic)} WHERE consumer_group = $1 FOR UPDATE`,
      [this.consumerGroup],
    );
    if (!off[0]) throw new Error(`no offsets row for group "${this.consumerGroup}" on topic "${topic}"`);
    // 2) New statement → new snapshot taken after the lock (READ COMMITTED).
    //    xid8/bigint stay strings end to end; never compared in JS.
    const { rows } = await tx.query(
      `SELECT m."offset"::text AS "offset", m.transaction_id::text AS transaction_id, m.uuid, m.payload, m.metadata
       FROM ${tables.messages(topic)} m
       WHERE ((m.transaction_id = $1::xid8 AND m."offset" > $2::bigint) OR m.transaction_id > $1::xid8)
         AND m.transaction_id < pg_snapshot_xmin(pg_current_snapshot())
       -- qualified on purpose: bare names would sort by the ::text output aliases ("10" < "2")
       ORDER BY m.transaction_id, m."offset"
       LIMIT 1`,
      [off[0].last_tx, off[0].offset_acked],
    );
    return rows[0];
  }

  protected async markAcked(tx: Executor, topic: string, row: Row): Promise<void> {
    await tx.query(
      `UPDATE ${tables.offsets(topic)}
       SET offset_acked = $1::bigint, last_processed_transaction_id = $2::xid8
       WHERE consumer_group = $3`,
      [row.offset, row.transaction_id, this.consumerGroup],
    );
  }
}

/**
 * Queue-type subscriber: `FOR UPDATE SKIP LOCKED` claim, DELETE on ack.
 * Many subscribers run in parallel; no consumer groups, no global order.
 * Rows with a future `delayed_until` are invisible → usable as delayed requeue store.
 */
export class PgQueueSubscriber extends PgSubscriberBase {
  static readonly features: Features = {
    ordered: false,
    persistent: true,
    consumerGroups: false,
    newSubscriberReceivesOldMessages: true,
    exactlyOnceWithTx: true,
  };

  protected channel(topic: string): string {
    return queueNotifyChannel(topic);
  }

  protected initSchema(topic: string): Promise<void> {
    return initializeSchema(this.driver, { queueTopics: [topic] });
  }

  protected async prepare(): Promise<void> {}

  protected async selectNext(tx: Executor, topic: string): Promise<Row | undefined> {
    const { rows } = await tx.query(
      `SELECT q.id::text AS id, q.uuid, q.payload, q.metadata FROM ${tables.queue(topic)} q
       WHERE q.delayed_until IS NULL OR q.delayed_until <= now()
       ORDER BY q.id
       LIMIT 1
       FOR UPDATE SKIP LOCKED`,
    );
    return rows[0];
  }

  protected async markAcked(tx: Executor, topic: string, row: Row): Promise<void> {
    await tx.query(`DELETE FROM ${tables.queue(topic)} WHERE id = $1::bigint`, [row.id]);
  }
}
