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
import {
  ER_LOCK_DEADLOCK,
  ER_LOCK_WAIT_TIMEOUT,
  type MysqlDriver,
  type MysqlDriverLike,
  errnoOf,
  toMysqlDriver,
} from './driver.js';
import { initializeSchema, tables, validateTopic } from './schema.js';

export interface MysqlSubscriberOptions {
  /** Wait between polls when nothing was found. Default 1000ms. */
  pollIntervalMs?: number;
  /** Wait before redelivering a nacked message. Default 100ms. */
  resendIntervalMs?: number;
  /** Wait after an unexpected error. Default 1000ms. */
  retryIntervalMs?: number;
  /**
   * Max time to wait for Ack/Nack; then the transaction is rolled back and the
   * message redelivered. `null` = forever (holds the row lock and a connection). Default 30s.
   */
  ackDeadlineMs?: number | null;
  /** Create the table on subscribe (outside any business transaction). Default false. */
  initializeSchema?: boolean;
  logger?: Logger;
}

type Row = { id: string | number; uuid: string; payload: Buffer; metadata: unknown };

function isTransient(e: unknown): boolean {
  const n = errnoOf(e);
  return n === ER_LOCK_DEADLOCK || n === ER_LOCK_WAIT_TIMEOUT;
}

/** mysql2 parses JSON columns; other drivers / MariaDB (JSON = LONGTEXT) return a string. */
function parseMetadata(raw: unknown): Record<string, string> {
  if (raw == null) return {};
  if (typeof raw === 'string') return JSON.parse(raw);
  if (Buffer.isBuffer(raw)) return JSON.parse(raw.toString('utf8'));
  return raw as Record<string, string>;
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
 * Queue-type subscriber: `FOR UPDATE SKIP LOCKED` claim, DELETE on ack (MySQL 8.0+).
 * Many subscribers run in parallel; no consumer groups, no global order.
 * Rows with a future `delayed_until` are invisible → usable as delayed requeue store.
 *
 *   BEGIN (READ COMMITTED) → claim next due row → SAVEPOINT → deliver → wait Ack/Nack
 *     Ack      → DELETE row → COMMIT
 *     Nack     → ROLLBACK TO SAVEPOINT (undo the handler's tx writes) → redeliver a copy
 *     timeout / close → ROLLBACK (row unlocked, picked up again)
 *
 * The handler runs inside the subscriber's transaction and can join it via
 * `msg.tx` / `txFromContext()`: business writes + ack commit atomically.
 * The claim runs under READ COMMITTED: no gap locks, so publishers are never blocked.
 */
export class MysqlQueueSubscriber implements Subscriber {
  static readonly features: Features = {
    ordered: false,
    persistent: true,
    consumerGroups: false,
    newSubscriberReceivesOldMessages: true,
    exactlyOnceWithTx: true,
  };

  readonly #logger: Logger;
  readonly #closing = new AbortController();
  readonly #pollIntervalMs: number;
  readonly #resendIntervalMs: number;
  readonly #retryIntervalMs: number;
  readonly #ackDeadlineMs: number | null;
  readonly #initializeSchema: boolean;
  readonly #driver: MysqlDriver;

  constructor(driver: MysqlDriverLike, opts: MysqlSubscriberOptions = {}) {
    this.#driver = toMysqlDriver(driver);
    this.#logger = opts.logger ?? nopLogger;
    this.#pollIntervalMs = opts.pollIntervalMs ?? 1000;
    this.#resendIntervalMs = opts.resendIntervalMs ?? 100;
    this.#retryIntervalMs = opts.retryIntervalMs ?? 1000;
    this.#ackDeadlineMs = opts.ackDeadlineMs === undefined ? 30_000 : opts.ackDeadlineMs;
    this.#initializeSchema = opts.initializeSchema ?? false;
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('MysqlQueueSubscriber');
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
    if (this.#initializeSchema) {
      await this.#withRetry(stop, () => initializeSchema(this.#driver, { queueTopics: [topic] }));
    }
    while (!stop.aborted) {
      let found = false;
      try {
        found = await this.#processOne(topic, deliver, stop);
      } catch (error) {
        if (isAbortError(error) || stop.aborted) return;
        if (isTransient(error)) continue;
        this.#logger.error('mysql subscriber query failed', { topic, error });
        await sleep(this.#retryIntervalMs, stop);
        continue;
      }
      if (!found) await sleep(this.#pollIntervalMs, stop);
    }
  }

  /** One transaction: returns whether a row was found. */
  async #processOne(topic: string, deliver: Deliver, stop: AbortSignal): Promise<boolean> {
    let found = false;
    try {
      await this.#driver.transaction(
        async (tx) => {
          const { rows } = await tx.query(
            `SELECT q.id, q.uuid, q.payload, q.metadata FROM ${tables.queue(topic)} q
             WHERE q.delayed_until IS NULL OR q.delayed_until <= UTC_TIMESTAMP(6)
             ORDER BY q.id
             LIMIT 1
             FOR UPDATE SKIP LOCKED`,
          );
          const row = rows[0] as Row | undefined;
          if (!row) return;
          found = true;
          if ((await this.#deliver(tx, row, deliver, stop)) !== 'ack') throw new Rollback();
          await tx.query(`DELETE FROM ${tables.queue(topic)} WHERE id = ?`, [row.id]);
        },
        { isolationLevel: 'read committed' },
      );
    } catch (error) {
      if (!(error instanceof Rollback)) throw error;
    }
    return found;
  }

  async #deliver(tx: Executor, row: Row, deliver: Deliver, stop: AbortSignal): Promise<string> {
    const metadata = parseMetadata(row.metadata);
    const payload = Buffer.isBuffer(row.payload) ? row.payload : Buffer.from(row.payload);
    for (;;) {
      await tx.query('SAVEPOINT millstream_delivery');
      const msg = new Message(row.uuid, payload, { ...metadata });
      const guard = guardedExecutor(tx);
      msg.tx = guard.exec;
      const r = await deliver(msg, this.#ackDeadlineMs);
      guard.revoke();
      if (r === 'ack') return r;
      if (r === 'timeout') this.#logger.error('ack deadline exceeded, rolling back', { uuid: msg.uuid });
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
        this.#logger.error('mysql subscriber setup failed, retrying', { error });
        await sleep(this.#retryIntervalMs, stop);
      }
    }
  }
}
