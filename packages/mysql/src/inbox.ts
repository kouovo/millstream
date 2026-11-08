import { type Message, type Middleware, txFromContext } from '@millstream/core';
import { ER_DUP_ENTRY, type MysqlDriverLike, errnoOf, toMysqlDriver } from './driver.js';
import { tables } from './schema.js';
import { withTransaction } from './tx.js';

export interface InboxOptions {
  /** Dedup scope, usually the handler name (≤ 255 chars). */
  consumer: string;
  /**
   * Used to open a transaction when the message didn't arrive with one
   * (e.g. consuming Kafka/BullMQ). Without it, a transaction in context is required.
   */
  driver?: MysqlDriverLike;
  /** Dedup key (≤ 255 chars). Default: message uuid (never a payload hash). */
  key?: (msg: Message) => string;
}

const MAX_KEY = 255;

/**
 * Reliable dedup: record `(consumer, uuid)` in the *same* transaction as the business writes.
 * Duplicate → skip handler and ack. Handler fails → the record rolls back with it,
 * so a retry is not mistaken for a duplicate.
 *
 * A duplicate-key error only aborts the INSERT statement in InnoDB, not the transaction.
 */
export function inbox(opts: InboxOptions): Middleware {
  if (!opts.consumer || opts.consumer.length > MAX_KEY) throw new Error(`inbox consumer must be 1..${MAX_KEY} chars`);
  const keyOf = opts.key ?? ((m: Message) => m.uuid);
  return (next) => async (msg) => {
    const run = async () => {
      const tx = txFromContext();
      if (!tx) throw new Error('inbox middleware requires a transaction (or the `driver` option)');
      const key = keyOf(msg);
      // Longer keys would be truncated (or rejected) by VARCHAR(255): refuse instead of colliding.
      if (!key || key.length > MAX_KEY) throw new Error(`inbox key must be 1..${MAX_KEY} chars`);
      try {
        await tx.query(`INSERT INTO ${tables.inbox} (consumer, uuid) VALUES (?, ?)`, [opts.consumer, key]);
      } catch (error) {
        if (errnoOf(error) === ER_DUP_ENTRY) return; // already processed
        throw error;
      }
      return next(msg);
    };
    if (txFromContext() || !opts.driver) return run();
    return withTransaction(opts.driver, run);
  };
}

export interface PruneInboxOptions {
  /**
   * Delete records processed longer ago than this. Keep it well above the longest time a
   * duplicate can still arrive (broker retention, retries, replays), or it won't be caught.
   */
  olderThanMs: number;
  /** Only this consumer's records. Default: all. */
  consumer?: string;
  /** Rows per DELETE, each its own short transaction. Default 10_000. */
  batchSize?: number;
}

/**
 * Delete old inbox records in batches (run it from a scheduled job). Returns how many were deleted.
 * Never call it inside a business transaction.
 */
export async function pruneInbox(driver: MysqlDriverLike, opts: PruneInboxOptions): Promise<number> {
  const d = toMysqlDriver(driver);
  if (!(Number.isFinite(opts.olderThanMs) && opts.olderThanMs > 0)) {
    throw new Error(`olderThanMs must be a positive number, got ${opts.olderThanMs}`);
  }
  const batchSize = opts.batchSize ?? 10_000;
  const params: unknown[] = [Math.floor(opts.olderThanMs) * 1000];
  if (opts.consumer !== undefined) params.push(opts.consumer);
  params.push(batchSize);
  const text = `DELETE FROM ${tables.inbox}
WHERE processed_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
  ${opts.consumer !== undefined ? 'AND consumer = ?' : ''}
LIMIT ?`;
  let total = 0;
  for (;;) {
    const { rowCount } = await d.query(text, params);
    const n = Number(rowCount ?? 0);
    total += n;
    if (n < batchSize) return total;
  }
}
