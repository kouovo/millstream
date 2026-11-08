import { type Message, type Middleware, txFromContext } from '@millstream/core';
import { tables } from './schema.js';
import { type SqlDriverLike, toSqlDriver } from './driver.js';
import { withTransaction } from './tx.js';

export interface InboxOptions {
  /** Dedup scope, usually the handler name. */
  consumer: string;
  /**
   * Used to open a transaction when the message didn't arrive with one
   * (e.g. consuming Kafka/Rabbit). Without it, a transaction in context is required.
   */
  driver?: SqlDriverLike;
  /** Dedup key. Default: message uuid (never a payload hash — see Watermill Deduplicator pitfalls). */
  key?: (msg: Message) => string;
}

/**
 * Reliable dedup (Watermill has only an in-memory Deduplicator): record
 * `(consumer, uuid)` in the *same* transaction as the business writes.
 * Duplicate → skip handler and ack. Handler fails → the record rolls back with it,
 * so a retry is not mistaken for a duplicate.
 */
export function inbox(opts: InboxOptions): Middleware {
  const keyOf = opts.key ?? ((m: Message) => m.uuid);
  return (next) => async (msg) => {
    const run = async () => {
      const tx = txFromContext();
      if (!tx) throw new Error('inbox middleware requires a transaction (or the `driver` option)');
      const r = await tx.query(
        `INSERT INTO ${tables.inbox} (consumer, uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING 1`,
        [opts.consumer, keyOf(msg)],
      );
      if (r.rows.length === 0) return; // already processed
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
export async function pruneInbox(driver: SqlDriverLike, opts: PruneInboxOptions): Promise<number> {
  const d = toSqlDriver(driver);
  if (!(Number.isFinite(opts.olderThanMs) && opts.olderThanMs > 0)) {
    throw new Error(`olderThanMs must be a positive number, got ${opts.olderThanMs}`);
  }
  const batchSize = opts.batchSize ?? 10_000;
  const params: unknown[] = [String(Math.floor(opts.olderThanMs)), batchSize];
  if (opts.consumer !== undefined) params.push(opts.consumer);
  const text = `WITH deleted AS (
  DELETE FROM ${tables.inbox} WHERE ctid = ANY(ARRAY(
    SELECT ctid FROM ${tables.inbox}
    WHERE processed_at < now() - $1::bigint * interval '1 millisecond'
      ${opts.consumer !== undefined ? 'AND consumer = $3' : ''}
    LIMIT $2::int))
  RETURNING 1)
SELECT count(*)::int AS n FROM deleted`;
  let total = 0;
  for (;;) {
    const { rows } = await d.query(text, params);
    const n = Number(rows[0]?.n ?? 0);
    total += n;
    if (n < batchSize) return total;
  }
}
