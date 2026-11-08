import { type Message, type Middleware, txFromContext } from '@millstream/core';
import { tables } from './schema.js';
import type { SqlDriverLike } from './driver.js';
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
