import { type Message, type Middleware, txFromContext } from '@millstream/core';
import { ER_DUP_ENTRY, type MysqlDriverLike, errnoOf } from './driver.js';
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
