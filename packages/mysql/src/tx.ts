import { type Executor, withContext } from '@millstream/core';
import { type MysqlDriverLike, toMysqlDriver } from './driver.js';

/**
 * Run `fn` in a transaction. The transaction is also put into the async context, so
 * publishers built on `contextExecutor(driver)` (and `txFromContext()`) join it.
 *
 * ```ts
 * await withTransaction(pool, async (tx) => {
 *   await tx.query('INSERT INTO orders ...');
 *   await new MysqlQueuePublisher(tx).publish('orders', msg); // same transaction
 * });
 * ```
 */
export function withTransaction<T>(driver: MysqlDriverLike, fn: (tx: Executor) => Promise<T>): Promise<T> {
  return toMysqlDriver(driver).transaction((tx) => withContext({ tx }, () => fn(tx)));
}
