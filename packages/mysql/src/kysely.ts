import type { Executor } from '@millstream/core';
import { CompiledQuery, type Kysely, type Transaction } from 'kysely';
import type { MysqlDriver } from './driver.js';

function kyselyExecutor(db: Kysely<any> | Transaction<any>): Executor {
  return {
    async query(text, params = []) {
      const r = await db.executeQuery(CompiledQuery.raw(text, params as unknown[]));
      return { rows: r.rows as any[], rowCount: r.numAffectedRows == null ? r.rows.length : Number(r.numAffectedRows) };
    },
  };
}

/**
 * Use a Kysely instance (MySQL dialect) as the Pub/Sub driver.
 *
 * ```ts
 * import { fromKysely, kyselyTx } from '@millstream/mysql/kysely';
 * const sub = new MysqlQueueSubscriber(fromKysely(db));
 * // inside your own Kysely transaction:
 * await db.transaction().execute((trx) => new MysqlQueuePublisher(kyselyTx(trx)).publish('outbox', msg));
 * ```
 */
export function fromKysely(db: Kysely<any>): MysqlDriver {
  return {
    ...kyselyExecutor(db),
    transaction: (fn, opts) => {
      let builder = db.transaction();
      if (opts?.isolationLevel) builder = builder.setIsolationLevel(opts.isolationLevel);
      return builder.execute((trx) => fn(kyselyExecutor(trx)));
    },
  };
}

/** Executor for an existing Kysely transaction — for publishing inside your business transaction. */
export function kyselyTx(trx: Transaction<any> | Kysely<any>): Executor {
  return kyselyExecutor(trx);
}
