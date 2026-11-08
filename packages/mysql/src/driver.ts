import type { Executor, QueryResultLike } from '@millstream/core';

export interface TransactionOptions {
  /**
   * The subscriber claims rows under `READ COMMITTED`, so InnoDB takes no gap/next-key
   * locks and never blocks concurrent publishers. Business transactions use the server default.
   */
  isolationLevel?: 'read committed';
}

/**
 * What the MySQL Pub/Sub needs from a driver: autocommit queries, plus a transaction
 * pinned to one connection that commits when `fn` resolves and rolls back when it throws.
 * Placeholders are `?`.
 *
 * Adapters: a `mysql2/promise` pool (or a callback pool, via `.promise()`) is detected
 * automatically (see `toMysqlDriver`); Kysely via `fromKysely` from `@millstream/mysql/kysely`.
 */
export interface MysqlDriver extends Executor {
  transaction<T>(fn: (tx: Executor) => Promise<T>, opts?: TransactionOptions): Promise<T>;
}

/** Structural `mysql2/promise` Pool / PoolConnection (no import of `mysql2`). */
export interface Mysql2PoolLike {
  query(sql: string, values?: any): Promise<[unknown, unknown]>;
  getConnection(): Promise<Mysql2PoolConnectionLike>;
}
export interface Mysql2PoolConnectionLike {
  query(sql: string, values?: any): Promise<[unknown, unknown]>;
  release(): void;
  destroy?(): void;
}
/** Structural callback-style `mysql2` Pool. */
export interface Mysql2CallbackPoolLike {
  promise(): Mysql2PoolLike;
}

export type MysqlDriverLike = MysqlDriver | Mysql2PoolLike | Mysql2CallbackPoolLike;

/** mysql2 returns rows for SELECT and a `ResultSetHeader` for writes. */
export function toQueryResult(result: unknown): QueryResultLike {
  if (Array.isArray(result)) return { rows: result, rowCount: result.length };
  const header = result as { affectedRows?: number } | null | undefined;
  return { rows: [], rowCount: header?.affectedRows ?? null };
}

function mysql2Executor(conn: { query(sql: string, values?: any): Promise<[unknown, unknown]> }): Executor {
  return {
    async query(text, params = []) {
      const [result] = await conn.query(text, params);
      return toQueryResult(result);
    },
  };
}

export function fromMysql2(pool: Mysql2PoolLike): MysqlDriver {
  return {
    ...mysql2Executor(pool),
    async transaction(fn, opts) {
      const conn = await pool.getConnection();
      let broken = false;
      try {
        if (opts?.isolationLevel) await conn.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
        await conn.query('START TRANSACTION');
        const result = await fn(mysql2Executor(conn));
        await conn.query('COMMIT');
        return result;
      } catch (error) {
        broken = !(await conn.query('ROLLBACK').then(
          () => true,
          () => false,
        ));
        throw error;
      } finally {
        // A connection whose ROLLBACK failed is destroyed instead of going back to the pool.
        if (broken && conn.destroy) conn.destroy();
        else conn.release();
      }
    },
  };
}

function isMysqlDriver(x: any): x is MysqlDriver {
  return typeof x?.query === 'function' && typeof x?.transaction === 'function';
}

export function toMysqlDriver(x: MysqlDriverLike): MysqlDriver {
  if (isMysqlDriver(x)) return x;
  const any = x as any;
  if (typeof any?.executeQuery === 'function') {
    throw new TypeError("Kysely instance: wrap it with `fromKysely(db)` from '@millstream/mysql/kysely'");
  }
  // The callback pool also has `getConnection`/`query`, but with callbacks: use its promise wrapper.
  if (typeof any?.promise === 'function' && typeof any?.getConnection === 'function') return fromMysql2(any.promise());
  if (typeof any?.getConnection === 'function' && typeof any?.query === 'function') return fromMysql2(any);
  throw new TypeError('unsupported MySQL driver: pass a mysql2 Pool or a MysqlDriver');
}

/** MySQL error number of a driver error (mysql2 sets `errno`). */
export function errnoOf(error: unknown): number | undefined {
  const e = error as { errno?: unknown; cause?: { errno?: unknown } } | undefined;
  const n = e?.errno ?? e?.cause?.errno;
  return typeof n === 'number' ? n : undefined;
}

export const ER_DUP_ENTRY = 1062;
export const ER_LOCK_WAIT_TIMEOUT = 1205;
export const ER_LOCK_DEADLOCK = 1213;
