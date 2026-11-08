import type { Executor, QueryResultLike } from '@millstream/core';

/**
 * What the PostgreSQL Pub/Sub needs from a driver: autocommit queries, plus a transaction
 * pinned to one connection that commits when `fn` resolves and rolls back when it throws.
 *
 * Adapters: node-postgres `Pool` and postgres.js `sql` are detected automatically
 * (see `toSqlDriver`); Kysely via `fromKysely` from `@millstream/pg/kysely`.
 */
export interface SqlDriver extends Executor {
  transaction<T>(fn: (tx: Executor) => Promise<T>): Promise<T>;
}

/** Structural node-postgres `Pool` (no import of `pg`). */
export interface NodePostgresPoolLike {
  connect(): Promise<NodePostgresClientLike>;
  query(text: string, params?: any[]): Promise<QueryResultLike>;
}
export interface NodePostgresClientLike {
  query(text: string, params?: any[]): Promise<QueryResultLike>;
  release(err?: Error | boolean): void;
}

/** Structural postgres.js `sql` instance (no import of `postgres`). */
export interface PostgresJsLike {
  unsafe(text: string, params?: any[]): PromiseLike<ArrayLike<any> & { count?: number | null }>;
  begin<T>(fn: (tx: PostgresJsLike) => Promise<T>): Promise<unknown>;
}

export type SqlDriverLike = SqlDriver | NodePostgresPoolLike | PostgresJsLike;

export function fromNodePostgres(pool: NodePostgresPoolLike): SqlDriver {
  return {
    query: (text, params) => pool.query(text, params as any[]),
    async transaction(fn) {
      const client = await pool.connect();
      let broken = false;
      try {
        await client.query('BEGIN');
        const result = await fn(client);
        await client.query('COMMIT');
        return result;
      } catch (error) {
        broken = !(await client.query('ROLLBACK').then(
          () => true,
          () => false,
        ));
        throw error;
      } finally {
        // A connection whose ROLLBACK failed is destroyed instead of going back to the pool.
        client.release(broken || undefined);
      }
    },
  };
}

function postgresJsExecutor(sql: PostgresJsLike): Executor {
  return {
    async query(text, params = []) {
      const result = await sql.unsafe(text, params as any[]);
      const rows = Array.from(result);
      return { rows, rowCount: result.count ?? rows.length };
    },
  };
}

export function fromPostgresJs(sql: PostgresJsLike): SqlDriver {
  return {
    ...postgresJsExecutor(sql),
    async transaction<T>(fn: (tx: Executor) => Promise<T>): Promise<T> {
      let result!: T;
      // Box the result: postgres.js treats an array returned from `begin` as a list of queries.
      await sql.begin(async (tx) => {
        result = await fn(postgresJsExecutor(tx));
      });
      return result;
    },
  };
}

function isSqlDriver(x: any): x is SqlDriver {
  return typeof x?.query === 'function' && typeof x?.transaction === 'function';
}

export function toSqlDriver(x: SqlDriverLike): SqlDriver {
  if (isSqlDriver(x)) return x;
  const any = x as any;
  if (typeof any?.unsafe === 'function' && typeof any?.begin === 'function') return fromPostgresJs(any);
  if (typeof any?.connect === 'function' && typeof any?.query === 'function') return fromNodePostgres(any);
  if (typeof any?.executeQuery === 'function') {
    throw new TypeError("Kysely instance: wrap it with `fromKysely(db)` from '@millstream/pg/kysely'");
  }
  throw new TypeError('unsupported SQL driver: pass a node-postgres Pool, a postgres.js sql, or a SqlDriver');
}
