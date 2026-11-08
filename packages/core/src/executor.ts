/**
 * Minimal SQL executor — the counterpart of Watermill's `ContextExecutor`.
 * `pg.Pool`, `pg.PoolClient` and `pg.Client` all satisfy it.
 *
 * Pitfall: `pool.query` may use a different connection per call, so a transaction
 * must always be driven through one checked-out `PoolClient`.
 */
export interface QueryResultLike {
  rows: any[];
  /** Not every driver reports it reliably — prefer `RETURNING` when you need to know what changed. */
  rowCount?: number | null;
}

export interface Executor {
  query(text: string, params?: unknown[]): Promise<QueryResultLike>;
}
