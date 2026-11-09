// The published dist on plain Node (node:test) — no TS, no vitest.
import { randomUUID } from 'node:crypto';
import { after, afterEach, before, describe, it } from 'node:test';
import { GraphileWorkerPublisher, GraphileWorkerSubscriber } from '@millstream/graphile-worker';
import { pubSubContract } from '@millstream/test-kit';
import { runMigrations } from 'graphile-worker';
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.PG_URL ?? 'postgres://postgres:postgres@localhost:55432/postgres',
});
pool.on('error', () => {});
const schema = `gw_node_${process.pid}`;
before(() => runMigrations({ pgPool: pool, schema }));
after(async () => {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});

pubSubContract(
  { describe, it, afterEach },
  'GraphileWorkerSubscriber (dist)',
  GraphileWorkerSubscriber.features,
  async () => {
    const publisher = new GraphileWorkerPublisher(pool, { schema });
    const subs = [];
    return {
      publisher,
      subscriber: () => {
        const s = new GraphileWorkerSubscriber({ pgPool: pool, schema, pollInterval: 100, nackResendMs: 10 });
        subs.push(s);
        return s;
      },
      topic: async () => `node_${randomUUID()}`,
      close: async () => {
        await Promise.all(subs.map((s) => s.close()));
        await publisher.close();
      },
    };
  },
);
