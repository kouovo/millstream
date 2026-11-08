// Runs the published artifacts (dist) on plain Node with node:test — no TS, no vitest.
// Guards against "works in dev, broken as shipped" (exports map, ESM paths, d.ts-only types…).
import { after, afterEach, describe, it } from 'node:test';
import { MemoryPubSub } from '@millstream/core';
import { PgPublisher, PgQueuePublisher, PgQueueSubscriber, PgSubscriber, initializeSchema } from '@millstream/pg';
import { pubSubContract } from '@millstream/test-kit';
import pg from 'pg';

const t = { describe, it, afterEach };
const fast = { pollIntervalMs: 50, resendIntervalMs: 10, retryIntervalMs: 100 };
const pool = new pg.Pool({
  connectionString: process.env.PG_URL ?? 'postgres://postgres:postgres@localhost:55432/postgres',
  max: 30,
});
after(() => pool.end());

let seq = 0;
const topic = () => `node${process.pid}_${Date.now() % 1e8}_${++seq}`;

pubSubContract(t, 'MemoryPubSub (dist)', MemoryPubSub.features, async () => {
  const ps = new MemoryPubSub();
  return { publisher: ps, subscriber: () => ps, topic: async () => topic(), close: () => ps.close() };
});

for (const [name, Pub, Sub, kind] of [
  ['PgSubscriber (dist)', PgPublisher, PgSubscriber, 'logTopics'],
  ['PgQueueSubscriber (dist)', PgQueuePublisher, PgQueueSubscriber, 'queueTopics'],
]) {
  pubSubContract(t, name, Sub.features, async () => {
    const subs = [];
    return {
      publisher: new Pub(pool),
      subscriber: (consumerGroup) => {
        const s = new Sub(pool, { ...fast, consumerGroup });
        subs.push(s);
        return s;
      },
      topic: async () => {
        const created = topic();
        await initializeSchema(pool, { [kind]: [created] });
        return created;
      },
      close: async () => void (await Promise.all(subs.map((s) => s.close()))),
    };
  });
}
