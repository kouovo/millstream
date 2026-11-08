// Runs the published artifacts (dist) on plain Node with node:test — no TS, no vitest.
// Guards against "works in dev, broken as shipped" (exports map, ESM paths, d.ts-only types…).
import { after, afterEach, describe, it } from 'node:test';
import { MysqlQueuePublisher, MysqlQueueSubscriber, initializeSchema } from '@millstream/mysql';
import { pubSubContract } from '@millstream/test-kit';
import mysql from 'mysql2/promise';

const t = { describe, it, afterEach };
const fast = { pollIntervalMs: 50, resendIntervalMs: 10, retryIntervalMs: 100 };
const pool = mysql.createPool({
  uri: process.env.MYSQL_URL ?? 'mysql://root:mysql@localhost:53306/millstream',
  connectionLimit: 30,
});
after(() => pool.end());

let seq = 0;
const topic = () => `node${process.pid}_${Date.now() % 1e8}_${++seq}`;

pubSubContract(t, 'MysqlQueueSubscriber (dist)', MysqlQueueSubscriber.features, async () => {
  const subs = [];
  return {
    publisher: new MysqlQueuePublisher(pool),
    subscriber: () => {
      const s = new MysqlQueueSubscriber(pool, fast);
      subs.push(s);
      return s;
    },
    topic: async () => {
      const created = topic();
      await initializeSchema(pool, { queueTopics: [created] });
      return created;
    },
    close: async () => void (await Promise.all(subs.map((s) => s.close()))),
  };
});
