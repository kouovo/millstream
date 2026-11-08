import { MemoryPubSub, REQUEUER_RETRIES, Router, consoleLogger, newMessage, txFromContext } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { fromKysely, kyselyTx } from '@millstream/mysql/kysely';
import { collect, delay, eventually, msgs, nextWithTimeout, payloads, pubSubContract } from '@millstream/test-kit';
import { Kysely, MysqlDialect } from 'kysely';
import { createPool } from 'mysql2';
import mysql from 'mysql2/promise';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  MysqlQueuePublisher,
  MysqlQueueSubscriber,
  type MysqlDriver,
  inbox,
  initializeSchema,
  mysqlDelayedRequeuer,
  pruneInbox,
  toMysqlDriver,
  withTransaction,
} from '../src/index.js';

const MYSQL_URL = process.env.MYSQL_URL ?? 'mysql://root:mysql@localhost:53306/millstream';
const fast = {
  pollIntervalMs: 50,
  resendIntervalMs: 10,
  retryIntervalMs: 100,
  ...(process.env.DEBUG_MYSQL ? { logger: consoleLogger } : {}),
};

interface DriverCase {
  name: string;
  driver: MysqlDriver;
  end: () => Promise<void>;
}

function mysql2(): DriverCase {
  const pool = mysql.createPool({ uri: MYSQL_URL, connectionLimit: 30 });
  return { name: 'mysql2', driver: toMysqlDriver(pool), end: () => pool.end() };
}

function kysely(): DriverCase {
  const db = new Kysely<any>({
    dialect: new MysqlDialect({ pool: createPool({ uri: MYSQL_URL, connectionLimit: 30 }) }),
  });
  return { name: 'kysely', driver: fromKysely(db), end: () => db.destroy() };
}

let seq = 0;
const newTopic = () => `t${process.pid}_${Date.now() % 1e8}_${++seq}`;

for (const make of [mysql2, kysely]) {
  const d = make();
  const { driver } = d;
  const testApi = { describe, it, afterEach };
  const queueTopic = async () => {
    const t = newTopic();
    await initializeSchema(driver, { queueTopics: [t] });
    return t;
  };
  const count = async (sql: string, params: unknown[] = []) => Number((await driver.query(sql, params)).rows[0].n);

  describe(`[${d.name}]`, () => {
    beforeAll(async () => {
      await initializeSchema(driver, { inbox: true });
      await driver.query(
        'CREATE TABLE IF NOT EXISTS business (id INT AUTO_INCREMENT PRIMARY KEY, uuid VARCHAR(255), note VARCHAR(255))',
      );
    });
    afterAll(() => d.end());

    pubSubContract(testApi, 'MysqlQueueSubscriber', MysqlQueueSubscriber.features, async () => {
      const subs: MysqlQueueSubscriber[] = [];
      return {
        publisher: new MysqlQueuePublisher(driver),
        subscriber: () => {
          const s = new MysqlQueueSubscriber(driver, fast);
          subs.push(s);
          return s;
        },
        topic: queueTopic,
        close: async () => void (await Promise.all(subs.map((s) => s.close()))),
      };
    });

    describe('specifics', () => {
      const controllers: AbortController[] = [];
      const ctrl = () => {
        const c = new AbortController();
        controllers.push(c);
        return c.signal;
      };
      const routers: Router[] = [];
      const newRouter = () => {
        const r = new Router({ closeTimeoutMs: 2000 });
        routers.push(r);
        return r;
      };
      afterEach(async () => {
        for (const c of controllers.splice(0)) c.abort();
        await Promise.all(routers.splice(0).map((r) => r.close()));
      });

      it('round-trips payload bytes and metadata exactly', async () => {
        const topic = await queueTopic();
        const original = newMessage(Buffer.from([0, 255, 1, 254]), { a: 'x', quote: '"{}"', empty: '', ü: '✓' });
        await new MysqlQueuePublisher(driver).publish(topic, original);
        const [m] = await collect(new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl()), 1);
        expect(m!.equals(original)).toBe(true);
      });

      it('acked rows are deleted', async () => {
        const topic = await queueTopic();
        await new MysqlQueuePublisher(driver).publish(topic, ...msgs(3));
        await collect(new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl()), 3);
        await eventually(async () => (await count(`SELECT count(*) AS n FROM \`millstream_queue_${topic}\``)) === 0);
      });

      it('a rolled-back publish is never delivered', async () => {
        const topic = await queueTopic();
        await withTransaction(driver, async (tx) => {
          await new MysqlQueuePublisher(tx).publish(topic, newMessage('ghost'));
          throw new Error('rollback');
        }).catch(() => {});
        await new MysqlQueuePublisher(driver).publish(topic, newMessage('real'));
        const iter = new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl())[Symbol.asyncIterator]();
        const got = await collect(iter, 1);
        expect(payloads(got)).toEqual(['real']);
        expect(await nextWithTimeout(iter, 300)).toBe('timeout');
      });

      it('parallel subscribers split the work without duplicates', async () => {
        const topic = await queueTopic();
        await new MysqlQueuePublisher(driver).publish(topic, ...msgs(30));
        const seen: string[] = [];
        const run = () =>
          collect(new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl()), 30, {
            timeoutMs: 15_000,
            onMessage: (m) => {
              seen.push(m.payload.toString());
              m.ack();
            },
          }).catch(() => {});
        void run();
        void run();
        void run();
        await eventually(() => seen.length >= 30);
        await delay(200);
        expect(seen.length).toBe(30);
        expect(new Set(seen)).toEqual(new Set(payloads(msgs(30))));
      });

      it('a claim only locks the claimed row (READ COMMITTED: skipped not-yet-due rows stay claimable)', async () => {
        const topic = await queueTopic();
        const due = new Date(Date.now() + 400).toISOString();
        // id 1: delayed, scanned past by the first claim; id 2: due now, claimed and held.
        await new MysqlQueuePublisher(driver).publish(
          topic,
          newMessage('delayed', { _millstream_delayed_until: due }),
          newMessage('held'),
        );
        const a = new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl())[Symbol.asyncIterator]();
        const held = await nextWithTimeout(a, 5000);
        if (held === 'timeout' || held.done) throw new Error('no message');
        expect(held.value.payload.toString()).toBe('held');
        // Under REPEATABLE READ the scanned 'delayed' row would stay locked until 'held' is acked.
        const b = new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl())[Symbol.asyncIterator]();
        const r = await nextWithTimeout(b, 3000);
        if (r === 'timeout' || r.done) throw new Error('delayed row was not claimable while another row is held');
        expect(r.value.payload.toString()).toBe('delayed');
        // Publishers aren't blocked either.
        const t0 = Date.now();
        await withTransaction(driver, (tx) => new MysqlQueuePublisher(tx).publish(topic, newMessage('next')));
        expect(Date.now() - t0).toBeLessThan(1000);
        r.value.ack();
        held.value.ack();
      });

      it('delayed messages stay invisible until due', async () => {
        const topic = await queueTopic();
        const due = new Date(Date.now() + 600).toISOString();
        await new MysqlQueuePublisher(driver).publish(
          topic,
          newMessage('later', { _millstream_delayed_until: due }),
          newMessage('now'),
        );
        const t0 = Date.now();
        const got = await collect(new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl()), 2);
        expect(payloads(got)).toEqual(['now', 'later']);
        expect(Date.now() - t0).toBeGreaterThanOrEqual(450);
      });

      it('handler writes via the subscriber tx commit atomically with the ack (exactly-once effect)', async () => {
        const topic = await queueTopic();
        const router = newRouter();
        let attempts = 0;
        router.addConsumerHandler({
          name: 'eo',
          subscribeTopic: topic,
          subscriber: new MysqlQueueSubscriber(driver, fast),
          handler: async (m) => {
            attempts++;
            await txFromContext()!.query('INSERT INTO business (uuid, note) VALUES (?, ?)', [m.uuid, topic]);
            if (attempts === 1) throw new Error('fail after write'); // nack → savepoint rollback
          },
        });
        void router.run();
        await router.running;
        const m = newMessage('x');
        await new MysqlQueuePublisher(driver).publish(topic, m);
        await eventually(() => attempts === 2);
        await router.close();
        expect(await count('SELECT count(*) AS n FROM business WHERE uuid = ?', [m.uuid])).toBe(1);
      });

      it('ack deadline rolls back and redelivers', async () => {
        const topic = await queueTopic();
        await new MysqlQueuePublisher(driver).publish(topic, newMessage('slow'));
        const s = new MysqlQueueSubscriber(driver, { ...fast, ackDeadlineMs: 200 });
        const got = await collect(s.subscribe(topic, ctrl()), 2, {
          onMessage: (m, i) => {
            if (i === 1) m.ack(); // first delivery: never answered
          },
        });
        expect(got[0]!.uuid).toBe(got[1]!.uuid);
      });

      it('outbox: business write + ForwarderPublisher in one tx, relayed to the broker', async () => {
        const outboxTopic = await queueTopic();
        const broker = new MemoryPubSub();
        const dest = broker.subscribe('orders.created');
        const router = newRouter();
        addForwarder(router, {
          subscriber: new MysqlQueueSubscriber(driver, fast),
          publisher: broker,
          forwarderTopic: outboxTopic,
        });
        void router.run();
        await router.running;

        const ok = newMessage('order-1', { correlation_id: 'c1' });
        await withTransaction(driver, async (tx) => {
          await tx.query('INSERT INTO business (uuid, note) VALUES (?, ?)', [ok.uuid, 'order']);
          await new ForwarderPublisher(new MysqlQueuePublisher(tx), outboxTopic).publish('orders.created', ok);
        });
        await withTransaction(driver, async (tx) => {
          await new ForwarderPublisher(new MysqlQueuePublisher(tx), outboxTopic).publish(
            'orders.created',
            newMessage('nope'),
          );
          throw new Error('business failed');
        }).catch(() => {});

        const [m] = await collect(dest, 1);
        expect(m!.equals(ok)).toBe(true);
        expect(await nextWithTimeout(dest[Symbol.asyncIterator](), 300)).toBe('timeout');
        await broker.close();
      });

      it('delayed requeue: a failing message moves aside, the topic keeps flowing, then it is retried', async () => {
        const topic = await queueTopic();
        const requeueTopic = await queueTopic();
        const router = newRouter();
        const { middlewares } = mysqlDelayedRequeuer({
          driver,
          router,
          requeueTopic,
          delay: { initialIntervalMs: 300 },
          subscriberOptions: fast,
        });
        const handled: Array<{ p: string; retries?: string; at: number }> = [];
        let failedOnce = false;
        router.addConsumerHandler({
          name: 'work',
          subscribeTopic: topic,
          subscriber: new MysqlQueueSubscriber(driver, fast),
          middlewares,
          handler: (m) => {
            const p = m.payload.toString();
            if (p === 'bad' && !failedOnce) {
              failedOnce = true;
              throw new Error('temporary');
            }
            handled.push({ p, retries: m.metadata[REQUEUER_RETRIES], at: Date.now() });
          },
        });
        void router.run();
        await router.running;
        const t0 = Date.now();
        await new MysqlQueuePublisher(driver).publish(topic, newMessage('bad'), newMessage('good'));
        await eventually(() => handled.length === 2);
        expect(handled.map((h) => h.p)).toEqual(['good', 'bad']);
        expect(handled[1]!.retries).toBe('1');
        expect(handled[1]!.at - t0).toBeGreaterThanOrEqual(250);
        expect(await count(`SELECT count(*) AS n FROM \`millstream_queue_${requeueTopic}\``)).toBe(0);
      });

      it('inbox middleware dedupes duplicate messages', async () => {
        const topic = await queueTopic();
        const router = newRouter();
        let runs = 0;
        router.addConsumerHandler({
          name: 'dedup',
          subscribeTopic: topic,
          subscriber: new MysqlQueueSubscriber(driver, fast),
          middlewares: [inbox({ consumer: `dedup_${topic}` })],
          handler: () => void runs++,
        });
        void router.run();
        await router.running;
        const m = newMessage('once');
        await new MysqlQueuePublisher(driver).publish(topic, m);
        await new MysqlQueuePublisher(driver).publish(topic, m.copy(), newMessage('other'));
        await eventually(() => runs === 2);
        await delay(300);
        expect(runs).toBe(2);
      });

      it('pruneInbox deletes old records in batches, optionally per consumer', async () => {
        const c1 = `prune_${newTopic()}`;
        const c2 = `prune_${newTopic()}`;
        const insert = (consumer: string, uuid: string, ageSeconds: number) =>
          driver.query(
            'INSERT INTO millstream_inbox (consumer, uuid, processed_at) VALUES (?, ?, UTC_TIMESTAMP(6) - INTERVAL ? SECOND)',
            [consumer, uuid, ageSeconds],
          );
        for (let i = 0; i < 5; i++) await insert(c1, `old${i}`, 7200);
        await insert(c1, 'fresh', 1);
        await insert(c2, 'old', 7200);

        expect(await pruneInbox(driver, { olderThanMs: 3_600_000, consumer: c1, batchSize: 2 })).toBe(5);
        expect(await count('SELECT count(*) AS n FROM millstream_inbox WHERE consumer = ?', [c1])).toBe(1); // 'fresh' kept
        expect(await count('SELECT count(*) AS n FROM millstream_inbox WHERE consumer = ?', [c2])).toBe(1); // other consumer untouched
        expect(await pruneInbox(driver, { olderThanMs: 3_600_000 })).toBeGreaterThanOrEqual(1);
        expect(await count('SELECT count(*) AS n FROM millstream_inbox WHERE consumer = ?', [c2])).toBe(0);
        await expect(pruneInbox(driver, { olderThanMs: 0 })).rejects.toThrow(/olderThanMs/);
      });

      it('inbox records default to UTC', async () => {
        const consumer = `utc_${newTopic()}`;
        await driver.query('INSERT INTO millstream_inbox (consumer, uuid) VALUES (?, ?)', [consumer, 'x']);
        const { rows } = await driver.query(
          'SELECT ABS(TIMESTAMPDIFF(SECOND, processed_at, UTC_TIMESTAMP(6))) AS drift FROM millstream_inbox WHERE consumer = ?',
          [consumer],
        );
        expect(Number(rows[0].drift)).toBeLessThan(5);
      });

      it('inbox opens its own tx for non-SQL subscribers', async () => {
        const ps = new MemoryPubSub();
        const router = newRouter();
        const consumer = `mem_${newTopic()}`;
        let runs = 0;
        router.addConsumerHandler({
          name: 'mem',
          subscribeTopic: 't',
          subscriber: ps,
          middlewares: [inbox({ consumer, driver })],
          handler: async (m) => {
            runs++;
            await txFromContext()!.query('INSERT INTO business (uuid, note) VALUES (?, ?)', [m.uuid, consumer]);
          },
        });
        void router.run();
        await router.running;
        const m = newMessage('x');
        await ps.publish('t', m, m.copy());
        await eventually(
          async () =>
            (await count('SELECT count(*) AS n FROM business WHERE note = ?', [consumer])) === 1 && runs === 1,
        );
        await ps.close();
      });

      if (d.name === 'kysely') {
        it('kyselyTx joins an existing Kysely transaction', async () => {
          const topic = await queueTopic();
          const db = new Kysely<any>({ dialect: new MysqlDialect({ pool: createPool({ uri: MYSQL_URL }) }) });
          try {
            await db
              .transaction()
              .execute((trx) => new MysqlQueuePublisher(kyselyTx(trx)).publish(topic, newMessage('k')));
            await db
              .transaction()
              .execute(async (trx) => {
                await new MysqlQueuePublisher(kyselyTx(trx)).publish(topic, newMessage('rolled-back'));
                throw new Error('rollback');
              })
              .catch(() => {});
          } finally {
            await db.destroy();
          }
          const iter = new MysqlQueueSubscriber(driver, fast).subscribe(topic, ctrl())[Symbol.asyncIterator]();
          expect(payloads(await collect(iter, 1))).toEqual(['k']);
          expect(await nextWithTimeout(iter, 300)).toBe('timeout');
        });
      }
    });
  });
}

describe('toMysqlDriver', () => {
  it('accepts a callback mysql2 pool', async () => {
    const pool = createPool({ uri: MYSQL_URL });
    try {
      const r = await toMysqlDriver(pool).query('SELECT 1 AS one');
      expect(Number(r.rows[0].one)).toBe(1);
    } finally {
      await new Promise((resolve) => pool.end(resolve));
    }
  });

  it('rejects a bare Kysely instance with a hint', () => {
    expect(() => toMysqlDriver({ executeQuery() {} } as any)).toThrow(/fromKysely/);
  });
});
