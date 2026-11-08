import {
  MemoryPubSub,
  REQUEUER_RETRIES,
  Router,
  consoleLogger,
  contextExecutor,
  newMessage,
  poisonQueue,
  txFromContext,
} from '@millstream/core';
import { EventBus, EventProcessor, defineEvent } from '@millstream/cqrs';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { fromKysely } from '@millstream/pg/kysely';
import { collect, delay, eventually, msgs, nextWithTimeout, payloads, pubSubContract } from '@millstream/test-kit';
import { Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';
import { z } from 'zod';
import postgres from 'postgres';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  PgNotifier,
  PgPublisher,
  PgQueuePublisher,
  PgQueueSubscriber,
  PgSubscriber,
  type SqlDriver,
  inbox,
  initializeSchema,
  pgDelayedRequeuer,
  pruneInbox,
  toSqlDriver,
  withTransaction,
} from '../src/index.js';

const PG_URL = process.env.PG_URL ?? 'postgres://postgres:postgres@localhost:55432/postgres';
const fast = {
  pollIntervalMs: 50,
  resendIntervalMs: 10,
  retryIntervalMs: 100,
  ...(process.env.DEBUG_PG ? { logger: consoleLogger } : {}),
};

interface DriverCase {
  name: string;
  driver: SqlDriver;
  notifier: () => PgNotifier;
  end: () => Promise<void>;
}

function nodePostgres(): DriverCase {
  const pool = new pg.Pool({ connectionString: PG_URL, max: 30 });
  return {
    name: 'node-postgres',
    driver: toSqlDriver(pool),
    notifier: () => PgNotifier.nodePostgres(() => new pg.Client(PG_URL)),
    end: () => pool.end(),
  };
}

function postgresJs(): DriverCase {
  const sql = postgres(PG_URL, { max: 30, onnotice: () => {} });
  return {
    name: 'postgres.js',
    driver: toSqlDriver(sql),
    notifier: () => PgNotifier.postgresJs(sql),
    end: () => sql.end({ timeout: 1 }),
  };
}

function kysely(): DriverCase {
  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: PG_URL, max: 30 }) }),
  });
  return {
    name: 'kysely',
    driver: fromKysely(db),
    notifier: () => PgNotifier.nodePostgres(() => new pg.Client(PG_URL)),
    end: () => db.destroy(),
  };
}

let seq = 0;
const newTopic = () => `t${process.pid}_${Date.now() % 1e8}_${++seq}`;
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
};

for (const make of [nodePostgres, postgresJs, kysely]) {
  const d = make();
  const { driver } = d;
  const testApi = { describe, it, afterEach };
  const logTopic = async () => {
    const t = newTopic();
    await initializeSchema(driver, { logTopics: [t] });
    return t;
  };
  const queueTopic = async () => {
    const t = newTopic();
    await initializeSchema(driver, { queueTopics: [t] });
    return t;
  };
  const count = async (sql: string, params: unknown[] = []) => Number((await driver.query(sql, params)).rows[0].n);

  describe(`[${d.name}]`, () => {
    beforeAll(async () => {
      await initializeSchema(driver, { inbox: true });
      await driver.query('CREATE TABLE IF NOT EXISTS business (id SERIAL PRIMARY KEY, uuid TEXT, note TEXT)');
    });
    afterAll(() => d.end());

    pubSubContract(testApi, 'PgSubscriber (log)', PgSubscriber.features, async () => {
      const subs: PgSubscriber[] = [];
      return {
        publisher: new PgPublisher(driver),
        subscriber: (group) => {
          const s = new PgSubscriber(driver, { ...fast, consumerGroup: group });
          subs.push(s);
          return s;
        },
        topic: logTopic,
        close: async () => void (await Promise.all(subs.map((s) => s.close()))),
      };
    });

    pubSubContract(testApi, 'PgQueueSubscriber (queue)', PgQueueSubscriber.features, async () => {
      const subs: PgQueueSubscriber[] = [];
      return {
        publisher: new PgQueuePublisher(driver),
        subscriber: () => {
          const s = new PgQueueSubscriber(driver, fast);
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
        const topic = await logTopic();
        const original = newMessage(Buffer.from([0, 255, 1, 254]), { a: 'x', quote: '"{}"', empty: '' });
        await new PgPublisher(driver).publish(topic, original);
        const [m] = await collect(new PgSubscriber(driver, fast).subscribe(topic, ctrl()), 1);
        expect(m!.equals(original)).toBe(true);
      });

      it('does not lose a message whose transaction commits after a later one (xid8 watermark)', async () => {
        const topic = await logTopic();
        const t1Inserted = deferred();
        const commitT1 = deferred();
        const t1 = driver.transaction(async (tx) => {
          await new PgPublisher(tx).publish(topic, newMessage('first-inserted'));
          t1Inserted.resolve();
          await commitT1.promise;
        });
        await t1Inserted.promise;
        // Larger offset, but commits first.
        await driver.transaction((tx) => new PgPublisher(tx).publish(topic, newMessage('second-inserted')));

        const iter = new PgSubscriber(driver, fast).subscribe(topic, ctrl())[Symbol.asyncIterator]();
        // t1 still running → xmin is held back → t2's row is not readable yet.
        const first = iter.next();
        expect(await Promise.race([first, delay(400).then(() => 'blocked')])).toBe('blocked');

        commitT1.resolve();
        await t1;
        const r = await first;
        if (r.done) throw new Error('subscription ended');
        r.value.ack();
        const got = [r.value, ...(await collect(iter, 1))];
        expect(payloads(got)).toEqual(['first-inserted', 'second-inserted']);
      });

      it('a rolled-back publish is never delivered', async () => {
        const topic = await logTopic();
        await withTransaction(driver, async (tx) => {
          await new PgPublisher(tx).publish(topic, newMessage('ghost'));
          throw new Error('rollback');
        }).catch(() => {});
        await new PgPublisher(driver).publish(topic, newMessage('real'));
        const got = await collect(new PgSubscriber(driver, fast).subscribe(topic, ctrl()), 1);
        expect(payloads(got)).toEqual(['real']);
      });

      it('same consumer group: single active consumer, no duplicates', async () => {
        const topic = await logTopic();
        await new PgPublisher(driver).publish(topic, ...msgs(20));
        const seen: string[] = [];
        const run = (s: PgSubscriber) =>
          collect(s.subscribe(topic, ctrl()), 20, {
            timeoutMs: 15_000,
            onMessage: (m) => {
              seen.push(m.payload.toString());
              m.ack();
            },
          }).catch(() => {});
        void run(new PgSubscriber(driver, { ...fast, consumerGroup: 'g' }));
        void run(new PgSubscriber(driver, { ...fast, consumerGroup: 'g' }));
        await eventually(() => seen.length >= 20);
        await delay(200);
        expect(seen).toEqual(payloads(msgs(20))); // ordered, each exactly once
      });

      it('handler writes via the subscriber tx commit atomically with the ack (exactly-once effect)', async () => {
        const topic = await logTopic();
        const router = newRouter();
        let attempts = 0;
        router.addConsumerHandler({
          name: 'eo',
          subscribeTopic: topic,
          subscriber: new PgSubscriber(driver, fast),
          handler: async (m) => {
            attempts++;
            await txFromContext()!.query('INSERT INTO business (uuid, note) VALUES ($1, $2)', [m.uuid, topic]);
            if (attempts === 1) throw new Error('fail after write'); // nack → savepoint rollback
          },
        });
        void router.run();
        await router.running;
        const m = newMessage('x');
        await new PgPublisher(driver).publish(topic, m);
        await eventually(() => attempts === 2);
        await router.close();
        expect(await count('SELECT count(*) AS n FROM business WHERE uuid = $1', [m.uuid])).toBe(1);
      });

      it('ack deadline rolls back and redelivers', async () => {
        const topic = await logTopic();
        await new PgPublisher(driver).publish(topic, newMessage('slow'));
        const s = new PgSubscriber(driver, { ...fast, ackDeadlineMs: 200 });
        const got = await collect(s.subscribe(topic, ctrl()), 2, {
          onMessage: (m, i) => {
            if (i === 1) m.ack(); // first delivery: never answered
          },
        });
        expect(got[0]!.uuid).toBe(got[1]!.uuid);
      });

      it('LISTEN/NOTIFY wakes the subscriber well before the poll interval', async () => {
        const topic = await logTopic();
        const notifier = d.notifier();
        try {
          const sub = new PgSubscriber(driver, { pollIntervalMs: 10_000, notifier });
          const iter = sub.subscribe(topic, ctrl())[Symbol.asyncIterator]();
          const first = nextWithTimeout(iter, 3000);
          await delay(300); // let it poll once and go to sleep
          const t0 = Date.now();
          await new PgPublisher(driver).publish(topic, newMessage('ping'));
          const r = await first;
          expect(r).not.toBe('timeout');
          expect(Date.now() - t0).toBeLessThan(1500);
          if (r !== 'timeout' && !r.done) r.value.ack();
        } finally {
          await notifier.close();
        }
      });

      it('outbox: business write + ForwarderPublisher in one tx, relayed to the broker', async () => {
        const outboxTopic = await logTopic();
        const broker = new MemoryPubSub();
        const dest = broker.subscribe('orders.created');
        const router = newRouter();
        addForwarder(router, {
          subscriber: new PgSubscriber(driver, fast),
          publisher: broker,
          forwarderTopic: outboxTopic,
        });
        void router.run();
        await router.running;

        const ok = newMessage('order-1', { correlation_id: 'c1' });
        await withTransaction(driver, async (tx) => {
          await tx.query('INSERT INTO business (uuid, note) VALUES ($1, $2)', [ok.uuid, 'order']);
          await new ForwarderPublisher(new PgPublisher(tx), outboxTopic).publish('orders.created', ok);
        });
        await withTransaction(driver, async (tx) => {
          await new ForwarderPublisher(new PgPublisher(tx), outboxTopic).publish('orders.created', newMessage('nope'));
          throw new Error('business failed');
        }).catch(() => {});

        const [m] = await collect(dest, 1);
        expect(m!.equals(ok)).toBe(true);
        expect(await nextWithTimeout(dest[Symbol.asyncIterator](), 300)).toBe('timeout');
        await broker.close();
      });

      it('cqrs: EventBus writes events in the business transaction; each handler is its own consumer group', async () => {
        const topic = await logTopic();
        const OrderPlaced = defineEvent('order.placed.v1', z.object({ orderId: z.string(), total: z.number() }));
        const bus = new EventBus({ publisher: new PgPublisher(contextExecutor(driver)), topic: () => topic });
        const router = newRouter();
        const processor = new EventProcessor({
          router,
          topic: () => topic,
          subscriber: ({ handlerName }) => new PgSubscriber(driver, { ...fast, consumerGroup: handlerName }),
        });
        const got: string[] = [];
        processor.on(OrderPlaced, 'billing', (e) => void got.push(`billing:${e.orderId}`));
        processor.on(OrderPlaced, 'email', (e) => void got.push(`email:${e.orderId}`));
        void router.run();
        await router.running;

        await withTransaction(driver, async (tx) => {
          await tx.query('INSERT INTO business (uuid, note) VALUES ($1, $2)', ['o1', topic]);
          await bus.publish(OrderPlaced, { orderId: 'o1', total: 10 }); // joins tx via contextExecutor
        });
        await withTransaction(driver, async () => {
          await bus.publish(OrderPlaced, { orderId: 'o2', total: 20 });
          throw new Error('business rule failed'); // event rolled back with the data
        }).catch(() => {});
        await bus.publish(OrderPlaced, { orderId: 'o3', total: 30 }); // outside a tx: autocommit

        await eventually(() => got.length === 4);
        await delay(200);
        expect(got.filter((g) => g.startsWith('billing'))).toEqual(['billing:o1', 'billing:o3']);
        expect(got.filter((g) => g.startsWith('email'))).toEqual(['email:o1', 'email:o3']);
      });

      it('delayed requeue: a failing message moves aside, the topic keeps flowing, then it is retried', async () => {
        const topic = await logTopic();
        const requeueTopic = await queueTopic();
        const router = newRouter();
        const { middlewares } = pgDelayedRequeuer({
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
          subscriber: new PgSubscriber(driver, fast),
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
        await new PgPublisher(driver).publish(topic, newMessage('bad'), newMessage('good'));
        await eventually(() => handled.length === 2);
        expect(handled.map((h) => h.p)).toEqual(['good', 'bad']); // not head-of-line blocked
        expect(handled[1]!.retries).toBe('1');
        expect(handled[1]!.at - t0).toBeGreaterThanOrEqual(250);
        expect(await count(`SELECT count(*) AS n FROM "millstream_queue_${requeueTopic}"`)).toBe(0);
      });

      it('poison queue publish joins the subscriber tx via contextExecutor', async () => {
        const topic = await logTopic();
        const poison = await logTopic();
        const router = newRouter();
        router.addConsumerHandler({
          name: 'p',
          subscribeTopic: topic,
          subscriber: new PgSubscriber(driver, fast),
          middlewares: [poisonQueue(new PgPublisher(contextExecutor(driver)), poison)],
          handler: () => {
            throw new Error('always');
          },
        });
        void router.run();
        await router.running;
        await new PgPublisher(driver).publish(topic, newMessage('x'));
        const [p] = await collect(new PgSubscriber(driver, fast).subscribe(poison, ctrl()), 1);
        expect(p!.metadata.reason_poisoned).toBe('always');
      });

      it('inbox middleware dedupes redelivered/duplicate messages', async () => {
        const topic = await logTopic();
        const router = newRouter();
        let runs = 0;
        router.addConsumerHandler({
          name: 'dedup',
          subscribeTopic: topic,
          subscriber: new PgSubscriber(driver, fast),
          middlewares: [inbox({ consumer: `dedup_${topic}` })],
          handler: () => void runs++,
        });
        void router.run();
        await router.running;
        const m = newMessage('once');
        await new PgPublisher(driver).publish(topic, m);
        await new PgPublisher(driver).publish(topic, m.copy(), newMessage('other'));
        await eventually(() => runs === 2);
        await delay(300);
        expect(runs).toBe(2);
      });

      it('pruneInbox deletes old records in batches, optionally per consumer', async () => {
        const c1 = `prune_${newTopic()}`;
        const c2 = `prune_${newTopic()}`;
        const insert = (consumer: string, uuid: string, ageSql: string) =>
          driver.query(
            `INSERT INTO millstream_inbox (consumer, uuid, processed_at) VALUES ($1, $2, now() - ${ageSql})`,
            [consumer, uuid],
          );
        for (let i = 0; i < 5; i++) await insert(c1, `old${i}`, "interval '2 hours'");
        await insert(c1, 'fresh', "interval '1 second'");
        await insert(c2, 'old', "interval '2 hours'");

        expect(await pruneInbox(driver, { olderThanMs: 3_600_000, consumer: c1, batchSize: 2 })).toBe(5);
        expect(await count('SELECT count(*) AS n FROM millstream_inbox WHERE consumer = $1', [c1])).toBe(1); // 'fresh' kept
        expect(await count('SELECT count(*) AS n FROM millstream_inbox WHERE consumer = $1', [c2])).toBe(1); // other consumer untouched
        expect(await pruneInbox(driver, { olderThanMs: 3_600_000 })).toBeGreaterThanOrEqual(1);
        expect(await count('SELECT count(*) AS n FROM millstream_inbox WHERE consumer = $1', [c2])).toBe(0);
        await expect(pruneInbox(driver, { olderThanMs: 0 })).rejects.toThrow(/olderThanMs/);
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
            await txFromContext()!.query('INSERT INTO business (uuid, note) VALUES ($1, $2)', [m.uuid, consumer]);
          },
        });
        void router.run();
        await router.running;
        const m = newMessage('x');
        await ps.publish('t', m, m.copy());
        await eventually(
          async () =>
            (await count('SELECT count(*) AS n FROM business WHERE note = $1', [consumer])) === 1 && runs === 1,
        );
        await ps.close();
      });
    });
  });
}
