import { MemoryPubSub, Router, newMessage, newUUID, withDelay } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { withTransaction } from '@millstream/pg';
import { collect, delay, eventually, msgs, nextWithTimeout, payloads, pubSubContract } from '@millstream/test-kit';
import { makeWorkerUtils, runMigrations } from 'graphile-worker';
import pg from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  GraphileWorkerPublisher,
  GraphileWorkerSubscriber,
  type GraphileWorkerSubscriberOptions,
  InvalidConsumerGroupError,
  NackedError,
  consumerGroupTaskIdentifier,
  graphileJob,
} from '../src/index.js';

const PG_URL = process.env.PG_URL ?? 'postgres://postgres:postgres@localhost:55432/postgres';
const schema = `gw_test_${process.pid}`;
const pool = new pg.Pool({ connectionString: PG_URL, max: 30 });
pool.on('error', () => {}); // Graphile warns about pools without error handlers
const fast = { schema, pollInterval: 100, nackResendMs: 10 } satisfies Partial<GraphileWorkerSubscriberOptions>;

const newTopic = () => `t_${Date.now() % 1e8}_${newUUID().slice(0, 8)}`;
const jobsOf = async (task: string) =>
  (
    await pool.query(
      `SELECT j.* FROM "${schema}"._private_jobs j JOIN "${schema}"._private_tasks t ON t.id = j.task_id WHERE t.identifier = $1`,
      [task],
    )
  ).rows;

beforeAll(async () => {
  await runMigrations({ pgPool: pool, schema });
});
afterAll(async () => {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});

pubSubContract({ describe, it, afterEach }, 'GraphileWorkerSubscriber', GraphileWorkerSubscriber.features, async () => {
  const publisher = new GraphileWorkerPublisher(pool, { schema });
  const subs: GraphileWorkerSubscriber[] = [];
  return {
    publisher,
    subscriber: () => {
      const s = new GraphileWorkerSubscriber({ pgPool: pool, ...fast });
      subs.push(s);
      return s;
    },
    topic: async () => newTopic(),
    close: async () => {
      await Promise.all(subs.map((s) => s.close()));
      await publisher.close();
    },
  };
});

describe('Graphile Worker specifics', () => {
  const publisher = new GraphileWorkerPublisher(pool, { schema });
  const controllers: AbortController[] = [];
  const signal = () => {
    const c = new AbortController();
    controllers.push(c);
    return c.signal;
  };
  const sub = (opts: Partial<GraphileWorkerSubscriberOptions> = {}) =>
    new GraphileWorkerSubscriber({ pgPool: pool, ...fast, ...opts });
  const routers: Router[] = [];
  afterEach(async () => {
    for (const c of controllers.splice(0)) c.abort();
    await Promise.all(routers.splice(0).map((r) => r.close()));
    await delay(50); // let runners shut down before the next test
  });

  it('keeps uuid / metadata / binary payloads and exposes the job', async () => {
    const topic = newTopic();
    const text = newMessage('hi', { a: 'b' });
    const binary = newMessage(Buffer.from([0, 1, 2, 255]), { c: 'd' });
    await publisher.publish(topic, text, binary);
    const got = await collect(sub().subscribe(topic, signal()), 2);
    expect(got[0]!.equals(text)).toBe(true);
    expect(got[1]!.equals(binary)).toBe(true);
    expect(graphileJob(got[0]!)).toMatchObject({ task_identifier: topic, attempts: 1 });
    await eventually(async () => (await jobsOf(topic)).length === 0); // completed jobs are deleted
  });

  it('publishes inside a business transaction: rolled back → no job', async () => {
    const topic = newTopic();
    await expect(
      withTransaction(pool, async (tx) => {
        await new GraphileWorkerPublisher(tx, { schema }).publish(topic, newMessage('rolled back'));
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');
    await withTransaction(pool, (tx) => new GraphileWorkerPublisher(tx, { schema }).publish(topic, newMessage('kept')));
    const iter = sub().subscribe(topic, signal())[Symbol.asyncIterator]();
    expect(payloads(await collect(iter, 1))).toEqual(['kept']);
    expect(await nextWithTimeout(iter, 300)).toBe('timeout');
  });

  it("nack: 'redeliver' redelivers in place without consuming an attempt", async () => {
    const topic = newTopic();
    await new GraphileWorkerPublisher(pool, { schema, jobSpec: () => ({ maxAttempts: 1 }) }).publish(
      topic,
      newMessage('x'),
    );
    const got = await collect(sub().subscribe(topic, signal()), 4, {
      onMessage: (m, i) => void (i < 3 ? m.nack() : m.ack()),
    });
    expect(got).toHaveLength(4);
    expect(graphileJob(got[3]!)?.attempts).toBe(1);
  });

  it("nack: 'fail' hands retries to Graphile, then keeps the job as permanently failed", async () => {
    const topic = newTopic();
    await new GraphileWorkerPublisher(pool, { schema, jobSpec: () => ({ maxAttempts: 2 }) }).publish(
      topic,
      newMessage('flaky'),
    );
    const iter = sub({ nack: 'fail' }).subscribe(topic, signal())[Symbol.asyncIterator]();
    // Attempt 1 fails; Graphile's backoff (exp(1) ≈ 2.7s) delays attempt 2.
    const got = await collect(iter, 2, { onMessage: (m) => void m.nack(), timeoutMs: 8000 });
    expect(got.map((g) => graphileJob(g)?.attempts)).toEqual([1, 2]);
    await eventually(async () => {
      const [job] = await jobsOf(topic);
      return job?.attempts === 2 && job.locked_at === null;
    });
    const [job] = await jobsOf(topic);
    expect(job.last_error).toBe(new NackedError('nack').message);
  });

  it("jobSpec: jobKey = uuid + 'unsafe_dedupe' ignores a re-published message", async () => {
    const topic = newTopic();
    const pub = new GraphileWorkerPublisher(pool, {
      schema,
      jobSpec: (_t, m) => ({ jobKey: m.uuid, jobKeyMode: 'unsafe_dedupe' }),
    });
    const m = newMessage('once');
    await pub.publish(topic, m);
    await pub.publish(topic, m.copy());
    expect(await jobsOf(topic)).toHaveLength(1);
  });

  it('keeps publish order for large batches', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(100));
    const got = await collect(sub().subscribe(topic, signal()), 100);
    expect(payloads(got)).toEqual(payloads(msgs(100)));
  });

  it('_millstream_delayed_until becomes run_at', async () => {
    const topic = newTopic();
    const t0 = Date.now();
    await publisher.publish(topic, withDelay(newMessage('later'), 500));
    const [got] = await collect(sub().subscribe(topic, signal()), 1);
    expect(got!.payload.toString()).toBe('later');
    expect(Date.now() - t0).toBeGreaterThanOrEqual(450);
  });

  it('jobSpec: named queues run serially, priority runs first', async () => {
    const topic = newTopic();
    await new GraphileWorkerPublisher(pool, {
      schema,
      jobSpec: (_t, m) => ({ queueName: `${topic}-q`, priority: m.payload.toString() === 'urgent' ? -1 : 0 }),
    }).publish(topic, newMessage('a'), newMessage('b'), newMessage('urgent'));
    let active = 0;
    let peak = 0;
    const got = await collect(sub({ concurrency: 3 }).subscribe(topic, signal()), 3, {
      onMessage: (m) => {
        peak = Math.max(peak, ++active);
        setTimeout(() => {
          active--;
          m.ack();
        }, 30);
      },
    });
    expect(payloads(got)).toEqual(['urgent', 'a', 'b']);
    expect(peak).toBe(1);
  });

  it('concurrency: runs several jobs in parallel', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(6));
    let active = 0;
    let peak = 0;
    await collect(sub({ concurrency: 3 }).subscribe(topic, signal()), 6, {
      onMessage: (m) => {
        peak = Math.max(peak, ++active);
        setTimeout(() => {
          active--;
          m.ack();
        }, 50);
      },
    });
    expect(peak).toBe(3);
  });

  it('consumer groups: one job per group, each group runs its own', async () => {
    const topic = newTopic();
    expect(consumerGroupTaskIdentifier(topic, 'billing')).toBe(`${topic}.billing`);
    expect(() => consumerGroupTaskIdentifier(topic, '')).toThrow(InvalidConsumerGroupError);
    await new GraphileWorkerPublisher(pool, { schema, consumerGroups: () => ['billing', 'email'] }).publish(
      topic,
      ...msgs(3),
    );
    const [b, e] = await Promise.all([
      collect(sub({ consumerGroup: 'billing' }).subscribe(topic, signal()), 3),
      collect(sub({ consumerGroup: 'email' }).subscribe(topic, signal()), 3),
    ]);
    expect(payloads(b)).toEqual(payloads(msgs(3)));
    expect(payloads(e)).toEqual(payloads(msgs(3)));
  });

  it('jobs added by plain Graphile code are read too', async () => {
    const topic = newTopic();
    const utils = await makeWorkerUtils({ pgPool: pool, schema });
    const job = await utils.addJob(topic, { hello: 'world' });
    await utils.release();
    const [m] = await collect(sub().subscribe(topic, signal()), 1);
    expect(m!.uuid).toBe(`graphile:${job.id}`);
    expect(JSON.parse(m!.payload.toString())).toEqual({ hello: 'world' });
  });

  it('forwarder relays an outbox to Graphile Worker', async () => {
    const topic = newTopic();
    const outbox = new MemoryPubSub();
    const router = new Router();
    routers.push(router);
    addForwarder(router, { subscriber: outbox, publisher, forwarderTopic: 'outbox' });
    void router.run();
    await router.running;
    const original = newMessage('order-created', { key: 'order-9' });
    await new ForwarderPublisher(outbox, 'outbox').publish(topic, original);
    const [m] = await collect(sub().subscribe(topic, signal()), 1);
    expect(m!.equals(original)).toBe(true);
    await outbox.close();
  });
});
