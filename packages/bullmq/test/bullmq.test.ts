import { MemoryPubSub, Router, newMessage, newUUID, withDelay } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { collect, delay, eventually, msgs, nextWithTimeout, payloads, pubSubContract } from '@millstream/test-kit';
import { Queue } from 'bullmq';
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import {
  BullMQPublisher,
  BullMQSubscriber,
  type BullMQSubscriberOptions,
  InvalidConsumerGroupError,
  NackedError,
  bullmqJob,
  consumerGroupQueueName,
  toJobId,
} from '../src/index.js';

const url = new URL(process.env.REDIS_URL ?? 'redis://localhost:56379');
const connection = { host: url.hostname, port: Number(url.port || 6379) };
const fast: Partial<BullMQSubscriberOptions> = { nackResendMs: 10 };

const created = new Set<string>();
const newTopic = () => {
  const t = `t${process.pid}_${Date.now() % 1e8}_${newUUID().slice(0, 8)}`;
  created.add(t);
  return t;
};
/** A topic plus the queues of its consumer groups (all cleaned up after the run). */
const groupedTopic = (...groups: string[]) => {
  const t = newTopic();
  for (const g of groups) created.add(consumerGroupQueueName(t, g));
  return t;
};
afterAll(async () => {
  for (const name of created) {
    const q = new Queue(name, { connection });
    await q.obliterate({ force: true }).catch(() => {});
    await q.close();
  }
});

pubSubContract({ describe, it, afterEach }, 'BullMQSubscriber', BullMQSubscriber.features, async () => {
  const publisher = new BullMQPublisher({ connection });
  const subs: BullMQSubscriber[] = [];
  return {
    publisher,
    subscriber: () => {
      const s = new BullMQSubscriber({ connection, ...fast });
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

describe('BullMQ specifics', () => {
  const publisher = new BullMQPublisher({ connection });
  const controllers: AbortController[] = [];
  const signal = () => {
    const c = new AbortController();
    controllers.push(c);
    return c.signal;
  };
  const routers: Router[] = [];
  afterEach(async () => {
    for (const c of controllers.splice(0)) c.abort();
    await Promise.all(routers.splice(0).map((r) => r.close()));
  });
  afterAll(() => publisher.close());

  it('jobId = uuid: re-publishing the same message is deduplicated by BullMQ', async () => {
    const topic = newTopic();
    const m = newMessage('once');
    await publisher.publish(topic, m);
    await publisher.publish(topic, m.copy()); // e.g. forwarder retry after a crash
    const iter = new BullMQSubscriber({ connection, ...fast }).subscribe(topic, signal())[Symbol.asyncIterator]();
    const [got] = await collect(iter, 1);
    expect(got!.uuid).toBe(m.uuid);
    expect(bullmqJob(got!)?.id).toBe(m.uuid);
    expect(await nextWithTimeout(iter, 500)).toBe('timeout');
  });

  it('uuids that BullMQ would reject as job ids are escaped', async () => {
    expect(toJobId('kafka:orders:0:42')).toBe('kafka_orders_0_42');
    expect(toJobId('123')).toBe('id-123');
    const topic = newTopic();
    const m = newMessage('from kafka', {}, 'kafka:orders:0:42');
    await publisher.publish(topic, m);
    const [got] = await collect(new BullMQSubscriber({ connection, ...fast }).subscribe(topic, signal()), 1);
    expect(got!.equals(m)).toBe(true);
  });

  it('_millstream_delayed_until becomes a native BullMQ delay', async () => {
    const topic = newTopic();
    const t0 = Date.now();
    await publisher.publish(topic, withDelay(newMessage('later'), 400));
    const [got] = await collect(new BullMQSubscriber({ connection, ...fast }).subscribe(topic, signal()), 1);
    expect(got!.payload.toString()).toBe('later');
    expect(Date.now() - t0).toBeGreaterThanOrEqual(350);
  });

  it("nack: 'redeliver' does not consume BullMQ attempts", async () => {
    const topic = newTopic();
    await new BullMQPublisher({ connection, jobOptions: () => ({ attempts: 1 }) }).publish(topic, newMessage('x'));
    const got = await collect(new BullMQSubscriber({ connection, ...fast }).subscribe(topic, signal()), 4, {
      onMessage: (m, i) => void (i < 3 ? m.nack() : m.ack()),
    });
    expect(got).toHaveLength(4); // 3 nacks with attempts: 1 — still redelivered
    expect(bullmqJob(got[3]!)?.attemptsMade).toBe(0);
  });

  it("nack: 'fail' hands retries to BullMQ attempts/backoff, then keeps it in the failed set", async () => {
    const topic = newTopic();
    const pub = new BullMQPublisher({
      connection,
      jobOptions: () => ({ attempts: 3, backoff: { type: 'fixed', delay: 50 } }),
    });
    const m = newMessage('flaky');
    await pub.publish(topic, m);
    await pub.close();
    const iter = new BullMQSubscriber({ connection, nack: 'fail' }).subscribe(topic, signal())[Symbol.asyncIterator]();
    const got = await collect(iter, 3, { onMessage: (msg) => void msg.nack() });
    expect(got.map((g) => g.uuid)).toEqual([m.uuid, m.uuid, m.uuid]);
    const next = iter.next(); // keep it: a timed-out next() would swallow the replayed job
    expect(await Promise.race([next, delay(500).then(() => 'blocked')])).toBe('blocked'); // attempts exhausted

    const q = new Queue(topic, { connection });
    const [failed] = await q.getFailed();
    expect(failed?.id).toBe(m.uuid);
    expect(failed?.attemptsMade).toBe(3);
    expect(failed?.failedReason).toBe(new NackedError('nack').message);
    // Manual replay from the failed set (what a BullMQ dashboard does):
    await failed!.retry();
    const replayed = await next;
    expect(replayed.done ? null : replayed.value.uuid).toBe(m.uuid);
    if (!replayed.done) replayed.value.ack();
    await q.close();
  });

  it('jobs that are not millstream envelopes go to the failed set instead of blocking', async () => {
    const topic = newTopic();
    const q = new Queue(topic, { connection });
    await q.add('foreign', { hello: 'world' });
    await publisher.publish(topic, newMessage('ok'));
    const [got] = await collect(new BullMQSubscriber({ connection, ...fast }).subscribe(topic, signal()), 1);
    expect(got!.payload.toString()).toBe('ok');
    await eventually(async () => (await q.getFailedCount()) === 1);
    await q.close();
  });

  it('extends the job lock while a slow handler runs (no stalled duplicate)', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('slow'));
    const opts = { connection, ...fast, workerOptions: { lockDuration: 600, stalledInterval: 300 } };
    const seen: string[] = [];
    const a = collect(new BullMQSubscriber(opts).subscribe(topic, signal()), 1, {
      onMessage: async (m) => {
        seen.push('a');
        await delay(2000); // > 3× lockDuration
        m.ack();
      },
    });
    await eventually(() => seen.length === 1); // `a` holds the job
    // A competing consumer would get the job if its lock expired.
    const b = new BullMQSubscriber(opts).subscribe(topic, signal())[Symbol.asyncIterator]();
    expect(await nextWithTimeout(b, 2500)).toBe('timeout');
    await a;
    expect(seen).toEqual(['a']);
  });

  it('concurrency: jobs of one subscription run in parallel', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(9));
    const router = new Router();
    routers.push(router);
    let active = 0;
    let peak = 0;
    let done = 0;
    router.addConsumerHandler({
      name: 'par',
      subscribeTopic: topic,
      subscriber: new BullMQSubscriber({ connection, ...fast, concurrency: 3 }),
      concurrency: 3,
      handler: async () => {
        peak = Math.max(peak, ++active);
        await delay(50);
        active--;
        done++;
      },
    });
    void router.run();
    await eventually(() => done === 9);
    expect(peak).toBe(3);
  });

  it('competing subscriptions share the work (queue semantics, no duplicates)', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(20));
    const seen: string[] = [];
    const take = (s: BullMQSubscriber) =>
      collect(s.subscribe(topic, signal()), 20, {
        timeoutMs: 3000,
        onMessage: (m) => {
          seen.push(m.payload.toString());
          m.ack();
        },
      }).catch(() => {});
    void take(new BullMQSubscriber({ connection, ...fast }));
    void take(new BullMQSubscriber({ connection, ...fast }));
    await eventually(() => seen.length === 20);
    await delay(200);
    expect(seen.toSorted()).toEqual(payloads(msgs(20)).toSorted());
  });

  describe('consumer groups (fan-out)', () => {
    it('every group gets its own copy, subscribers of one group share it', async () => {
      const topic = groupedTopic('billing', 'email');
      const pub = new BullMQPublisher({ connection, consumerGroups: () => ['billing', 'email'] });
      await pub.publish(topic, ...msgs(6));
      await pub.close();

      const billing = await collect(
        new BullMQSubscriber({ connection, ...fast, consumerGroup: 'billing' }).subscribe(topic, signal()),
        6,
      );
      expect(new Set(payloads(billing))).toEqual(new Set(payloads(msgs(6))));

      // Two subscribers of 'email' compete: together they see each message once.
      const seen: string[] = [];
      const run = () =>
        collect(new BullMQSubscriber({ connection, ...fast, consumerGroup: 'email' }).subscribe(topic, signal()), 6, {
          timeoutMs: 5000,
          onMessage: (m) => {
            seen.push(m.payload.toString());
            m.ack();
          },
        }).catch(() => {});
      void run();
      void run();
      await eventually(() => seen.length === 6);
      await delay(200);
      expect(seen.toSorted()).toEqual(payloads(msgs(6)).toSorted());

      // Nothing went to the topic's shared queue.
      const shared = new Queue(topic, { connection });
      expect(await shared.count()).toBe(0);
      await shared.close();
    });

    it('groups retry and fail independently (per-group job options)', async () => {
      const topic = groupedTopic('flaky', 'ok');
      const pub = new BullMQPublisher({
        connection,
        consumerGroups: () => ['flaky', 'ok'],
        jobOptions: (_t, _m, { consumerGroup }) => ({
          attempts: consumerGroup === 'flaky' ? 2 : 1,
          backoff: { type: 'fixed', delay: 20 },
        }),
      });
      const m = newMessage('event');
      await pub.publish(topic, m);
      await pub.close();

      const flaky = new BullMQSubscriber({ connection, nack: 'fail', consumerGroup: 'flaky' });
      const failedDeliveries = await collect(flaky.subscribe(topic, signal()), 2, { onMessage: (x) => void x.nack() });
      expect(failedDeliveries.map((x) => x.uuid)).toEqual([m.uuid, m.uuid]);
      const [ok] = await collect(
        new BullMQSubscriber({ connection, ...fast, consumerGroup: 'ok' }).subscribe(topic, signal()),
        1,
      );
      expect(ok!.uuid).toBe(m.uuid);

      const flakyQueue = new Queue(consumerGroupQueueName(topic, 'flaky'), { connection });
      await eventually(async () => (await flakyQueue.getFailedCount()) === 1);
      await flakyQueue.close();
    });

    it('a repeated publish (e.g. forwarder retry) is deduplicated per group', async () => {
      const topic = groupedTopic('a', 'b');
      const pub = new BullMQPublisher({ connection, consumerGroups: () => ['a', 'b'] });
      const m = newMessage('once');
      await pub.publish(topic, m);
      await pub.publish(topic, m.copy());
      await pub.close();
      for (const g of ['a', 'b']) {
        const q = new Queue(consumerGroupQueueName(topic, g), { connection });
        expect(await q.count()).toBe(1);
        await q.close();
      }
    });

    it('[] means no consumers (nothing added); undefined keeps the shared queue', async () => {
      const none = newTopic();
      const shared = newTopic();
      const pub = new BullMQPublisher({
        connection,
        consumerGroups: (t) => (t === none ? [] : undefined),
      });
      await pub.publish(none, newMessage('dropped'));
      await pub.publish(shared, newMessage('kept'));
      await pub.close();
      const qNone = new Queue(none, { connection });
      expect(await qNone.count()).toBe(0);
      await qNone.close();
      const [got] = await collect(new BullMQSubscriber({ connection, ...fast }).subscribe(shared, signal()), 1);
      expect(got!.payload.toString()).toBe('kept');
    });

    it('rejects group names BullMQ cannot use', () => {
      expect(() => consumerGroupQueueName('t', '')).toThrow(InvalidConsumerGroupError);
      expect(() => new BullMQSubscriber({ connection, consumerGroup: 'a:b' })).toThrow(InvalidConsumerGroupError);
    });

    it('forwarder relays an outbox into every group', async () => {
      const topic = groupedTopic('projection', 'notify');
      const outbox = new MemoryPubSub();
      const router = new Router();
      routers.push(router);
      const pub = new BullMQPublisher({ connection, consumerGroups: () => ['projection', 'notify'] });
      addForwarder(router, { subscriber: outbox, publisher: pub, forwarderTopic: 'outbox' });
      void router.run();
      await router.running;
      const original = newMessage('todo-completed', { correlation_id: 'c1' });
      await new ForwarderPublisher(outbox, 'outbox').publish(topic, original);
      for (const consumerGroup of ['projection', 'notify']) {
        const [m] = await collect(
          new BullMQSubscriber({ connection, ...fast, consumerGroup }).subscribe(topic, signal()),
          1,
        );
        expect(m!.equals(original)).toBe(true);
      }
      await outbox.close();
      await pub.close();
    });
  });

  it("a middleware can skip BullMQ's remaining attempts for a permanent error (job.discard())", async () => {
    const topic = newTopic();
    const pub = new BullMQPublisher({ connection, jobOptions: () => ({ attempts: 5 }) });
    await pub.publish(topic, newMessage('invalid'));
    await pub.close();
    class ValidationError extends Error {}
    const router = new Router();
    routers.push(router);
    let calls = 0;
    router.addConsumerHandler({
      name: 'permanent',
      subscribeTopic: topic,
      subscriber: new BullMQSubscriber({ connection, nack: 'fail' }),
      middlewares: [
        (next) => async (msg) => {
          try {
            return await next(msg);
          } catch (e) {
            if (e instanceof ValidationError) bullmqJob(msg)?.discard();
            throw e;
          }
        },
      ],
      handler: () => {
        calls++;
        throw new ValidationError('never valid');
      },
    });
    void router.run();
    await router.running;
    const q = new Queue(topic, { connection });
    await eventually(async () => (await q.getFailedCount()) === 1);
    await delay(200);
    expect(calls).toBe(1);
    await q.close();
  });

  it('forwarder relays an outbox to BullMQ', async () => {
    const topic = newTopic();
    const outbox = new MemoryPubSub();
    const router = new Router();
    routers.push(router);
    addForwarder(router, { subscriber: outbox, publisher, forwarderTopic: 'outbox' });
    void router.run();
    await router.running;
    const original = newMessage('todo-completed', { correlation_id: 'c1' });
    await new ForwarderPublisher(outbox, 'outbox').publish(topic, original);
    const [m] = await collect(new BullMQSubscriber({ connection, ...fast }).subscribe(topic, signal()), 1);
    expect(m!.equals(original)).toBe(true);
    await outbox.close();
  });
});
