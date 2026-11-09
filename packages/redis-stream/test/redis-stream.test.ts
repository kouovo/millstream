import { MemoryPubSub, Router, newMessage, newUUID } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { collect, delay, eventually, msgs, nextWithTimeout, payloads, pubSubContract } from '@millstream/test-kit';
import { Redis } from 'ioredis';
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import {
  RedisStreamPublisher,
  RedisStreamSubscriber,
  type RedisStreamSubscriberOptions,
  redisStreamInfo,
} from '../src/index.js';

const client = new Redis(process.env.REDIS_URL ?? 'redis://localhost:56379');
const fast: Partial<RedisStreamSubscriberOptions> = { nackResendMs: 10, blockMs: 200 };

const created = new Set<string>();
const newTopic = () => {
  const t = `t${process.pid}_${Date.now() % 1e8}_${newUUID().slice(0, 8)}`;
  created.add(t);
  return t;
};
afterAll(async () => {
  if (created.size) await client.del(...created);
  client.disconnect();
});

pubSubContract({ describe, it, afterEach }, 'RedisStreamSubscriber', RedisStreamSubscriber.features, async () => {
  const publisher = new RedisStreamPublisher({ client });
  const subs: RedisStreamSubscriber[] = [];
  return {
    publisher,
    subscriber: (group) => {
      const s = new RedisStreamSubscriber({ client, consumerGroup: group, ...fast });
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

describe('Redis Streams specifics', () => {
  const publisher = new RedisStreamPublisher({ client });
  const controllers: AbortController[] = [];
  const signal = () => {
    const c = new AbortController();
    controllers.push(c);
    return c.signal;
  };
  const sub = (opts: Partial<RedisStreamSubscriberOptions> = {}) =>
    new RedisStreamSubscriber({ client, ...fast, ...opts });
  const routers: Router[] = [];
  afterEach(async () => {
    for (const c of controllers.splice(0)) c.abort();
    await Promise.all(routers.splice(0).map((r) => r.close()));
  });

  it('keeps uuid / metadata / binary payloads and exposes the entry id', async () => {
    const topic = newTopic();
    const original = newMessage(Buffer.from([0, 1, 2, 255]), { a: 'b' });
    await publisher.publish(topic, original);
    const [m] = await collect(sub({ consumerGroup: 'g' }).subscribe(topic, signal()), 1);
    expect(m!.equals(original)).toBe(true);
    expect(redisStreamInfo(m!)).toMatchObject({ stream: topic, consumerGroup: 'g' });
    expect(redisStreamInfo(m!)!.id).toMatch(/^\d+-\d+$/);
  });

  it('ack → XACK: nothing stays pending', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(3));
    await collect(sub({ consumerGroup: 'g' }).subscribe(topic, signal()), 3);
    await eventually(async () => ((await client.xpending(topic, 'g')) as [number])[0] === 0);
  });

  it('entries of a crashed consumer are claimed after claimIdleMs', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('orphan'));
    await client.xgroup('CREATE', topic, 'g', '0');
    // A consumer that read the entry and died without acking.
    await client.xreadgroup('GROUP', 'g', 'dead', 'COUNT', 1, 'STREAMS', topic, '>');
    const t0 = Date.now();
    const [m] = await collect(sub({ consumerGroup: 'g', claimIdleMs: 500 }).subscribe(topic, signal()), 1);
    expect(m!.payload.toString()).toBe('orphan');
    expect(Date.now() - t0).toBeGreaterThanOrEqual(300);
  });

  it('a slow handler keeps its entry (idle time reset while held)', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('slow'));
    const other = sub({ consumerGroup: 'g', claimIdleMs: 300 }).subscribe(topic, signal())[Symbol.asyncIterator]();
    const [m] = await collect(sub({ consumerGroup: 'g', claimIdleMs: 300 }).subscribe(topic, signal()), 1, {
      onMessage: async (msg) => {
        expect(await nextWithTimeout(other, 1500)).toBe('timeout');
        msg.ack();
      },
    });
    expect(m!.payload.toString()).toBe('slow');
  });

  it('subscribers of one group compete for its messages', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(20));
    const seen: string[] = [];
    const counts = [0, 0];
    const take = (i: number) =>
      collect(sub({ consumerGroup: 'workers' }).subscribe(topic, signal()), 20, {
        timeoutMs: 1500,
        onMessage: async (m) => {
          seen.push(m.payload.toString());
          counts[i]!++;
          await delay(10);
          m.ack();
        },
      }).catch(() => []);
    await Promise.all([take(0), take(1)]);
    expect(new Set(seen).size).toBe(20);
    expect(counts[0]).toBeGreaterThan(0);
    expect(counts[1]).toBeGreaterThan(0);
  });

  it('concurrency: handles several entries in parallel', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(6));
    let active = 0;
    let peak = 0;
    await collect(sub({ consumerGroup: 'g', concurrency: 3 }).subscribe(topic, signal()), 6, {
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

  it("startId '$' skips older entries (ungrouped and new groups)", async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('old'));
    for (const consumerGroup of [undefined, 'g']) {
      const iter = sub({ startId: '$', consumerGroup }).subscribe(topic, signal())[Symbol.asyncIterator]();
      const next = iter.next();
      await delay(300);
      await publisher.publish(topic, newMessage(`new-${consumerGroup}`));
      const r = await next;
      expect(r.done ? null : r.value.payload.toString()).toBe(`new-${consumerGroup}`);
      if (!r.done) r.value.ack();
    }
  });

  it('maxLen trims the stream; foreign entries get a stable uuid', async () => {
    const topic = newTopic();
    await new RedisStreamPublisher({ client, maxLen: () => 5 }).publish(topic, ...msgs(500));
    expect(await client.xlen(topic)).toBeLessThan(500);
    const other = newTopic();
    const id = await client.xadd(other, '*', 'hello', 'world');
    const [m] = await collect(sub().subscribe(other, signal()), 1);
    expect(m!.uuid).toBe(`redis:${other}:${id}`);
    expect(JSON.parse(m!.payload.toString())).toEqual({ hello: 'world' });
  });

  it('removes its consumer from the group when nothing is pending', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('x'));
    const c = new AbortController();
    await collect(sub({ consumerGroup: 'g' }).subscribe(topic, c.signal), 1);
    c.abort();
    await eventually(async () => ((await client.xinfo('CONSUMERS', topic, 'g')) as unknown[]).length === 0);
  });

  it('forwarder relays an outbox to a Redis stream', async () => {
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
    expect(payloads([m!])).toEqual(['order-created']);
    await outbox.close();
  });
});
