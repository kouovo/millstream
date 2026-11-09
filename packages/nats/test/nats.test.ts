import { MemoryPubSub, Router, newMessage, newUUID } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { collect, delay, msgs, nextWithTimeout, payloads, pubSubContract } from '@millstream/test-kit';
import { jetstream, jetstreamManager } from '@nats-io/jetstream';
import { headers } from '@nats-io/nats-core';
import { type NatsConnection, connect } from '@nats-io/transport-node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  NatsPublisher,
  NatsSubscriber,
  type NatsSubscriberOptions,
  WATERMILL_UUID_HEADER,
  natsInfo,
  sanitizeName,
} from '../src/index.js';

const servers = process.env.NATS_URL ?? 'localhost:54222';
const fast: Partial<NatsSubscriberOptions> = { nackResendMs: 10, reconnectMs: 100 };

let nc: NatsConnection;
const sub = (opts: Partial<NatsSubscriberOptions> = {}) => new NatsSubscriber({ connection: nc, ...fast, ...opts });
const topics = new Set<string>();
const newTopic = () => {
  const t = `t${process.pid}.${Date.now() % 1e8}.${newUUID().slice(0, 8)}`;
  topics.add(t);
  return t;
};

beforeAll(async () => {
  nc = await connect({ servers });
});
afterAll(async () => {
  const jsm = await jetstreamManager(nc);
  for (const t of topics) await jsm.streams.delete(sanitizeName(t)).catch(() => {});
  await nc.drain();
});

pubSubContract({ describe, it, afterEach }, 'NatsSubscriber', NatsSubscriber.features, async () => {
  const publisher = new NatsPublisher({ connection: nc });
  const subs: NatsSubscriber[] = [];
  return {
    publisher,
    subscriber: (group) => {
      const s = new NatsSubscriber({ connection: nc, consumerGroup: group, ...fast });
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

describe('NATS specifics', () => {
  let publisher: NatsPublisher;
  const controllers: AbortController[] = [];
  const signal = () => {
    const c = new AbortController();
    controllers.push(c);
    return c.signal;
  };
  const routers: Router[] = [];
  beforeAll(() => {
    publisher = new NatsPublisher({ connection: nc });
  });
  afterEach(async () => {
    for (const c of controllers.splice(0)) c.abort();
    await Promise.all(routers.splice(0).map((r) => r.close()));
  });

  it('keeps uuid / metadata across the wire and exposes stream coordinates', async () => {
    const topic = newTopic();
    const original = newMessage('hi', { a: 'b', 'X-Key': 'v' });
    await publisher.publish(topic, original);
    const [m] = await collect(sub().subscribe(topic, signal()), 1);
    expect(m!.equals(original)).toBe(true);
    expect(natsInfo(m!)).toMatchObject({ stream: sanitizeName(topic), streamSequence: 1, deliveryCount: 1 });
  });

  it('Nats-Msg-Id = uuid: re-publishing the same message is deduplicated by the stream', async () => {
    const topic = newTopic();
    const m = newMessage('once');
    await publisher.publish(topic, m);
    await publisher.publish(topic, m.copy());
    const iter = sub().subscribe(topic, signal())[Symbol.asyncIterator]();
    await collect(iter, 1);
    expect(await nextWithTimeout(iter, 500)).toBe('timeout');
  });

  it('a nacked message is redelivered with a higher delivery count', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('x'));
    const got = await collect(sub().subscribe(topic, signal()), 2, {
      onMessage: (m, i) => void (i === 0 ? m.nack() : m.ack()),
    });
    expect(natsInfo(got[1]!)?.deliveryCount).toBe(2);
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

  it('a durable consumer created with other settings is updated (e.g. a new concurrency)', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(4));
    const c1 = new AbortController();
    await collect(sub({ consumerGroup: 'g' }).subscribe(topic, c1.signal), 2);
    c1.abort();
    const got = await collect(sub({ consumerGroup: 'g', concurrency: 2 }).subscribe(topic, signal()), 2);
    expect(payloads(got).toSorted()).toEqual(['m-2', 'm-3']);
    const jsm = await jetstreamManager(nc);
    const info = await jsm.consumers.info(sanitizeName(topic), sanitizeName(`g_${topic}`));
    expect(info.config.max_ack_pending).toBe(2);
  });

  it("deliverPolicy 'new' skips what was published before", async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('old'));
    const iter = sub({ deliverPolicy: 'new' }).subscribe(topic, signal())[Symbol.asyncIterator]();
    const next = iter.next();
    await delay(300); // consumer created
    await publisher.publish(topic, newMessage('new'));
    const r = await next;
    expect(r.done ? null : r.value.payload.toString()).toBe('new');
  });

  it('concurrency: handles several messages in parallel', async () => {
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

  it('a slow handler is not redelivered after ack_wait (working() keep-alive)', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('slow'), newMessage('next'));
    const got = await collect(sub({ ackWaitMs: 1000 }).subscribe(topic, signal()), 2, {
      onMessage: async (m) => {
        await delay(2500);
        m.ack();
      },
    });
    expect(payloads(got)).toEqual(['slow', 'next']);
    expect(natsInfo(got[0]!)?.deliveryCount).toBe(1);
  });

  it('uuidHeader / foreign messages: Watermill header, or a deterministic stream-based uuid', async () => {
    const topic = newTopic();
    await new NatsPublisher({ connection: nc }).publish(topic, newMessage('provision'));
    const js = jetstream(nc);
    const h = headers();
    h.set(WATERMILL_UUID_HEADER, 'go-uuid');
    await js.publish(topic, 'from-go', { headers: h });
    await js.publish(topic, 'anonymous');
    const [, go, anon] = await collect(sub({ uuidHeader: WATERMILL_UUID_HEADER }).subscribe(topic, signal()), 3);
    expect(go!.uuid).toBe('go-uuid');
    expect(anon!.uuid).toBe(`nats:${sanitizeName(topic)}:3`);
  });

  it('forwarder relays an outbox to NATS', async () => {
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
