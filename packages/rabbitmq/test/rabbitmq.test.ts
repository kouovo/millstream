import { type Message, MemoryPubSub, Router, newMessage, newUUID } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { collect, delay, eventually, msgs, nextWithTimeout, payloads, pubSubContract } from '@millstream/test-kit';
import { type ChannelModel, connect } from 'amqplib';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  InvalidConsumerGroupError,
  RabbitMQPublisher,
  RabbitMQSubscriber,
  type RabbitMQSubscriberOptions,
  WATERMILL_UUID_HEADER,
  defaultQueueName,
  rabbitmqInfo,
} from '../src/index.js';

const url = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:55672';
const fast = { nackResendMs: 10, reconnectMs: 100 };

const exchanges = new Set<string>();
const queues = new Set<string>();
const newTopic = (...groups: Array<string | undefined>) => {
  const t = `t${process.pid}_${Date.now() % 1e8}_${newUUID().slice(0, 8)}`;
  exchanges.add(t);
  for (const g of groups.length ? groups : [undefined]) queues.add(defaultQueueName(t, g));
  return t;
};

let admin: ChannelModel;
beforeAll(async () => {
  admin = await connect(url);
});
/** The first message (already requested) plus `n - 1` more. */
async function rest(first: Promise<IteratorResult<Message>>, iter: AsyncIterator<Message>, n = 3): Promise<Message[]> {
  const r = await first;
  if (r.done) throw new Error('ended');
  r.value.ack();
  return [r.value, ...(await collect(iter, n - 1))];
}

/** checkQueue closes the channel on 404, so use a throwaway one. */
async function queueExists(name: string): Promise<boolean> {
  const ch = await admin.createChannel();
  ch.on('error', () => {});
  try {
    await ch.checkQueue(name);
    return true;
  } catch {
    return false;
  } finally {
    await ch.close().catch(() => {});
  }
}

afterAll(async () => {
  const ch = await admin.createChannel();
  ch.on('error', () => {});
  for (const q of queues) await ch.deleteQueue(q).catch(() => {});
  for (const e of exchanges) await ch.deleteExchange(e).catch(() => {});
  await admin.close();
});

// Contract groups ('a', 'b', 'g') and the ungrouped subscriber ('default') are declared by the
// publisher, so messages published before a subscriber exists are kept — like any durable setup.
const CONTRACT_GROUPS = ['default', 'a', 'b', 'g'];
pubSubContract({ describe, it, afterEach }, 'RabbitMQSubscriber', RabbitMQSubscriber.features, async () => {
  const publisher = new RabbitMQPublisher({ url, consumerGroups: () => CONTRACT_GROUPS });
  const subs: RabbitMQSubscriber[] = [];
  return {
    publisher,
    subscriber: (group) => {
      const s = new RabbitMQSubscriber({ url, consumerGroup: group ?? 'default', ...fast });
      subs.push(s);
      return s;
    },
    topic: async () => newTopic(...CONTRACT_GROUPS),
    close: async () => {
      await Promise.all(subs.map((s) => s.close()));
      await publisher.close();
    },
  };
});

describe('RabbitMQ specifics', () => {
  const publisher = new RabbitMQPublisher({ url });
  const controllers: AbortController[] = [];
  const subs: RabbitMQSubscriber[] = [];
  const signal = () => {
    const c = new AbortController();
    controllers.push(c);
    return c.signal;
  };
  const subscriber = (opts: Partial<RabbitMQSubscriberOptions> = {}) => {
    const s = new RabbitMQSubscriber({ url, ...fast, ...opts } as RabbitMQSubscriberOptions);
    subs.push(s);
    return s;
  };
  const routers: Router[] = [];
  afterEach(async () => {
    for (const c of controllers.splice(0)) c.abort();
    await Promise.all(routers.splice(0).map((r) => r.close()));
    await Promise.all(subs.splice(0).map((s) => s.close()));
  });
  afterAll(() => publisher.close());

  it('keeps uuid / metadata across the wire and exposes delivery info', async () => {
    const topic = newTopic();
    const original = newMessage('hi', { a: 'b', 'x-key': 'v' });
    await publisher.publish(topic, original);
    const [m] = await collect(subscriber().subscribe(topic, signal()), 1);
    expect(m!.equals(original)).toBe(true);
    expect(rabbitmqInfo(m!)).toMatchObject({ exchange: topic, routingKey: topic, redelivered: false });
  });

  it('a nacked message comes back flagged as redelivered', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('x'));
    const got = await collect(subscriber().subscribe(topic, signal()), 2, {
      onMessage: (m, i) => void (i === 0 ? m.nack() : m.ack()),
    });
    expect(rabbitmqInfo(got[1]!)?.redelivered).toBe(true);
  });

  it('consumer groups: queues declared by the subscriber each get every later message', async () => {
    const topic = newTopic('billing', 'email');
    const billing = subscriber({ consumerGroup: 'billing' }).subscribe(topic, signal())[Symbol.asyncIterator]();
    const email = subscriber({ consumerGroup: 'email' }).subscribe(topic, signal())[Symbol.asyncIterator]();
    // Subscriptions start on the first next(): wait until both queues are bound.
    const firstB = billing.next();
    const firstE = email.next();
    await eventually(async () => (await queueExists(`${topic}.billing`)) && (await queueExists(`${topic}.email`)));
    const pub = new RabbitMQPublisher({ url, consumerGroups: () => [] }); // declares nothing
    await pub.publish(topic, ...msgs(3));
    await pub.close();
    const [b, e] = await Promise.all([rest(firstB, billing), rest(firstE, email)]);
    expect(payloads(b)).toEqual(payloads(msgs(3)));
    expect(payloads(e)).toEqual(payloads(msgs(3)));
  });

  it('subscribers of one group compete for its messages', async () => {
    const topic = newTopic('workers');
    const pub = new RabbitMQPublisher({ url, consumerGroups: () => ['workers'] });
    await pub.publish(topic, ...msgs(20));
    await pub.close();
    const seen: string[] = [];
    const counts = [0, 0];
    const take = (i: number) =>
      collect(subscriber({ consumerGroup: 'workers' }).subscribe(topic, signal()), 20, {
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

  it("nack: 'reject' dead-letters via the queue's x-dead-letter-exchange", async () => {
    const topic = newTopic();
    const dlx = `${topic}.dlx`;
    exchanges.add(dlx);
    queues.add(dlx);
    const queueOptions = () => ({ deadLetterExchange: dlx });
    // The dead-letter exchange is just another topic; declare its queue so nothing is dropped.
    const dead = subscriber().subscribe(dlx, signal())[Symbol.asyncIterator]();
    const deadNext = dead.next();
    await eventually(() => queueExists(dlx));
    const pub = new RabbitMQPublisher({ url, queueOptions });
    await pub.publish(topic, newMessage('poison'));
    await pub.close();
    const sub = subscriber({ queueOptions, nack: 'reject' });
    const iter = sub.subscribe(topic, signal())[Symbol.asyncIterator]();
    await collect(iter, 1, { onMessage: (m) => void m.nack() });
    expect(await nextWithTimeout(iter, 300)).toBe('timeout'); // not requeued

    const r = await deadNext;
    if (r.done) throw new Error('ended');
    r.value.ack();
    expect(r.value.payload.toString()).toBe('poison');
    expect(JSON.parse(r.value.metadata['x-death']!)[0].reason).toBe('rejected');
  });

  it('concurrency: handles several messages in parallel', async () => {
    const topic = newTopic();
    await publisher.publish(topic, ...msgs(6));
    let active = 0;
    let peak = 0;
    await collect(subscriber({ concurrency: 3 }).subscribe(topic, signal()), 6, {
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

  it("uuidHeader: interoperates with Go Watermill's `_watermill_message_uuid` header", async () => {
    const topic = newTopic();
    const ch = await admin.createChannel();
    await ch.assertExchange(topic, 'fanout', { durable: true });
    await ch.assertQueue(topic, { durable: true });
    await ch.bindQueue(topic, topic, topic);
    ch.publish(topic, '', Buffer.from('from-go'), { headers: { [WATERMILL_UUID_HEADER]: 'go-uuid', a: 'b' } });
    ch.publish(topic, '', Buffer.from('anonymous'));
    await ch.close();
    const [go, anon] = await collect(subscriber({ uuidHeader: WATERMILL_UUID_HEADER }).subscribe(topic, signal()), 2);
    expect(go!.uuid).toBe('go-uuid');
    expect(go!.metadata).toEqual({ a: 'b' });
    expect(anon!.uuid).toMatch(/^amqp:/);
  });

  it('re-subscribes after the broker closes the channel', async () => {
    const topic = newTopic();
    await publisher.publish(topic, newMessage('before'));
    const iter = subscriber().subscribe(topic, signal())[Symbol.asyncIterator]();
    const [first] = await collect(iter, 1);
    expect(first!.payload.toString()).toBe('before');
    // Deleting the queue cancels the consumer; the subscription re-declares it and goes on.
    const ch = await admin.createChannel();
    await ch.deleteQueue(topic);
    await ch.close();
    await delay(50);
    await eventually(() => queueExists(topic));
    await publisher.publish(topic, newMessage('after'));
    const [after] = await collect(iter, 1, { timeoutMs: 5000 });
    expect(after!.payload.toString()).toBe('after');
  });

  it('rejects an empty consumer group', () => {
    expect(() => new RabbitMQSubscriber({ url, consumerGroup: '' })).toThrow(InvalidConsumerGroupError);
  });

  it('forwarder relays an outbox to RabbitMQ', async () => {
    const topic = newTopic();
    const outbox = new MemoryPubSub();
    const router = new Router();
    routers.push(router);
    addForwarder(router, { subscriber: outbox, publisher, forwarderTopic: 'outbox' });
    void router.run();
    await router.running;
    const original = newMessage('order-created', { key: 'order-9' });
    await new ForwarderPublisher(outbox, 'outbox').publish(topic, original);
    const [m] = await collect(subscriber().subscribe(topic, signal()), 1);
    expect(m!.equals(original)).toBe(true);
    await outbox.close();
  });
});
