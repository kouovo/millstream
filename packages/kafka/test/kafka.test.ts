import { Admin, Producer } from '@platformatic/kafka';
import { MemoryPubSub, Router, newMessage, newUUID } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { collect, delay, eventually, msgs, payloads, pubSubContract } from '@millstream/test-kit';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  KafkaPublisher,
  KafkaSubscriber,
  type KafkaSubscriberOptions,
  WATERMILL_UUID_HEADER,
  kafkaInfo,
} from '../src/index.js';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:59092').split(',');
const admin = new Admin({ clientId: 'millstream-test', bootstrapBrokers: BROKERS });
const fast: Partial<KafkaSubscriberOptions> = {
  nackResendMs: 10,
  consume: { maxWaitTime: 100 },
};

let seq = 0;
async function newTopic(partitions = 1): Promise<string> {
  const topic = `t${process.pid}_${Date.now() % 1e8}_${++seq}`;
  await admin.createTopics({ topics: [topic], partitions, replicas: 1 });
  return topic;
}

afterAll(() => admin.close());

pubSubContract({ describe, it, afterEach }, 'KafkaSubscriber', KafkaSubscriber.features, async () => {
  const publisher = new KafkaPublisher({ bootstrapBrokers: BROKERS });
  const subs: KafkaSubscriber[] = [];
  const groupPrefix = newUUID(); // contract groups ('a', 'g', …) must be unique per test
  return {
    publisher,
    subscriber: (group) => {
      const s = new KafkaSubscriber({
        bootstrapBrokers: BROKERS,
        consumerGroup: group ? `${groupPrefix}-${group}` : undefined,
        ...fast,
      });
      subs.push(s);
      return s;
    },
    topic: () => newTopic(1),
    close: async () => {
      await Promise.all(subs.map((s) => s.close()));
      await publisher.close();
    },
  };
});

describe('KafkaSubscriber specifics', () => {
  const publisher = new KafkaPublisher({
    bootstrapBrokers: BROKERS,
    partitionKey: (_t, m) => m.metadata.key,
  });
  const controllers: AbortController[] = [];
  const signal = () => {
    const c = new AbortController();
    controllers.push(c);
    return c.signal;
  };
  const routers: Router[] = [];
  beforeAll(() => admin.metadata({ topics: [] }));
  afterEach(async () => {
    for (const c of controllers.splice(0)) c.abort();
    await Promise.all(routers.splice(0).map((r) => r.close()));
  });
  afterAll(() => publisher.close());

  it('exposes Kafka coordinates and keeps uuid/metadata across the wire', async () => {
    const topic = await newTopic();
    const original = newMessage('hi', { key: 'order-1', a: 'b' });
    await publisher.publish(topic, original);
    const [m] = await collect(
      new KafkaSubscriber({ bootstrapBrokers: BROKERS, ...fast }).subscribe(topic, signal()),
      1,
    );
    expect(m!.equals(original)).toBe(true);
    const info = kafkaInfo(m!)!;
    expect(info.topic).toBe(topic);
    expect(info.offset).toBe('0');
    expect(info.key?.toString()).toBe('order-1');
  });

  it("uuidHeader: interoperates with Go Watermill's `_watermill_message_uuid` header", async () => {
    const topic = await newTopic();
    const goStyle = new KafkaPublisher({ bootstrapBrokers: BROKERS, uuidHeader: WATERMILL_UUID_HEADER });
    const original = newMessage('from-go', { a: 'b' });
    await goStyle.publish(topic, original);
    await goStyle.close();
    const [same] = await collect(
      new KafkaSubscriber({ bootstrapBrokers: BROKERS, uuidHeader: WATERMILL_UUID_HEADER, ...fast }).subscribe(
        topic,
        signal(),
      ),
      1,
    );
    expect(same!.equals(original)).toBe(true);
    // A subscriber on the default header does not see a uuid → deterministic fallback, header kept as metadata.
    const [plain] = await collect(
      new KafkaSubscriber({ bootstrapBrokers: BROKERS, ...fast }).subscribe(topic, signal()),
      1,
    );
    expect(plain!.uuid).toBe(`kafka:${topic}:0:0`);
    expect(plain!.metadata[WATERMILL_UUID_HEADER]).toBe(original.uuid);
  });

  it('derives a deterministic uuid for records produced outside millstream', async () => {
    const topic = await newTopic();
    const raw = new Producer({ clientId: 'raw', bootstrapBrokers: BROKERS });
    await raw.send({ messages: [{ topic, value: Buffer.from('foreign') }] });
    await raw.close();
    const [m] = await collect(
      new KafkaSubscriber({ bootstrapBrokers: BROKERS, ...fast }).subscribe(topic, signal()),
      1,
    );
    expect(m!.uuid).toBe(`kafka:${topic}:0:0`);
  });

  it('partitionConcurrency: partitions run in parallel, each key stays in order', async () => {
    const topic = await newTopic(3);
    const keys = ['a', 'b', 'c', 'd', 'e', 'f'];
    const sent = keys.flatMap((key) => Array.from({ length: 5 }, (_, i) => newMessage(`${key}-${i}`, { key })));
    await publisher.publish(topic, ...sent);

    const router = new Router();
    routers.push(router);
    const seen = new Map<string, string[]>();
    let active = 0;
    let peak = 0;
    router.addConsumerHandler({
      name: 'lanes',
      subscribeTopic: topic,
      subscriber: new KafkaSubscriber({ bootstrapBrokers: BROKERS, partitionConcurrency: 3, ...fast }),
      concurrency: 3,
      handler: async (m) => {
        peak = Math.max(peak, ++active);
        await delay(20);
        active--;
        const key = m.metadata.key!;
        seen.set(key, [...(seen.get(key) ?? []), m.payload.toString()]);
      },
    });
    void router.run();
    await eventually(() => [...seen.values()].flat().length === sent.length, 15_000);
    for (const key of keys) expect(seen.get(key)).toEqual(Array.from({ length: 5 }, (_, i) => `${key}-${i}`));
    expect(peak).toBeGreaterThan(1); // actually parallel across partitions
  });

  it('a nacked message blocks only its own partition', async () => {
    const topic = await newTopic(2);
    const fines = ['k2', 'k3', 'k4', 'k5', 'k6'].map((key) => newMessage(`fine-${key}`, { key }));
    await publisher.publish(topic, newMessage('poison', { key: 'k1' }), ...fines);

    const order: Array<{ p: string; partition: number }> = [];
    let poisonDeliveries = 0;
    await collect(
      new KafkaSubscriber({ bootstrapBrokers: BROKERS, partitionConcurrency: 2, ...fast }).subscribe(topic, signal()),
      fines.length + 5,
      {
        onMessage: async (m) => {
          order.push({ p: m.payload.toString(), partition: kafkaInfo(m)!.partition });
          if (m.payload.toString() !== 'poison') return void m.ack();
          // Keep the poison message failing for a while (in-place redelivery).
          await delay(50);
          if (++poisonDeliveries < 5) m.nack();
          else m.ack();
        },
      },
    );
    const poisonPartition = order.find((o) => o.p === 'poison')!.partition;
    const lastPoison = order.map((o) => o.p).lastIndexOf('poison');
    const otherPartition = order.filter((o) => o.p !== 'poison' && o.partition !== poisonPartition);
    expect(poisonDeliveries).toBe(5);
    expect(otherPartition.length).toBeGreaterThan(0); // murmur2 is deterministic: some key lands elsewhere
    // …and those were processed while the poison message was still being redelivered.
    for (const o of otherPartition) expect(order.indexOf(o)).toBeLessThan(lastPoison);
  });

  it('forwarder relays an outbox to Kafka (PG-free here: in-memory outbox)', async () => {
    const topic = await newTopic();
    const outbox = new MemoryPubSub();
    const router = new Router();
    routers.push(router);
    addForwarder(router, { subscriber: outbox, publisher, forwarderTopic: 'outbox' });
    void router.run();
    await router.running;
    const original = newMessage('order-created', { key: 'order-9' });
    await new ForwarderPublisher(outbox, 'outbox').publish(topic, original);
    const [m] = await collect(
      new KafkaSubscriber({ bootstrapBrokers: BROKERS, ...fast }).subscribe(topic, signal()),
      1,
    );
    expect(m!.equals(original)).toBe(true);
    await outbox.close();
  });
});

describe('KafkaPublisher', () => {
  it('preserves publish order within a key', async () => {
    const topic = await newTopic(3);
    const publisher = new KafkaPublisher({ bootstrapBrokers: BROKERS, partitionKey: () => 'same' });
    await publisher.publish(topic, ...msgs(20));
    await publisher.close();
    const c = new AbortController();
    const got = await collect(
      new KafkaSubscriber({ bootstrapBrokers: BROKERS, ...fast }).subscribe(topic, c.signal),
      20,
    );
    c.abort();
    expect(payloads(got)).toEqual(payloads(msgs(20)));
  });
});
