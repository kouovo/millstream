// The published dist on plain Node (node:test) — no TS, no vitest.
import { randomUUID } from 'node:crypto';
import { after, afterEach, describe, it } from 'node:test';
import { Admin } from '@platformatic/kafka';
import { KafkaPublisher, KafkaSubscriber } from '@millstream/kafka';
import { pubSubContract } from '@millstream/test-kit';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:59092').split(',');
const admin = new Admin({ clientId: 'node-test', bootstrapBrokers: brokers });
after(() => admin.close());

let seq = 0;
pubSubContract({ describe, it, afterEach }, 'KafkaSubscriber (dist)', KafkaSubscriber.features, async () => {
  const publisher = new KafkaPublisher({ bootstrapBrokers: brokers });
  const prefix = randomUUID();
  const subs = [];
  return {
    publisher,
    subscriber: (group) => {
      const s = new KafkaSubscriber({
        bootstrapBrokers: brokers,
        consumerGroup: group && `${prefix}-${group}`,
        nackResendMs: 10,
        consume: { maxWaitTime: 100 },
      });
      subs.push(s);
      return s;
    },
    topic: async () => {
      const topic = `node${process.pid}_${Date.now() % 1e8}_${++seq}`;
      await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
      return topic;
    },
    close: async () => {
      await Promise.all(subs.map((s) => s.close()));
      await publisher.close();
    },
  };
});
