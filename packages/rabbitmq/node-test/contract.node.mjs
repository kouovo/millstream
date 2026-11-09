// The published dist on plain Node (node:test) — no TS, no vitest.
import { randomUUID } from 'node:crypto';
import { after, afterEach, describe, it } from 'node:test';
import { RabbitMQPublisher, RabbitMQSubscriber } from '@millstream/rabbitmq';
import { pubSubContract } from '@millstream/test-kit';
import { connect } from 'amqplib';

const url = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:55672';
const groups = ['default', 'a', 'b', 'g'];
const topics = [];
after(async () => {
  const conn = await connect(url);
  const ch = await conn.createChannel();
  ch.on('error', () => {});
  for (const t of topics) {
    for (const g of groups) await ch.deleteQueue(`${t}.${g}`).catch(() => {});
    await ch.deleteExchange(t).catch(() => {});
  }
  await conn.close();
});

pubSubContract({ describe, it, afterEach }, 'RabbitMQSubscriber (dist)', RabbitMQSubscriber.features, async () => {
  const publisher = new RabbitMQPublisher({ url, consumerGroups: () => groups });
  const subs = [];
  return {
    publisher,
    subscriber: (group) => {
      const s = new RabbitMQSubscriber({ url, consumerGroup: group ?? 'default', nackResendMs: 10 });
      subs.push(s);
      return s;
    },
    topic: async () => {
      const t = `node_${randomUUID()}`;
      topics.push(t);
      return t;
    },
    close: async () => {
      await Promise.all(subs.map((s) => s.close()));
      await publisher.close();
    },
  };
});
