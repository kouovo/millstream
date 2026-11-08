// The published dist on plain Node (node:test) — no TS, no vitest.
import { randomUUID } from 'node:crypto';
import { after, afterEach, describe, it } from 'node:test';
import { BullMQPublisher, BullMQSubscriber } from '@millstream/bullmq';
import { pubSubContract } from '@millstream/test-kit';
import { Queue } from 'bullmq';

const url = new URL(process.env.REDIS_URL ?? 'redis://localhost:56379');
const connection = { host: url.hostname, port: Number(url.port || 6379) };
const created = [];
after(async () => {
  for (const name of created) {
    const q = new Queue(name, { connection });
    await q.obliterate({ force: true }).catch(() => {});
    await q.close();
  }
});

pubSubContract({ describe, it, afterEach }, 'BullMQSubscriber (dist)', BullMQSubscriber.features, async () => {
  const publisher = new BullMQPublisher({ connection });
  const subs = [];
  return {
    publisher,
    subscriber: () => {
      const s = new BullMQSubscriber({ connection, nackResendMs: 10 });
      subs.push(s);
      return s;
    },
    topic: async () => {
      const name = `node_${randomUUID()}`;
      created.push(name);
      return name;
    },
    close: async () => {
      await Promise.all(subs.map((s) => s.close()));
      await publisher.close();
    },
  };
});
