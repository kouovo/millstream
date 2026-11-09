// The published dist on plain Node (node:test) — no TS, no vitest.
import { randomUUID } from 'node:crypto';
import { after, afterEach, describe, it } from 'node:test';
import { RedisStreamPublisher, RedisStreamSubscriber } from '@millstream/redis-stream';
import { pubSubContract } from '@millstream/test-kit';
import { Redis } from 'ioredis';

const client = new Redis(process.env.REDIS_URL ?? 'redis://localhost:56379');
const created = [];
after(async () => {
  if (created.length) await client.del(...created);
  client.disconnect();
});

pubSubContract(
  { describe, it, afterEach },
  'RedisStreamSubscriber (dist)',
  RedisStreamSubscriber.features,
  async () => {
    const publisher = new RedisStreamPublisher({ client });
    const subs = [];
    return {
      publisher,
      subscriber: (group) => {
        const s = new RedisStreamSubscriber({ client, consumerGroup: group, nackResendMs: 10, blockMs: 200 });
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
  },
);
