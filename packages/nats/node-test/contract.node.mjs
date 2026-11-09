// The published dist on plain Node (node:test) — no TS, no vitest.
import { randomUUID } from 'node:crypto';
import { after, afterEach, before, describe, it } from 'node:test';
import { NatsPublisher, NatsSubscriber, sanitizeName } from '@millstream/nats';
import { pubSubContract } from '@millstream/test-kit';
import { jetstreamManager } from '@nats-io/jetstream';
import { connect } from '@nats-io/transport-node';

let nc;
const topics = [];
before(async () => {
  nc = await connect({ servers: process.env.NATS_URL ?? 'localhost:54222' });
});
after(async () => {
  const jsm = await jetstreamManager(nc);
  for (const t of topics) await jsm.streams.delete(sanitizeName(t)).catch(() => {});
  await nc.drain();
});

pubSubContract({ describe, it, afterEach }, 'NatsSubscriber (dist)', NatsSubscriber.features, async () => {
  const publisher = new NatsPublisher({ connection: nc });
  const subs = [];
  return {
    publisher,
    subscriber: (group) => {
      const s = new NatsSubscriber({ connection: nc, consumerGroup: group, nackResendMs: 10 });
      subs.push(s);
      return s;
    },
    topic: async () => {
      const t = `node.${randomUUID()}`;
      topics.push(t);
      return t;
    },
    close: async () => {
      await Promise.all(subs.map((s) => s.close()));
      await publisher.close();
    },
  };
});
