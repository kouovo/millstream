import { afterEach, describe, expect, it } from 'vitest';
import { MemoryPubSub } from '../src/memory.js';
import { newMessage } from '../src/message.js';
import { collect, payloads, pubSubContract } from '@millstream/test-kit';

let n = 0;
pubSubContract({ describe, it, afterEach }, 'MemoryPubSub', MemoryPubSub.features, async () => {
  const ps = new MemoryPubSub();
  return { publisher: ps, subscriber: () => ps, topic: async () => `t${++n}`, close: () => ps.close() };
});

describe('MemoryPubSub options', () => {
  it('persistent replays history to late subscribers', async () => {
    const ps = new MemoryPubSub({ persistent: true });
    await ps.publish('t', newMessage('a'), newMessage('b'));
    const got = await collect(ps.subscribe('t'), 2);
    expect(payloads(got)).toEqual(['a', 'b']);
    await ps.close();
  });

  it('blockPublishUntilSubscriberAck makes publish synchronous', async () => {
    const ps = new MemoryPubSub({ blockPublishUntilSubscriberAck: true });
    const sub = ps.subscribe('t');
    let acked = false;
    const consumer = collect(sub, 1, {
      onMessage: async (m) => {
        await new Promise((r) => setTimeout(r, 100));
        acked = true;
        m.ack();
      },
    });
    await ps.publish('t', newMessage('x'));
    expect(acked).toBe(true);
    await consumer;
    await ps.close();
  });
});

describe('Message', () => {
  it('ack/nack are idempotent and mutually exclusive', async () => {
    const m = newMessage('x');
    expect(m.ack()).toBe(true);
    expect(m.ack()).toBe(true);
    expect(m.nack()).toBe(false);
    expect(await m.settled).toBe('ack');
    const c = m.copy();
    expect(c.state).toBe('pending');
    expect(c.equals(m)).toBe(true);
  });
});
