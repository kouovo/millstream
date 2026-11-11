import { afterEach, describe, expect, it } from 'vitest';
import { MemoryPubSub } from '../src/memory.js';
import { newMessage } from '../src/message.js';
import { Router } from '../src/router.js';
import { collect, delay, eventually, payloads, pubSubContract } from '@millstream/test-kit';

let n = 0;
pubSubContract({ describe, it, afterEach }, 'MemoryPubSub', MemoryPubSub.features, async () => {
  const ps = new MemoryPubSub();
  return { publisher: ps, subscriber: () => ps, topic: async () => `t${++n}`, close: () => ps.close() };
});

pubSubContract({ describe, it, afterEach }, 'MemoryPubSub (concurrency: 4)', MemoryPubSub.features, async () => {
  const ps = new MemoryPubSub({ concurrency: 4 });
  return { publisher: ps, subscriber: () => ps, topic: async () => `c${++n}`, close: () => ps.close() };
});

pubSubContract(
  { describe, it, afterEach },
  'MemoryPubSub (concurrency: 1)',
  { ...MemoryPubSub.features, ordered: true },
  async () => {
    const ps = new MemoryPubSub({ concurrency: 1 });
    return { publisher: ps, subscriber: () => ps, topic: async () => `o${++n}`, close: () => ps.close() };
  },
);

describe('MemoryPubSub concurrency', () => {
  it('rejects invalid values', () => {
    expect(() => new MemoryPubSub({ concurrency: 0 })).toThrow();
    expect(() => new MemoryPubSub({ concurrency: 1.5 })).toThrow();
    expect(() => new MemoryPubSub({ concurrency: Infinity })).not.toThrow();
  });

  it('Infinity hands out every message without waiting for acks (GoChannel)', async () => {
    const ps = new MemoryPubSub({ concurrency: Infinity });
    const ac = new AbortController();
    const iter = ps.subscribe('t', ac.signal)[Symbol.asyncIterator]();
    const all = Array.from({ length: 50 }, (_, i) => newMessage(String(i)));
    await ps.publish('t', ...all);
    const held = [];
    for (let i = 0; i < 50; i++) held.push((await iter.next()).value!);
    expect(held).toHaveLength(50); // none acked yet
    held[0]!.nack();
    expect((await iter.next()).value!.uuid).toBe(held[0]!.uuid); // redelivered copy
    ac.abort();
    await ps.close();
  });

  it('blockPublishUntilSubscriberAck still waits for every ack', async () => {
    const ps = new MemoryPubSub({ concurrency: Infinity, blockPublishUntilSubscriberAck: true });
    const ac = new AbortController();
    const iter = ps.subscribe('t', ac.signal)[Symbol.asyncIterator]();
    let published = false;
    const pub = ps.publish('t', newMessage('a'), newMessage('b')).then(() => (published = true));
    const a = (await iter.next()).value!;
    const b = (await iter.next()).value!;
    b.ack();
    await delay(20);
    expect(published).toBe(false);
    a.ack();
    await pub;
    ac.abort();
    await ps.close();
  });

  it('aborting the subscription releases a blocked publisher', async () => {
    const ps = new MemoryPubSub({ concurrency: Infinity, blockPublishUntilSubscriberAck: true });
    const ac = new AbortController();
    const iter = ps.subscribe('t', ac.signal)[Symbol.asyncIterator]();
    const pub = ps.publish('t', newMessage('a'));
    await iter.next(); // held, never acked
    ac.abort();
    await pub;
    await ps.close();
  });

  it('hands out up to N unacked messages at once', async () => {
    const ps = new MemoryPubSub({ concurrency: 3 });
    const ac = new AbortController();
    const iter = ps.subscribe('t', ac.signal)[Symbol.asyncIterator]();
    await ps.publish('t', newMessage('a'), newMessage('b'), newMessage('c'), newMessage('d'));
    const held = [await iter.next(), await iter.next(), await iter.next()].map((r) => r.value!);
    expect(payloads(held)).toEqual(['a', 'b', 'c']);
    // The 4th waits for a free slot.
    const fourth = iter.next();
    const raced = await Promise.race([fourth, new Promise((r) => setTimeout(() => r('pending'), 50))]);
    expect(raced).toBe('pending');
    held[1]!.ack();
    expect((await fourth).value!.payload.toString()).toBe('d');
    ac.abort();
    await ps.close();
  });

  it('runs Router handlers in parallel; a nacked message does not block the others', async () => {
    const ps = new MemoryPubSub({ concurrency: Infinity }); // the Router's concurrency is the limit
    const router = new Router({ closeTimeoutMs: 2000 });
    const done: string[] = [];
    let active = 0;
    let maxActive = 0;
    let failedOnce = false;
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 't',
      subscriber: ps,
      concurrency: 4,
      handler: async (m) => {
        const p = m.payload.toString();
        maxActive = Math.max(maxActive, ++active);
        await delay(p === 'a' ? 60 : 20);
        active--;
        if (p === 'a' && !failedOnce) {
          failedOnce = true;
          throw new Error('boom'); // nack → redelivered by the same slot
        }
        done.push(p);
      },
    });
    void router.run();
    await router.running;
    await ps.publish('t', ...['a', 'b', 'c', 'd'].map((p) => newMessage(p)));
    await eventually(() => done.length === 4);
    expect(maxActive).toBe(4);
    expect(done.slice(0, 3).toSorted()).toEqual(['b', 'c', 'd']);
    expect(done[3]).toBe('a');
    await router.close();
    await ps.close();
  });
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
