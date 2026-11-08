import { MemoryPubSub, Router, newMessage } from '@millstream/core';
import { collect } from '@millstream/test-kit';
import { afterEach, describe, expect, it } from 'vitest';
import { ForwarderPublisher, addForwarder, unwrapEnvelope } from '../src/index.js';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).toReversed()) await c();
});

describe('Forwarder (in-memory)', () => {
  it('wraps into one outbox topic and unwraps to the destination', async () => {
    const outbox = new MemoryPubSub();
    const broker = new MemoryPubSub();
    const router = new Router();
    cleanups.push(async () => {
      await router.close();
      await outbox.close();
      await broker.close();
    });
    addForwarder(router, { subscriber: outbox, publisher: broker });
    const dest = broker.subscribe('orders.created');
    void router.run();
    await router.running;

    const original = newMessage('order-1', { k: 'v' });
    await new ForwarderPublisher(outbox).publish('orders.created', original);
    const [m] = await collect(dest, 1);
    expect(m!.uuid).toBe(original.uuid);
    expect(m!.equals(original)).toBe(true);
  });

  it('rejects malformed envelopes', () => {
    expect(() => unwrapEnvelope(newMessage('not json'))).toThrow(/envelope/);
  });
});
