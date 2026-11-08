import { collect, delay, eventually } from '@millstream/test-kit';
import { afterEach, describe, expect, it } from 'vitest';
import { MemoryPubSub, type MemoryPubSubOptions } from '../src/memory.js';
import { newMessage } from '../src/message.js';
import { type DropEvent, dropOnError, instantAck, retry, timeout } from '../src/middleware/index.js';
import { Router } from '../src/router.js';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).toReversed()) await c();
});

function setup(opts: MemoryPubSubOptions = {}) {
  const bus = new MemoryPubSub(opts);
  const router = new Router({ closeTimeoutMs: 500 });
  cleanups.push(async () => {
    await router.close();
    await bus.close();
  });
  const start = async () => {
    void router.run();
    await router.running;
  };
  return { bus, router, start };
}

describe('default (no middleware): nack → the transport redelivers', () => {
  it('a failing message comes back until it succeeds', async () => {
    const { bus, router, start } = setup();
    let calls = 0;
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 't',
      subscriber: bus,
      handler: () => {
        if (++calls < 3) throw new Error('transient');
      },
    });
    await start();
    await bus.publish('t', newMessage('x'));
    await eventually(() => calls === 3);
  });
});

describe('instantAck: "lost is lost, don\'t care"', () => {
  it('acks before the handler runs; a failure is not redelivered', async () => {
    const { bus, router, start } = setup();
    let calls = 0;
    let stateInHandler = '';
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 't',
      subscriber: bus,
      middlewares: [instantAck],
      handler: (m) => {
        calls++;
        stateInHandler = m.state;
        throw new Error('ignored');
      },
    });
    await start();
    await bus.publish('t', newMessage('x'));
    await eventually(() => calls === 1);
    await delay(100);
    expect(calls).toBe(1);
    expect(stateInHandler).toBe('ack');
  });
});

describe('dropOnError: "lost is fine, just log it"', () => {
  it('reports and acks: no redelivery, later messages and other handlers unaffected', async () => {
    const { bus, router, start } = setup();
    const drops: DropEvent[] = [];
    const seen: string[] = [];
    let failing = 0;
    router.addConsumerHandler({
      name: 'failing',
      subscribeTopic: 'order.placed',
      subscriber: bus,
      middlewares: [dropOnError({ onDrop: (e) => drops.push(e) })],
      handler: (m) => {
        failing++;
        if (m.payload.toString() === 'o1') throw new Error('boom');
      },
    });
    router.addConsumerHandler({
      name: 'ok',
      subscribeTopic: 'order.placed',
      subscriber: bus,
      handler: (m) => void seen.push(m.payload.toString()),
    });
    await start();
    await bus.publish('order.placed', newMessage('o1'), newMessage('o2'));
    await eventually(() => failing === 2 && seen.length === 2);
    await delay(50);
    expect(failing).toBe(2); // o1 once (dropped), o2 once
    expect(seen).toEqual(['o1', 'o2']);
    expect(drops).toHaveLength(1);
    expect(drops[0]).toMatchObject({ handlerName: 'failing', topic: 'order.placed' });
    expect((drops[0]!.error as Error).message).toBe('boom');
    expect(drops[0]!.message.payload.toString()).toBe('o1');
  });

  it('with retry inside: a few attempts first, then drop', async () => {
    const { bus, router, start } = setup();
    const drops: DropEvent[] = [];
    let calls = 0;
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 't',
      subscriber: bus,
      middlewares: [dropOnError({ onDrop: (e) => drops.push(e) }), retry({ maxRetries: 2, initialIntervalMs: 1 })],
      handler: () => {
        calls++;
        throw new Error('always');
      },
    });
    await start();
    await bus.publish('t', newMessage('x'));
    await eventually(() => drops.length === 1);
    expect(calls).toBe(3);
  });

  it('with timeout inside: a stuck handler is skipped and the next message flows', async () => {
    const { bus, router, start } = setup();
    const drops: DropEvent[] = [];
    const seen: string[] = [];
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 't',
      subscriber: bus,
      middlewares: [dropOnError({ onDrop: (e) => drops.push(e) }), timeout(100)],
      handler: async (m) => {
        if (m.payload.toString() === 'stuck') await new Promise(() => {}); // never settles
        seen.push(m.payload.toString());
      },
    });
    await start();
    await bus.publish('t', newMessage('stuck'), newMessage('next'));
    await eventually(() => seen.includes('next'), 2000);
    expect(drops.map((d) => (d.error as Error).name)).toEqual(['HandlerTimeoutError']);
  });

  it('shouldDrop: unlisted errors still nack (and get redelivered)', async () => {
    class Ignorable extends Error {}
    const { bus, router, start } = setup();
    let calls = 0;
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 't',
      subscriber: bus,
      middlewares: [dropOnError({ shouldDrop: (e) => e instanceof Ignorable })],
      handler: () => {
        calls++;
        if (calls === 1) throw new Error('important'); // → nack → redelivered
        throw new Ignorable('fine'); // → dropped
      },
    });
    await start();
    await bus.publish('t', newMessage('x'));
    await eventually(() => calls === 2);
    await delay(100);
    expect(calls).toBe(2);
  });

  it('a throwing onDrop hook does not turn the drop into a nack', async () => {
    const { bus, router, start } = setup();
    let calls = 0;
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 't',
      subscriber: bus,
      middlewares: [
        dropOnError({
          onDrop: () => {
            throw new Error('logger down');
          },
        }),
      ],
      handler: () => {
        calls++;
        throw new Error('boom');
      },
    });
    await start();
    await bus.publish('t', newMessage('x'));
    await eventually(() => calls === 1);
    await delay(100);
    expect(calls).toBe(1);
  });
});

describe('in-process event bus: MemoryPubSub({ blockPublishUntilSubscriberAck }) + dropOnError', () => {
  it('publish resolves after every handler ran, and never fails because of one', async () => {
    const { bus, router, start } = setup({ blockPublishUntilSubscriberAck: true });
    const done: string[] = [];
    const best = [dropOnError()];
    router.addConsumerHandler({
      name: 'a',
      subscribeTopic: 't',
      subscriber: bus,
      middlewares: best,
      handler: async () => {
        await delay(50);
        done.push('a');
      },
    });
    router.addConsumerHandler({
      name: 'b',
      subscribeTopic: 't',
      subscriber: bus,
      middlewares: best,
      handler: () => {
        done.push('b');
        throw new Error('b fails');
      },
    });
    await start();
    await bus.publish('t', newMessage('x'));
    expect(done.toSorted()).toEqual(['a', 'b']);
  });

  it('publishing from inside a handler does not deadlock', async () => {
    const { bus, router, start } = setup({ blockPublishUntilSubscriberAck: true });
    const seen: string[] = [];
    router.addConsumerHandler({
      name: 'chain',
      subscribeTopic: 't',
      subscriber: bus,
      handler: async (m) => {
        seen.push(m.payload.toString());
        // Waiting here would wait for this very subscription → must only enqueue.
        if (m.payload.toString() === 'first') await bus.publish('t', newMessage('second'));
      },
    });
    await start();
    await bus.publish('t', newMessage('first'));
    await eventually(() => seen.length === 2, 2000);
    expect(seen).toEqual(['first', 'second']);
  });

  it('a stopped subscription releases blocked publishers', async () => {
    const { bus } = setup({ blockPublishUntilSubscriberAck: true });
    const c = new AbortController();
    const sub = bus.subscribe('t', c.signal);
    const published = bus.publish('t', newMessage('a'), newMessage('b'));
    await collect(sub, 1, { onMessage: () => {} }); // take 'a', never answer
    c.abort();
    await expect(Promise.race([published.then(() => 'released'), delay(1000).then(() => 'stuck')])).resolves.toBe(
      'released',
    );
  });
});
