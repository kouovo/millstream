import { afterEach, describe, expect, it } from 'vitest';
import { fromPush } from '../src/bridge.js';
import { handlerContext, signalFromContext } from '../src/context.js';
import { MemoryPubSub } from '../src/memory.js';
import { newMessage, type Message } from '../src/message.js';
import {
  CORRELATION_ID,
  POISON_HANDLER,
  POISON_REASON,
  POISON_TOPIC,
  correlationId,
  poisonQueue,
  recoverer,
  retry,
  timeout,
} from '../src/middleware/index.js';
import type { Subscriber } from '../src/pubsub.js';
import { Router } from '../src/router.js';
import { collect, eventually, payloads } from '@millstream/test-kit';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).toReversed()) await c();
});

function setup(opts: { closeTimeoutMs?: number } = {}) {
  const ps = new MemoryPubSub();
  const router = new Router({ closeTimeoutMs: opts.closeTimeoutMs ?? 2000 });
  cleanups.push(async () => {
    await router.close();
    await ps.close();
  });
  return { ps, router };
}

/** Unordered subscriber: yields everything immediately, records acks. */
function unorderedSubscriber(messages: Message[]): Subscriber {
  return {
    subscribe: (_t, signal) =>
      fromPush(async (onMessage) => {
        for (const m of messages) void onMessage(m);
      }, signal),
    close: async () => {},
  };
}

describe('Router', () => {
  it('publishes produced messages, then acks the input', async () => {
    const { ps, router } = setup();
    router.addHandler({
      name: 'upper',
      subscribeTopic: 'in',
      subscriber: ps,
      publishTopic: 'out',
      publisher: ps,
      handler: (m) => [newMessage(m.payload.toString().toUpperCase())],
    });
    const out = ps.subscribe('out');
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('a'), newMessage('b'));
    expect(payloads(await collect(out, 2))).toEqual(['A', 'B']);
  });

  it('nacks on error and the message is redelivered', async () => {
    const { ps, router } = setup();
    let calls = 0;
    router.addConsumerHandler({
      name: 'flaky',
      subscribeTopic: 'in',
      subscriber: ps,
      handler: () => {
        if (++calls < 3) throw new Error('boom');
      },
    });
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => calls === 3);
  });

  it('nacks when a handler returns output but has no publisher', async () => {
    const { ps, router } = setup();
    const seen: string[] = [];
    router.addHandler({
      name: 'h',
      subscribeTopic: 'in',
      subscriber: ps,
      handler: (m) => {
        seen.push(m.uuid);
        return seen.length === 1 ? [newMessage('oops')] : undefined;
      },
    });
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => seen.length === 2);
    expect(seen[0]).toBe(seen[1]);
  });

  it('runs middlewares outermost-first and exposes handler context', async () => {
    const { ps, router } = setup();
    const order: string[] = [];
    const mw = (name: string) => (next: any) => async (m: Message) => {
      order.push(`${name}>`);
      const r = await next(m);
      order.push(`<${name}`);
      return r;
    };
    let ctx: ReturnType<typeof handlerContext>;
    router.use(mw('router'));
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 'in',
      subscriber: ps,
      middlewares: [mw('handler')],
      handler: () => {
        ctx = handlerContext();
        order.push('h');
      },
    });
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => order.length === 5);
    expect(order).toEqual(['router>', 'handler>', 'h', '<handler', '<router']);
    expect(ctx!.handlerName).toBe('h');
    expect(ctx!.subscribeTopic).toBe('in');
  });

  it('honours the concurrency limit with an unordered subscriber', async () => {
    const router = new Router();
    cleanups.push(() => router.close());
    const input = Array.from({ length: 10 }, (_, i) => newMessage(String(i)));
    let active = 0;
    let peak = 0;
    router.addConsumerHandler({
      name: 'par',
      subscribeTopic: 't',
      subscriber: unorderedSubscriber(input),
      concurrency: 3,
      handler: async () => {
        peak = Math.max(peak, ++active);
        await new Promise((r) => setTimeout(r, 30));
        active--;
      },
    });
    void router.run();
    await eventually(() => input.every((m) => m.state === 'ack'));
    expect(peak).toBe(3);
  });

  it('close() waits for in-flight messages', async () => {
    const { ps, router } = setup();
    let finished = false;
    let started = false;
    router.addConsumerHandler({
      name: 'slow',
      subscribeTopic: 'in',
      subscriber: ps,
      handler: async () => {
        started = true;
        await new Promise((r) => setTimeout(r, 200));
        finished = true;
      },
    });
    const run = router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => started);
    await router.close();
    expect(finished).toBe(true);
    await run;
  });

  it('close() aborts handlers that exceed closeTimeout', async () => {
    const { ps, router } = setup({ closeTimeoutMs: 100 });
    let aborted = false;
    let started = false;
    router.addConsumerHandler({
      name: 'stuck',
      subscribeTopic: 'in',
      subscriber: ps,
      handler: async () => {
        started = true;
        await new Promise<void>((r) =>
          signalFromContext()!.addEventListener('abort', () => {
            aborted = true;
            r();
          }),
        );
      },
    });
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => started);
    await router.close();
    expect(aborted).toBe(true);
  });

  it('close() returns after closeTimeout even if a handler ignores its signal', async () => {
    const { ps, router } = setup({ closeTimeoutMs: 100 });
    let started = false;
    router.addConsumerHandler({
      name: 'deaf',
      subscribeTopic: 'in',
      subscriber: ps,
      handler: async () => {
        started = true;
        await new Promise(() => {}); // never settles, never looks at the signal
      },
    });
    const run = router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => started);
    const t0 = Date.now();
    await router.close();
    await run;
    expect(Date.now() - t0).toBeLessThan(1000);
  });

  it('run() resolves when an external signal aborts', async () => {
    const { ps, router } = setup();
    router.addConsumerHandler({ name: 'h', subscribeTopic: 'in', subscriber: ps, handler: () => {} });
    const c = new AbortController();
    const run = router.run(c.signal);
    await router.running;
    c.abort();
    await run;
  });

  it('handlers can be added while running', async () => {
    const { ps, router } = setup();
    router.addConsumerHandler({ name: 'a', subscribeTopic: 'a', subscriber: ps, handler: () => {} });
    void router.run();
    await router.running;
    let got = '';
    const h = router.addConsumerHandler({
      name: 'b',
      subscribeTopic: 'b',
      subscriber: ps,
      handler: (m) => void (got = m.payload.toString()),
    });
    await ps.publish('b', newMessage('late'));
    await eventually(() => got === 'late');
    await h.stop();
    expect(router.handlers().map((x) => x.name)).toEqual(['a']);
  });
});

describe('middlewares', () => {
  it('Retry retries in-process before failing', async () => {
    const { ps, router } = setup();
    let calls = 0;
    let delivered = 0;
    router.use(retry({ maxRetries: 2, initialIntervalMs: 5 }));
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 'in',
      subscriber: ps,
      handler: (m) => {
        if (m.state === 'pending') delivered++;
        if (++calls < 3) throw new Error('boom');
      },
    });
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => calls === 3);
    expect(delivered).toBe(3); // same delivery, no nack in between
  });

  it('PoisonQueue (outside Retry) publishes the failure and acks the original', async () => {
    const { ps, router } = setup();
    const poison = ps.subscribe('poison');
    let calls = 0;
    router.use(recoverer, poisonQueue(ps, 'poison'), retry({ maxRetries: 2, initialIntervalMs: 1 }));
    router.addConsumerHandler({
      name: 'bad',
      subscribeTopic: 'in',
      subscriber: ps,
      handler: (m) => {
        if (m.payload.toString() === 'bad') {
          calls++;
          throw new Error('cannot process');
        }
      },
    });
    const processed = ps.subscribe('in');
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('bad'), newMessage('good'));
    const [p] = await collect(poison, 1);
    expect(calls).toBe(3);
    expect(p!.payload.toString()).toBe('bad');
    expect(p!.metadata[POISON_REASON]).toBe('cannot process');
    expect(p!.metadata[POISON_TOPIC]).toBe('in');
    expect(p!.metadata[POISON_HANDLER]).toBe('bad');
    await collect(processed, 2); // head of line not blocked
  });

  it('CorrelationID is copied to produced messages', async () => {
    const { ps, router } = setup();
    router.use(correlationId);
    router.addHandler({
      name: 'h',
      subscribeTopic: 'in',
      subscriber: ps,
      publishTopic: 'out',
      publisher: ps,
      handler: () => [newMessage('child')],
    });
    const out = ps.subscribe('out');
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('parent', { [CORRELATION_ID]: 'abc' }));
    const [m] = await collect(out, 1);
    expect(m!.metadata[CORRELATION_ID]).toBe('abc');
  });

  it('Timeout fails the message and aborts the context signal', async () => {
    const { ps, router } = setup();
    let attempts = 0;
    let sawAbort = false;
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 'in',
      subscriber: ps,
      middlewares: [timeout(50)],
      handler: async () => {
        if (++attempts > 1) return;
        const signal = signalFromContext()!;
        await new Promise<void>((r) => signal.addEventListener('abort', () => r()));
        sawAbort = true;
      },
    });
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => attempts === 2); // timed out → nack → redelivered
    expect(sawAbort).toBe(true);
  });
});
