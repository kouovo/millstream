import { collect, eventually } from '@millstream/test-kit';
import { afterEach, describe, expect, it } from 'vitest';
import {
  type DecoratorInfo,
  messageTransformPublisherDecorator,
  messageTransformSubscriberDecorator,
  transformPublisher,
} from '../src/decorators/index.js';
import { MemoryPubSub } from '../src/memory.js';
import { type Message, newMessage } from '../src/message.js';
import { signalsHandler } from '../src/plugins/index.js';
import type { Publisher, Subscriber } from '../src/pubsub.js';
import { Router } from '../src/router.js';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).toReversed()) await c();
});

function setup() {
  const ps = new MemoryPubSub();
  const router = new Router({ closeTimeoutMs: 500 });
  cleanups.push(async () => {
    await router.close();
    await ps.close();
  });
  return { ps, router };
}

describe('decorators', () => {
  it('subscriber decorators transform every message a handler receives', async () => {
    const { ps, router } = setup();
    router.addSubscriberDecorators(
      messageTransformSubscriberDecorator((m, topic) => void (m.metadata.received_from = topic)),
    );
    const seen: Message[] = [];
    router.addConsumerHandler({ name: 'h', subscribeTopic: 'in', subscriber: ps, handler: (m) => void seen.push(m) });
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await eventually(() => seen.length === 1);
    expect(seen[0]!.metadata.received_from).toBe('in');
  });

  it("publisher decorators transform a handler's produced messages", async () => {
    const { ps, router } = setup();
    router.addPublisherDecorators(messageTransformPublisherDecorator((m) => void (m.metadata.produced_by = 'svc')));
    router.addHandler({
      name: 'h',
      subscribeTopic: 'in',
      subscriber: ps,
      publishTopic: 'out',
      publisher: ps,
      handler: (m) => [newMessage(m.payload.toString().toUpperCase())],
    });
    const out = ps.subscribe('out');
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    const [m] = await collect(out, 1);
    expect(m!.payload.toString()).toBe('X');
    expect(m!.metadata.produced_by).toBe('svc');
  });

  it('decorators get the handler info (e.g. per-handler metrics) and apply to handlers added at runtime', async () => {
    const { ps, router } = setup();
    const received = new Map<string, number>();
    const published: DecoratorInfo[] = [];
    const counting = (sub: Subscriber, info: DecoratorInfo): Subscriber =>
      messageTransformSubscriberDecorator(() =>
        received.set(info.handlerName, (received.get(info.handlerName) ?? 0) + 1),
      )(sub, info);
    router.addSubscriberDecorators(counting);
    router.addPublisherDecorators((pub: Publisher, info) => {
      published.push(info);
      return pub;
    });
    router.addConsumerHandler({ name: 'a', subscribeTopic: 'a', subscriber: ps, handler: () => {} });
    void router.run();
    await router.running;
    router.addHandler({
      name: 'late',
      subscribeTopic: 'b',
      subscriber: ps,
      publishTopic: 'c',
      publisher: ps,
      handler: () => {},
    });
    await ps.publish('a', newMessage('1'), newMessage('2'));
    await ps.publish('b', newMessage('3'));
    await eventually(() => received.get('a') === 2 && received.get('late') === 1);
    expect(published).toEqual([{ handlerName: 'late', subscribeTopic: 'b', publishTopic: 'c' }]); // only handlers with a publisher
  });

  it('the first decorator added is the innermost (closest to the real transport)', async () => {
    const { ps, router } = setup();
    const order: string[] = [];
    router.addSubscriberDecorators(
      messageTransformSubscriberDecorator(() => void order.push('sub:first')),
      messageTransformSubscriberDecorator(() => void order.push('sub:second')),
    );
    router.addPublisherDecorators(
      messageTransformPublisherDecorator(() => void order.push('pub:first')),
      messageTransformPublisherDecorator(() => void order.push('pub:second')),
    );
    router.addHandler({
      name: 'h',
      subscribeTopic: 'in',
      subscriber: ps,
      publishTopic: 'out',
      publisher: ps,
      handler: () => [newMessage('y')],
    });
    const out = ps.subscribe('out');
    void router.run();
    await router.running;
    await ps.publish('in', newMessage('x'));
    await collect(out, 1);
    // Received: inner (first) sees it first. Published: outer (second) sees it first.
    expect(order).toEqual(['sub:first', 'sub:second', 'pub:second', 'pub:first']);
  });

  it('a transformed subscription keeps ack identity and synchronous registration', async () => {
    const ps = new MemoryPubSub({ concurrency: 1 }); // ordered: the nacked 'a' comes back before 'b'
    cleanups.push(() => ps.close());
    const decorated = messageTransformSubscriberDecorator(() => {})(ps, { handlerName: 'x', subscribeTopic: 't' });
    const iter = decorated.subscribe('t')[Symbol.asyncIterator]();
    await ps.publish('t', newMessage('a'), newMessage('b')); // published right after subscribe()
    const first = await iter.next();
    if (first.done) throw new Error('no message');
    first.value.nack(); // nacking the decorated message must redeliver: same object as the transport's
    const again = await iter.next();
    expect(again.done ? null : again.value.payload.toString()).toBe('a');
  });

  it('transformPublisher can be used outside the router', async () => {
    const ps = new MemoryPubSub();
    cleanups.push(() => ps.close());
    const sub = ps.subscribe('t');
    await transformPublisher(ps, (m) => void (m.metadata.v = '1')).publish('t', newMessage('x'));
    const [m] = await collect(sub, 1);
    expect(m!.metadata.v).toBe('1');
  });
});

describe('plugins', () => {
  it('run in order before any handler subscribes; async plugins are awaited', async () => {
    const { ps, router } = setup();
    const log: string[] = [];
    router.addPlugin(
      async () => {
        await new Promise((r) => setTimeout(r, 20));
        log.push('plugin 1');
      },
      (r) => {
        log.push('plugin 2');
        // A plugin may add handlers itself.
        r.addConsumerHandler({
          name: 'from-plugin',
          subscribeTopic: 't',
          subscriber: ps,
          handler: () => void log.push('handled'),
        });
      },
    );
    void router.run();
    await router.running;
    await ps.publish('t', newMessage('x'));
    await eventually(() => log.includes('handled'));
    expect(log).toEqual(['plugin 1', 'plugin 2', 'handled']);
  });

  it('a failing plugin makes run() reject', async () => {
    const { router } = setup();
    router.addPlugin(() => {
      throw new Error('cannot start');
    });
    await expect(router.run()).rejects.toThrow('cannot start');
  });

  it('cannot be added after run()', async () => {
    const { router } = setup();
    void router.run();
    expect(() => router.addPlugin(() => {})).toThrow(/before run/);
  });

  it('signalsHandler closes the router on a signal and removes its listeners afterwards', async () => {
    const { ps, router } = setup();
    const before = process.listenerCount('SIGUSR2');
    const got: string[] = [];
    router.addPlugin(signalsHandler({ signals: ['SIGUSR2'], onSignal: (s) => void got.push(s) }));
    router.addConsumerHandler({ name: 'h', subscribeTopic: 't', subscriber: ps, handler: () => {} });
    const run = router.run();
    await router.running;
    expect(process.listenerCount('SIGUSR2')).toBe(before + 1);
    process.emit('SIGUSR2', 'SIGUSR2');
    await run; // closed by the plugin
    await router.closed;
    await Promise.resolve();
    expect(got).toEqual(['SIGUSR2']);
    expect(process.listenerCount('SIGUSR2')).toBe(before);
  });
});
