import {
  CORRELATION_ID,
  MemoryPubSub,
  type Message,
  type Publisher,
  Router,
  dropOnError,
  newMessage,
  poisonQueue,
  retry,
} from '@millstream/core';
import { collect, delay, eventually } from '@millstream/test-kit';
import { afterEach, describe, expect, expectTypeOf, it } from 'vitest';
import { z } from 'zod';
import {
  EVENT_NAME,
  EventBus,
  EventProcessor,
  type EventPayload,
  InvalidEventError,
  UnknownEventError,
  defineEvent,
  eventNameOf,
  on,
} from '../src/index.js';

const TodoCreated = defineEvent('todo.created.v1', z.object({ todoId: z.string(), title: z.string().min(1) }));
const TodoCompleted = defineEvent(
  'todo.completed.v1',
  z.object({ todoId: z.string(), completedAt: z.coerce.date() }), // JSON string → Date for handlers
);
const UserRenamed = defineEvent<{ userId: string; name: string }>('user.renamed.v1'); // types only

/** One topic per aggregate: todo.*.v1 → "todo". */
const byAggregate = (name: string) => name.split('.')[0]!;

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).toReversed()) await c();
});

function setup(opts: { block?: boolean; topic?: (n: string) => string } = {}) {
  const transport = new MemoryPubSub({ blockPublishUntilSubscriberAck: opts.block });
  const router = new Router({ closeTimeoutMs: 500 });
  const bus = new EventBus({ publisher: transport, topic: opts.topic });
  const processor = new EventProcessor({ router, subscriber: transport, topic: opts.topic });
  cleanups.push(async () => {
    await router.close();
    await transport.close();
  });
  const start = async () => {
    void router.run();
    await router.running;
  };
  return { transport, router, bus, processor, start };
}

describe('defineEvent', () => {
  it('infers payload types from the schema (output for handlers, input for publishers)', () => {
    expectTypeOf<EventPayload<typeof TodoCompleted>>().toEqualTypeOf<{ todoId: string; completedAt: Date }>();
    expectTypeOf<EventPayload<typeof UserRenamed>>().toEqualTypeOf<{ userId: string; name: string }>();
  });

  it('rejects empty names', () => {
    expect(() => defineEvent('  ')).toThrow(/empty/);
  });
});

describe('EventBus + EventProcessor', () => {
  it('delivers typed, validated payloads to every handler of the event', async () => {
    const { bus, processor, start } = setup();
    const got: Array<[string, unknown]> = [];
    processor.on(TodoCompleted, 'notify', (e) => void got.push(['notify', e]));
    processor.on(TodoCompleted, 'project', (e) => {
      expectTypeOf(e.completedAt).toEqualTypeOf<Date>();
      got.push(['project', e]);
    });
    await start();
    const at = new Date('2026-11-08T10:00:00Z');
    const msg = await bus.publish(TodoCompleted, { todoId: 't1', completedAt: at });
    await eventually(() => got.length === 2);
    expect(got.map(([h]) => h).toSorted()).toEqual(['notify', 'project']);
    for (const [, e] of got) expect(e).toEqual({ todoId: 't1', completedAt: at }); // a real Date again
    expect(eventNameOf(msg)).toBe('todo.completed.v1');
  });

  it('publish rejects invalid payloads before anything is published', async () => {
    const { bus, transport } = setup();
    const sub = transport.subscribe('todo.created.v1');
    await expect(bus.publish(TodoCreated, { todoId: 't1', title: '' })).rejects.toBeInstanceOf(InvalidEventError);
    await expect(bus.publish(TodoCreated, { todoId: 't1', title: '' })).rejects.toThrow(/title/);
    await bus.publish(TodoCreated, { todoId: 't2', title: 'ok' });
    const [m] = await collect(sub, 1);
    expect(JSON.parse(m!.payload.toString())).toEqual({ todoId: 't2', title: 'ok' });
  });

  it('an invalid event on the wire is a permanent error: not retried, handed to the poison queue', async () => {
    const { transport, router, start } = setup();
    let retries = 0;
    let calls = 0;
    const processor = new EventProcessor({
      router,
      subscriber: transport,
      middlewares: [
        poisonQueue(transport, 'poison'),
        retry({ maxRetries: 3, initialIntervalMs: 1, onRetry: () => retries++ }),
      ],
    });
    processor.on(TodoCreated, 'h', () => void calls++);
    const poison = transport.subscribe('poison');
    await start();
    // Produced by something that skipped validation:
    await transport.publish(
      'todo.created.v1',
      newMessage(JSON.stringify({ todoId: 1 }), { [EVENT_NAME]: 'todo.created.v1' }),
    );
    const [p] = await collect(poison, 1);
    expect(p!.metadata.reason_poisoned).toMatch(/invalid "todo.created.v1" event/);
    expect(retries).toBe(0);
    expect(calls).toBe(0);
  });

  it('events of other types on a shared topic are skipped (acked) by default', async () => {
    const { bus, processor, start } = setup({ topic: byAggregate });
    const completed: string[] = [];
    processor.on(TodoCompleted, 'only-completed', (e) => void completed.push(e.todoId));
    await start();
    await bus.publish(TodoCreated, { todoId: 'a', title: 'x' });
    await bus.publish(TodoCompleted, { todoId: 'a', completedAt: new Date() });
    await eventually(() => completed.length === 1);
    expect(completed).toEqual(['a']);
  });

  it("onUnknownEvent: 'nack' fails messages that aren't the handler's events", async () => {
    const { transport, router, start } = setup();
    const drops: unknown[] = [];
    const processor = new EventProcessor({
      router,
      subscriber: transport,
      onUnknownEvent: 'nack',
      middlewares: [dropOnError({ onDrop: (e) => drops.push(e.error) })],
    });
    processor.on(TodoCreated, 'strict', () => {});
    await start();
    await transport.publish('todo.created.v1', newMessage('{}', { [EVENT_NAME]: 'something.else' }));
    await eventually(() => drops.length === 1);
    expect(drops[0]).toBeInstanceOf(UnknownEventError);
  });

  it('group: several event types on one subscription, handled in publish order', async () => {
    const { bus, processor, start } = setup({ topic: byAggregate });
    const log: string[] = [];
    processor.group('todo-projection', [
      on(TodoCreated, async (e) => {
        await delay(20); // slower handler must still not be overtaken
        log.push(`created:${e.todoId}`);
      }),
      on(TodoCompleted, (e) => void log.push(`completed:${e.todoId}`)),
    ]);
    await start();
    await bus.publishMany([
      TodoCreated.of({ todoId: 'a', title: 'x' }),
      TodoCompleted.of({ todoId: 'a', completedAt: new Date() }),
      TodoCreated.of({ todoId: 'b', title: 'y' }),
    ]);
    await eventually(() => log.length === 3);
    expect(log).toEqual(['created:a', 'completed:a', 'created:b']);
  });

  it('group: rejects events that map to different topics, and duplicates', () => {
    const { processor } = setup();
    expect(() => processor.group('g', [on(TodoCreated, () => {}), on(TodoCompleted, () => {})])).toThrow(
      /different topics/,
    );
    const shared = setup({ topic: () => 'all' }).processor;
    expect(() => shared.group('g', [on(TodoCreated, () => {}), on(TodoCreated, () => {})])).toThrow(/registered twice/);
  });

  it('publishMany: one publish call per run of consecutive same-topic events, order kept', async () => {
    const calls: Array<[string, string[]]> = [];
    const spy: Publisher = {
      publish: async (topic, ...ms) => void calls.push([topic, ms.map((m) => JSON.parse(m.payload.toString()).todoId)]),
      close: async () => {},
    };
    const bus = new EventBus({ publisher: spy });
    const out = await bus.publishMany([
      TodoCreated.of({ todoId: '1', title: 'a' }),
      TodoCreated.of({ todoId: '2', title: 'b' }, { uuid: 'fixed-uuid', metadata: { k: 'v' } }),
      TodoCompleted.of({ todoId: '1', completedAt: new Date() }),
      TodoCreated.of({ todoId: '3', title: 'c' }),
    ]);
    expect(calls).toEqual([
      ['todo.created.v1', ['1', '2']],
      ['todo.completed.v1', ['1']],
      ['todo.created.v1', ['3']],
    ]);
    expect(out[1]!.uuid).toBe('fixed-uuid');
    expect(out[1]!.metadata).toMatchObject({ k: 'v', [EVENT_NAME]: 'todo.created.v1' });
  });

  it('propagates correlation_id from the message being handled; onPublish can add metadata', async () => {
    const transport = new MemoryPubSub();
    const router = new Router({ closeTimeoutMs: 500 });
    cleanups.push(async () => {
      await router.close();
      await transport.close();
    });
    const bus = new EventBus({
      publisher: transport,
      onPublish: ({ message }) => void (message.metadata.source = 'todo-service'),
    });
    const processor = new EventProcessor({ router, subscriber: transport });
    // A reaction that publishes a follow-up event from inside a handler:
    processor.on(TodoCreated, 'auto-complete', (e) =>
      bus.publish(TodoCompleted, { todoId: e.todoId, completedAt: new Date() }).then(() => {}),
    );
    const out = transport.subscribe('todo.completed.v1');
    void router.run();
    await router.running;
    await bus.publish(TodoCreated, { todoId: 'a', title: 'x' }, { metadata: { [CORRELATION_ID]: 'req-42' } });
    const [m] = await collect(out, 1);
    expect(m!.metadata).toMatchObject({ [CORRELATION_ID]: 'req-42', source: 'todo-service' });
  });

  it('as an in-process best-effort bus: publish waits for handlers and never fails because of them', async () => {
    const { bus, router, start, transport } = setup({ block: true });
    const processor = new EventProcessor({ router, subscriber: transport, middlewares: [dropOnError()] });
    const done: string[] = [];
    processor.on(TodoCompleted, 'cache', async () => {
      await delay(30);
      done.push('cache');
    });
    processor.on(TodoCompleted, 'broken', () => {
      done.push('broken');
      throw new Error('nope');
    });
    await start();
    await bus.publish(TodoCompleted, { todoId: 'a', completedAt: new Date() });
    expect(done.toSorted()).toEqual(['broken', 'cache']);
  });

  it('subscriber factory receives the handler name (consumer group) and topic', async () => {
    const transport = new MemoryPubSub();
    const router = new Router();
    cleanups.push(async () => {
      await router.close();
      await transport.close();
    });
    const seen: Array<{ handlerName: string; topic: string }> = [];
    const processor = new EventProcessor({
      router,
      subscriber: (p) => {
        seen.push(p);
        return transport;
      },
    });
    processor.on(UserRenamed, 'search-index', (_e: { userId: string; name: string }, _m: Message) => {});
    expect(seen).toEqual([{ handlerName: 'search-index', topic: 'user.renamed.v1' }]);
  });
});
