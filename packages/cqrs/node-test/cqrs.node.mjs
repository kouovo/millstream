// The published dist on plain Node (node:test) — no TS, no vitest.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MemoryPubSub, Router, dropOnError } from '@millstream/core';
import { EventBus, EventProcessor, InvalidEventError, defineEvent } from '@millstream/cqrs';
import { z } from 'zod';

test('EventBus → EventProcessor round trip with Standard Schema validation (dist)', async () => {
  const TodoCompleted = defineEvent('todo.completed.v1', z.object({ todoId: z.string(), at: z.coerce.date() }));
  const transport = new MemoryPubSub({ blockPublishUntilSubscriberAck: true });
  const router = new Router();
  const bus = new EventBus({ publisher: transport });
  const processor = new EventProcessor({ router, subscriber: transport, middlewares: [dropOnError()] });
  const got = [];
  processor.on(TodoCompleted, 'h', (e) => void got.push(e));
  void router.run();
  await router.running;

  const at = new Date('2026-11-08T00:00:00Z');
  await bus.publish(TodoCompleted, { todoId: 't1', at });
  assert.deepEqual(got, [{ todoId: 't1', at }]);
  await assert.rejects(bus.publish(TodoCompleted, { todoId: 1 }), InvalidEventError);

  await router.close();
  await transport.close();
});

test('Mediator from the ./mediator entry (dist)', async () => {
  const { Mediator, InvalidRequestError, defineCommand, defineQuery } = await import('@millstream/cqrs/mediator');
  const Create = defineCommand('todo.create', z.object({ title: z.string().min(1) }));
  const Count = defineQuery('todo.count');
  const titles = [];
  const m = new Mediator()
    .handle(Create, ({ title }) => ({ id: String(titles.push(title)) }))
    .handle(Count, () => titles.length);
  assert.deepEqual(await m.execute(Create, { title: 'a' }), { id: '1' });
  assert.equal(await m.query(Count), 1);
  await assert.rejects(m.execute(Create, { title: '' }), InvalidRequestError);
});
