# @millstream/cqrs

Typed events on top of [millstream](../../README.md): an `EventBus` to publish and an `EventProcessor` to handle,
the counterparts of Watermill's `cqrs.EventBus` / `EventProcessor` / `EventGroupProcessor`. There are no runtime
dependencies. Validation uses any [Standard Schema](https://standardschema.dev) library (zod, valibot, arktype, …).

```ts
import { EventBus, EventProcessor, defineEvent, on } from '@millstream/cqrs';
import { z } from 'zod';

// The name is the wire contract: explicit, versioned, never renamed.
export const TodoCompleted = defineEvent(
  'todo.completed.v1',
  z.object({
    todoId: z.string(),
    completedAt: z.coerce.date(), // JSON → Date again on the consumer side
  }),
);
// Types only, no runtime validation:
export const UserRenamed = defineEvent<{ userId: string; name: string }>('user.renamed.v1');

const bus = new EventBus({ publisher });
await bus.publish(TodoCompleted, { todoId, completedAt: new Date() }); // payload type-checked and validated

const processor = new EventProcessor({ router, subscriber });
processor.on(TodoCompleted, 'notify-owner', async (event, msg) => {
  event.completedAt; // Date
});
```

## The bus is only as reliable as its Publisher

`EventBus` keeps no handler registry. It marshals the event, picks the topic, and calls `publisher.publish`. Pick the
semantics by choosing the transport:

**In-process, best effort** (post-commit notifications, cache invalidation, logging):

```ts
const transport = new MemoryPubSub({ blockPublishUntilSubscriberAck: true }); // publish waits for the handlers
const bus = new EventBus({ publisher: transport });
const processor = new EventProcessor({
  router,
  subscriber: transport,
  middlewares: [dropOnError({ onDrop: ({ handlerName, error }) => log.warn({ handlerName, error }) }), timeout(5_000)],
});
```

**Reliable** (the event is written in the business transaction, then delivered at least once):

```ts
const bus = new EventBus({ publisher: new PgPublisher(contextExecutor(db)) }); // or ForwarderPublisher(...) → Kafka/BullMQ
const processor = new EventProcessor({
  router,
  // One consumer group per handler: each one gets every event and progresses / fails on its own.
  subscriber: ({ handlerName }) => new PgSubscriber(db, { consumerGroup: handlerName }),
  middlewares: [poisonQueue(new PgPublisher(contextExecutor(db)), 'poison'), retry({ maxRetries: 3 })],
});

await withTransaction(db, async (tx) => {
  await tx.query('UPDATE todos SET completed_at = now() WHERE id = $1', [todoId]);
  await bus.publish(TodoCompleted, { todoId, completedAt: new Date() }); // same transaction; rolled back with it
});
```

## Details

- **Topics:** by default the topic is the event name. Use `topic: (name) => …` on both the bus and the processor, for
  example one topic per aggregate. A handler skips (acks) messages on its topic that aren't its events, unless you set
  `onUnknownEvent: 'nack'`.
- **`processor.group(name, [on(A, …), on(B, …)])`** handles several event types on one subscription, in publish
  order: for example Created → Completed of the same aggregate. All of them must map to the same topic. A failing event
  blocks the group.
- **`processor.on(E, name, [stepA, stepB])`** runs several handlers for one event in series, on one subscription:
  each step waits for the previous one, for example update a read model and then notify from it. The first failure
  fails the message, and a redelivery runs every step again, so keep the steps idempotent. Handlers registered
  with separate `on` calls run independently instead. `on(E, [...])` works inside a `group` too.
- **Validation:** payloads are validated on publish (`validate: false` to skip) and on consume. An invalid event on
  the wire becomes a `PermanentError`, so `retry` doesn't retry it and `poisonQueue` / `dropOnError` receive an
  `InvalidEventError`.
- **`bus.publishMany([A.of(...), B.of(...)])`** publishes in order. Consecutive events for the same topic go out in one
  `publish` call, which means one INSERT, one Kafka batch, or one `addBulk`.
- **Metadata:** `name` holds the event name, the same key Watermill uses. A `correlation_id` is copied from the message
  being handled when you publish from inside a handler (`propagateCorrelationId: false` to turn it off).
  `onPublish({ event, payload, message, topic })` lets you add your own metadata, such as tracing headers.
- **Handler names** are consumer-group identities for durable transports: keep them stable.
- **Custom wire format** (protobuf, avro, a schema registry): pass a `marshaler` (`marshal` / `unmarshal` / `nameOf`)
  to both the bus and the processor.
