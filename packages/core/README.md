# @millstream/core

Core of [millstream](../../README.md), a Watermill-inspired messaging toolkit for Node.js.

- `Message`: `ack()` / `nack()` (idempotent, mutually exclusive), `settled` promise, `copy()`.
- `Publisher` / `Subscriber`: `subscribe(topic, signal)` returns an `AsyncIterable<Message>`. Ordered implementations hold
  the next message until the current one is acked.
- `Router`: per-handler subscription loop, concurrency limit, middleware chain, publish-then-ack, graceful
  `close()` (stop pulling → drain in-flight → abort).
- Middlewares: `recoverer`, `retry`, `timeout`, `poisonQueue`, `dropOnError`, `correlationId`, `instantAck`, `throttle`,
  `delayOnError`.
- `MemoryPubSub` (GoChannel counterpart): ordered, Nack redelivers; `blockPublishUntilSubscriberAck` for a
  synchronous in-process event bus.
- `fromPush` (wrap push-style SDKs), `producerSubscription` (for writing
  subscribers), `addRequeuer`.
- Plugins (`addPlugin`, `signalsHandler`) and decorators (`addPublisherDecorators`, `addSubscriberDecorators`,
  `messageTransform*Decorator`).
- Context: `handlerContext()`, `txFromContext()`, `signalFromContext()`, `contextExecutor(pool)`.

```ts
import { MemoryPubSub, Router, newMessage, retry } from '@millstream/core';

const bus = new MemoryPubSub();
const router = new Router().use(retry({ maxRetries: 3 }));
router.addHandler({
  name: 'upper',
  subscribeTopic: 'in',
  subscriber: bus,
  publishTopic: 'out',
  publisher: bus,
  handler: (msg) => [newMessage(msg.payload.toString().toUpperCase())],
});
void router.run();
await router.running;
await bus.publish('in', newMessage('hello'));
```

## Plugins and decorators

**Plugins** run when `router.run()` starts, before any handler subscribes. A throwing plugin makes `run()` reject.
Clean up after `router.closed`.

```ts
import { Router, signalsHandler } from '@millstream/core';

const router = new Router().addPlugin(signalsHandler()); // SIGINT/SIGTERM → graceful close()
await router.run(); // resolves once the router has drained
```

**Decorators** wrap the publisher and subscriber of every handler: those already registered and those added later. The
first decorator added is the innermost, closest to the real transport. Unlike Watermill, each decorator also receives
`{ handlerName, subscribeTopic, publishTopic }`, so per-handler metrics and spans don't need to dig into message
context.

```ts
import { messageTransformPublisherDecorator, messageTransformSubscriberDecorator } from '@millstream/core';

router.addPublisherDecorators(
  // e.g. tracing: inject the current trace context into produced messages
  messageTransformPublisherDecorator((msg) => propagation.inject(context.active(), msg.metadata)),
);
router.addSubscriberDecorators((subscriber, info) =>
  // e.g. metrics: count received messages per handler
  messageTransformSubscriberDecorator(() => receivedCounter.add(1, { handler: info.handlerName }))(subscriber, info),
);
```

For complete tracing and metrics, use [`@millstream/otel`](../otel), which is built on these hooks.

Decorators apply to the Router's handlers only. Publishers you use directly, such as an `EventBus`'s or a
`poisonQueue`'s, can be wrapped with `transformPublisher(publisher, fn)`.

## Failure policies

Transports keep one rule: **a nacked message is delivered again**. Without it, Nack would mean "lose it". _What to do
about failures_ is a Router middleware decision, per handler:

| Scenario                               | Use                                                                                | Notes                                                                                                                                                    |
| -------------------------------------- | ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Must succeed                           | nothing (Nack → redelivery), plus `retry` + `poisonQueue` as a safety net          | Without a limit, a poison message is redelivered forever and blocks the ones behind it                                                                   |
| Retry later, without blocking the rest | `delayOnError` + `poisonQueue` to a delayed queue + requeuer (`pgDelayedRequeuer`) | Breaks per-key ordering                                                                                                                                  |
| Lost, but keep it for manual replay    | `poisonQueue` (+ `recoverer`)                                                      | Replay by temporarily running `addRequeuer` on the poison topic                                                                                          |
| Lost is fine, just log it              | `dropOnError({ onDrop })`                                                          | Acks on failure. `shouldDrop` limits it to specific errors (Watermill's `IgnoreErrors`)                                                                  |
| Lost is lost, don't care               | `instantAck`                                                                       | Acks _before_ the handler runs: crashes lose messages too, ordering may break, and a SQL subscriber commits before the handler can use `txFromContext()` |

Order matters: the first middleware is the outermost. In `[poisonQueue(...), retry(...)]`, retries happen first and
the poison queue gets what's left. In the reverse order, the poison queue swallows the first failure.

## In-process domain events (CQRS `EventBus`)

For events that should never affect the code that publishes them (post-commit notifications, cache invalidation,
logging), combine `MemoryPubSub` with `dropOnError`. No special transport is needed:

```ts
import { MemoryPubSub, Router, dropOnError, newMessage, timeout } from '@millstream/core';

const events = new MemoryPubSub({ blockPublishUntilSubscriberAck: true }); // publish waits for the handlers
const bestEffort = [
  dropOnError({ onDrop: ({ handlerName, error }) => logger.warn({ handlerName, err: error }, 'event handler failed') }),
  timeout(5_000), // a stuck handler is skipped
];

const router = new Router();
router.addConsumerHandler({
  name: 'invalidate-todo-cache',
  subscribeTopic: 'todo.completed',
  subscriber: events,
  middlewares: bestEffort,
  handler: async (msg) => cache.invalidate(JSON.parse(msg.payload.toString()).todoId),
});
router.addConsumerHandler({
  name: 'log-todo-completed',
  subscribeTopic: 'todo.completed',
  subscriber: events,
  middlewares: bestEffort,
  handler: (msg) => logger.info(JSON.parse(msg.payload.toString()), 'todo completed'),
});
void router.run();

// After the business transaction committed:
await events.publish('todo.completed', newMessage(JSON.stringify({ todoId })));
```

- Every handler is its own subscription with its own copy, in order. One failing or slow handler doesn't affect the
  others.
- With `blockPublishUntilSubscriberAck`, `publish` resolves once all handlers are done. Because of `dropOnError`, it
  never fails because of them. Publishing from inside a handler doesn't wait, so it can't deadlock. Leave the option
  off for fire-and-forget.
- Anything that **must** happen doesn't belong here. Write it to an outbox in the business transaction
  (`@millstream/pg` + `@millstream/forwarder`).
