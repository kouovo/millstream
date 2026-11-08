# @millstream/otel

[OpenTelemetry](https://opentelemetry.io) for [millstream](../../README.md): producer and consumer spans, trace
context propagation through message metadata, and messaging metrics, following the OpenTelemetry messaging semantic
conventions. Peer dependency: `@opentelemetry/api@^1.9`. Works with any transport.

```ts
import { Router } from '@millstream/core';
import { instrumentPublisher, instrumentRouter } from '@millstream/otel';

// Router: a consumer span per message + every handler's output publisher instrumented.
const router = instrumentRouter(new Router(), { system: 'kafka' }); // before other router.use(...)
router.use(retry({ maxRetries: 3 }));

// Publishers you use directly (EventBus, outbox, …):
const publisher = instrumentPublisher(kafkaPublisher, { system: 'kafka' });
```

Uses the global tracer provider, meter provider and propagator unless you pass `tracerProvider`, `meterProvider` or
`propagator`. Trace context only flows across `await`s if a context manager is registered, which the Node SDK does
for you (`AsyncLocalStorageContextManager`).

## What you get

| Piece                                                 | Span                                      | Metrics                                                                 |
| ----------------------------------------------------- | ----------------------------------------- | ----------------------------------------------------------------------- |
| `instrumentPublisher(pub)` / `otelPublisherDecorator` | `send <topic>` (PRODUCER), one per call   | `messaging.client.operation.duration`, `messaging.client.sent.messages` |
| `otelMiddleware()`                                    | `process <topic>` (CONSUMER), per message | `messaging.process.duration`                                            |
| `instrumentRouter(router)`                            | both of the above for the Router          |                                                                         |

Attributes: `messaging.system`, `messaging.operation.type` / `.name`, `messaging.destination.name`,
`messaging.destination.subscription.name` (the handler name), `messaging.message.id`, `messaging.message.body.size`,
`messaging.batch.message_count`, and `error.type` on failures.

- **Propagation.** The send span's context is injected into every message's metadata (`traceparent`, plus
  `tracestate`/`baggage` with the default propagator). The consumer span continues it, so one trace runs from
  the HTTP request to the last consumer.
- **The consumer span is active while the handler runs.** Database calls, HTTP calls and publishes from inside the
  handler nest under it. Messages a handler _returns_ are published by the Router after the handler finished; they
  still become children of the consumer span.
- **Parent of a send span:** the consumer span that produced the message, else the active span, else the trace
  context the message already carries. The last case is what keeps a relayed message, for example one moved by the
  outbox forwarder, on its original trace.
- **`consumerParent: 'link'`** starts a new trace per message with a span link to the producer instead of a child
  span. Use it when one trace per message would get too long, for example for fan-in or long-lived flows.
- **Put `otelMiddleware` first.** One consumer span then covers retries, the poison queue and the other
  middlewares, and its duration is the full processing time.

## Outbox

```ts
// Business transaction: the send span is a child of the request span.
const outbox = instrumentPublisher(new ForwarderPublisher(new PgQueuePublisher(tx), 'outbox'));
await outbox.publish('orders.created', msg);

// Relay: instrumented router + instrumented broker publisher.
addForwarder(instrumentRouter(router), {
  subscriber: new PgQueueSubscriber(pool),
  publisher: instrumentPublisher(bullmqPublisher, { system: 'bullmq' }),
  forwarderTopic: 'outbox',
});
```

The resulting trace is
`request → send orders.created → process outbox → send orders.created → process orders.created`.

## Manual propagation

`injectTraceContext(msg, ctx?)` and `extractTraceContext(msg)` read and write the trace context in message metadata,
for code paths that don't go through the helpers above.
