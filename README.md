# millstream

> Inspired by [Watermill](https://github.com/ThreeDotsLabs/watermill) (Go). This isn't a 1:1 port. It keeps Watermill's
> layering and adapts it to Node.js: `AsyncIterable` + `Promise` instead of channels, `AbortSignal` + `AsyncLocalStorage`
> instead of `context.Context`, and a checked-out `PoolClient` instead of `*sql.Tx`.

Messaging toolkit for Node.js with transactional outbox, ordered PostgreSQL log, Router + middlewares.

| Package                                                   | What                                                                                                                                      |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| [`@millstream/core`](packages/core)                       | `Message` (Ack/Nack), `Publisher`/`Subscriber`, `Router` + middlewares / plugins / decorators, in-memory Pub/Sub, requeuer                |
| [`@millstream/pg`](packages/pg)                           | PostgreSQL (node-postgres / postgres.js / Kysely): xid8-ordered log, `SKIP LOCKED` queue, outbox, inbox, delayed requeue                  |
| [`@millstream/mysql`](packages/mysql)                     | MySQL 8 (mysql2 / Kysely): `SKIP LOCKED` queue, outbox, inbox, delayed requeue                                                            |
| [`@millstream/kafka`](packages/kafka)                     | Kafka on `@platformatic/kafka` ≥ 2.3: commit on ack, per-partition ordering and parallelism                                               |
| [`@millstream/bullmq`](packages/bullmq)                   | BullMQ (Redis): jobId = uuid dedup, native delays, Nack → redeliver or BullMQ attempts/backoff + failed set                               |
| [`@millstream/graphile-worker`](packages/graphile-worker) | Graphile Worker (PostgreSQL jobs): `add_job` inside your transaction, named queues, `run_at` delays, nack → redeliver or Graphile retries |
| [`@millstream/rabbitmq`](packages/rabbitmq)               | RabbitMQ on `amqplib`: publisher confirms, durable queues, consumer groups via fanout exchanges, requeue or dead-letter                   |
| [`@millstream/nats`](packages/nats)                       | NATS JetStream: `Nats-Msg-Id` dedup, durable consumers as consumer groups, ack / nak with delay                                           |
| [`@millstream/redis-stream`](packages/redis-stream)       | Redis Streams on `ioredis`: `XREADGROUP` consumer groups, `XACK` on ack, `XAUTOCLAIM` recovery                                            |
| [`@millstream/cqrs`](packages/cqrs)                       | Typed `EventBus` / `EventProcessor`: explicit event names, Standard Schema validation, any transport underneath                           |
| [`@millstream/forwarder`](packages/forwarder)             | Outbox forwarder: envelope publisher + relay handler to any broker                                                                        |
| [`@millstream/otel`](packages/otel)                       | OpenTelemetry: producer/consumer spans, trace context through message metadata, messaging metrics                                         |
| [`@millstream/test-kit`](packages/test-kit)               | Universal Pub/Sub contract tests (works with vitest, `node:test`, jest…) for your own adapters                                            |

Requires Node.js ≥ 22, plus PostgreSQL ≥ 13 for `@millstream/pg`, MySQL ≥ 8.0 for `@millstream/mysql`, Kafka ≥ 3.5
for `@millstream/kafka`, Redis (BullMQ 5) for `@millstream/bullmq`, Graphile Worker ≥ 0.16 for `@millstream/graphile-worker`, RabbitMQ 3.x / 4.x for `@millstream/rabbitmq`,
NATS ≥ 2.10 with JetStream for `@millstream/nats`, and Redis ≥ 6.2 for `@millstream/redis-stream`.

## Quick look

```ts
import { Router, newMessage, poisonQueue, retry, recoverer, contextExecutor, txFromContext } from '@millstream/core';
import { PgPublisher, PgSubscriber, initializeSchema, withTransaction } from '@millstream/pg';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import pg from 'pg';

const pool = new pg.Pool();
await initializeSchema(pool, { logTopics: ['outbox', 'poison'] }); // normally in a migration

// 1) Business code: write data + message in ONE transaction (outbox).
await withTransaction(pool, async (tx) => {
  await tx.query('INSERT INTO orders (id) VALUES ($1)', [orderId]);
  await new ForwarderPublisher(new PgPublisher(tx), 'outbox').publish(
    'orders.created',
    newMessage(JSON.stringify({ orderId })),
  );
});

// 2) Relay: outbox → real broker (anything implementing Publisher).
const router = new Router();
router.use(recoverer, poisonQueue(new PgPublisher(contextExecutor(pool)), 'poison'), retry({ maxRetries: 3 }));
addForwarder(router, { subscriber: new PgSubscriber(pool), publisher: kafkaPublisher, forwarderTopic: 'outbox' });

// 3) Consumers: handler writes join the subscriber's transaction → effect happens exactly once.
router.addConsumerHandler({
  name: 'project-orders',
  subscribeTopic: 'orders',
  subscriber: new PgSubscriber(pool, { consumerGroup: 'projection' }),
  handler: async (msg) => {
    await txFromContext()!.query('INSERT INTO order_view ...');
  },
});

await router.run(AbortSignal.timeout(60_000)); // or router.close() on SIGTERM
```

## Semantics worth knowing

- **At-least-once by default.** The Router publishes a handler's output first and acks the input after, so a crash
  between the two redelivers the input. Exactly-once _effects_ only hold for writes that go through
  `txFromContext()` while consuming from `@millstream/pg` or `@millstream/mysql`.
- **The PG log subscriber never skips a late commit.** `bigserial` order ≠ commit order. Progress is tracked as
  `(transaction_id, offset)`, and the subscriber reads only rows from transactions older than `pg_snapshot_xmin`. The
  cost is that a long transaction _anywhere_ in the instance delays consumption, so monitor `pg_stat_activity`.
- **One active consumer per group per topic** (the bookmark row is `FOR UPDATE`-locked). To parallelise, split topics
  by key, or use the queue subscriber (`SKIP LOCKED`, unordered).
- **Transports always redeliver a nacked message; failure policy is a middleware choice.** Retry, poison queue, delayed
  requeue, `dropOnError` (log and move on) or `instantAck` are picked per handler. See
  [failure policies](packages/core/README.md#failure-policies). Best-effort in-process events are `MemoryPubSub` +
  `dropOnError`, with no special transport.
- **Nack = in-place redelivery and head-of-line blocking.** Pair SQL subscribers with `poisonQueue` or
  `pgDelayedRequeuer`. Retries belong to _either_ the Router or the broker, not both.

### Differences from Watermill

- **Pull-based subscriptions.** `subscribe()` returns an `AsyncIterable`. A producer loop drives delivery, so an Ack
  commits immediately and aborting stops the loop immediately, even if the consumer still holds a message.
- **Nack undoes the handler's writes.** The SQL subscriber wraps every delivery in a `SAVEPOINT`, and `retry()` does the
  same per attempt, so a failed attempt's transactional writes are rolled back.
- **The transaction handle expires.** `msg.tx` is revoked once the delivery is settled, so a slow handler can't write
  through a connection that is already back in the pool.
- **Reliable inbox dedup.** `inbox()` (transactional) replaces Watermill's in-memory, payload-hash Deduplicator.
- **`LISTEN/NOTIFY` wake-ups** on top of polling (`PgNotifier`). They are only a hint; polling still runs.
- **No MySQL log-type subscriber.** It needs serializable locking reads that block producers. `@millstream/mysql` is
  queue-type only (`SKIP LOCKED`, delete on ack); use it as an outbox and fan out in the broker, or use CDC.

## Development

Uses pnpm workspaces, vitest, oxlint + oxfmt, and changesets.

```sh
pnpm install
pnpm db:up            # PostgreSQL 16 :55432, MySQL 8.4 :53306, Redis 7 :56379, Kafka 4.1 (KRaft) :59092,
                      # RabbitMQ 4 :55672, NATS 2.11 (JetStream) :54222 (docker compose)
pnpm check            # fmt:check + lint + typecheck + vitest (runs against src/)
pnpm build            # tsdown (Rolldown) → packages/*/dist
pnpm test:node        # contract suite on the built dist with plain `node --test`
```

## License

MIT
