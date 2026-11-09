# @millstream/graphile-worker

[Graphile Worker](https://worker.graphile.org) (PostgreSQL job queue) `Publisher` / `Subscriber` for
[millstream](../../README.md). Peer dependency: `graphile-worker@^0.16 || ^0.17 || ^0.18`.

One topic = one Graphile **task identifier** (`taskIdentifier(topic)` to change). The publisher adds jobs with plain
SQL through any `Executor`, so jobs can be enqueued **in the same transaction as your business writes** — no outbox
relay needed. Each subscription runs a Graphile runner with a task for its topic.

```ts
import { contextExecutor } from '@millstream/core';
import { withTransaction } from '@millstream/pg';
import { GraphileWorkerPublisher, GraphileWorkerSubscriber } from '@millstream/graphile-worker';

// Enqueue atomically with business data:
await withTransaction(pool, async (tx) => {
  await tx.query('INSERT INTO orders (id) VALUES ($1)', [orderId]);
  await new GraphileWorkerPublisher(tx).publish('send-receipt', newMessage(JSON.stringify({ orderId })));
});

router.addConsumerHandler({
  name: 'send-receipt',
  subscribeTopic: 'send-receipt',
  subscriber: new GraphileWorkerSubscriber({ pgPool: pool, concurrency: 5 }),
  concurrency: 5,
  handler: async (msg) => {
    /* … */
  },
});
```

The `graphile_worker` schema must exist before publishing: a subscriber's runner migrates it on start, or call
`runMigrations({ pgPool })` from `graphile-worker` (e.g. in your migration step). `schema` changes its name on both
sides. Give the pool an `error` handler (`pool.on('error', …)`); Graphile warns otherwise.

## Who owns retries: pick one layer

| `nack`                  | What a Nack (or ack-deadline timeout) does                                                                                                      | Retry policy lives in                                         |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `'redeliver'` (default) | Redelivered **in place** after `nackResendMs` while the job stays locked, **without** consuming an attempt                                      | Router middlewares (`retry`, `poisonQueue`, `dropOnError`, …) |
| `'fail'`                | The job fails: Graphile retries it with exponential backoff (`exp(attempts)` seconds) up to `max_attempts`, then keeps it as permanently failed | Graphile (`jobSpec: () => ({ maxAttempts })`)                 |

Don't combine `'fail'` with Router `retry`, or the retry counts multiply.

## Mapping

- **Payload** is `{ uuid, payload, metadata }` — the payload as text when it is valid UTF-8, else base64 with
  `encoding: 'base64'`. Jobs added by plain Graphile code (any other JSON) are read too: the JSON becomes the message
  payload and the uuid is `graphile:<job id>`. Pass a custom `marshaler` to change both directions.
- **`jobSpec(topic, msg, { consumerGroup })`** sets Graphile's job options per message: `queueName` (a named queue runs
  its jobs one at a time), `priority`, `maxAttempts`, `runAt`, `flags`, `jobKey` / `jobKeyMode`. For dedup of a
  re-published message use `jobSpec: (_t, m) => ({ jobKey: m.uuid, jobKeyMode: 'unsafe_dedupe' })`.
- **`_millstream_delayed_until`** (`withDelay(msg, ms)`, `delayOnError`) becomes `run_at`.
- **Ordering**: Graphile picks jobs by `(priority, run_at)`; jobs without an explicit `runAt` get strictly increasing
  ones, so with `concurrency: 1`, one subscriber and `nack: 'redeliver'` they run in publish order. Graphile retries
  (`nack: 'fail'`) reorder.
- **Abort while holding a job** → it fails with `AbortedError` and runs again after Graphile's backoff, which is what
  Graphile's own shutdown does with interrupted jobs. A crashed process's jobs are unlocked by Graphile after 4 hours.
- **Completion isn't transactional with handler writes**: Graphile completes the job in its own statement, so the
  delivery is at-least-once — dedupe by `msg.uuid` (e.g. `inbox()` from `@millstream/pg`).
- `graphileJob(msg)` returns the underlying job (id, attempts, max_attempts, last_error, …). `runnerOptions` passes
  extra options to Graphile's runner (preset, events, …); Graphile's logs go to the subscriber's `logger`.

## Consumer groups (fan-out)

A task's jobs are **shared work**. To deliver every message to several consumers, list the topic's consumer groups on
the publisher; each message then becomes one job per group, for the task `<task>.<group>`, run by
`GraphileWorkerSubscriber({ consumerGroup })`. Every group retries and fails independently. `undefined` means one
shared task (default), `[]` adds nothing. As with BullMQ, the list is static configuration: a group only gets jobs
published while it is listed.
