# @millstream/pg

PostgreSQL (≥ 13) backend for [millstream](../../README.md). Works with **node-postgres**, **postgres.js** or
**Kysely**. The drivers are optional peer dependencies, so install the one you use.

```ts
// node-postgres `Pool` or postgres.js `sql`: pass them directly.
new PgSubscriber(pool);
new PgSubscriber(sql);

// Kysely (any PostgreSQL dialect):
import { fromKysely, kyselyTx } from '@millstream/pg/kysely';
new PgSubscriber(fromKysely(db));
await db.transaction().execute((trx) => new PgPublisher(kyselyTx(trx)).publish('outbox', msg));
```

Anything else can implement `SqlDriver` (`query(text, params)` plus `transaction(fn)` on a single connection).

- `PgPublisher(executor)`: `publish` is a multi-row INSERT. Pass the transaction's executor (or
  `contextExecutor(driver)`) to write an outbox atomically with your data.
- `PgSubscriber(driver, { consumerGroup })`: log type, ordered, consumer groups. It never skips a late commit
  (xid8 plus snapshot-xmin watermark). Each message gets its own transaction; Nack rolls back to a savepoint and
  redelivers.
- `PgQueuePublisher` / `PgQueueSubscriber`: queue type using `FOR UPDATE SKIP LOCKED`. Messages are deleted on ack,
  processed in parallel and unordered. Honours `_millstream_delayed_until`.
- `withTransaction(driver, fn)`.
- `inbox({ consumer })`: transactional dedup. `pruneInbox(driver, { olderThanMs })` deletes old records in batches; run
  it from a scheduled job.
- `pgDelayedRequeuer(...)`: retry later without blocking the head of the queue.
- `PgNotifier.nodePostgres(() => new pg.Client(url))` / `PgNotifier.postgresJs(sql)`: LISTEN/NOTIFY wake-ups.
- `initializeSchema(driver, { logTopics, queueTopics, inbox })`, or the `*SchemaSql()` helpers for your migrations.
