# @millstream/mysql

MySQL (≥ 8.0) backend for [millstream](../../README.md). Works with **mysql2** or **Kysely**. The drivers are
optional peer dependencies, so install the one you use.

```ts
import mysql from 'mysql2/promise';
import { MysqlQueuePublisher, MysqlQueueSubscriber, withTransaction } from '@millstream/mysql';

const pool = mysql.createPool(url); // a callback `mysql2` pool works too

// Outbox: business write + message in one transaction.
await withTransaction(pool, async (tx) => {
  await tx.query('INSERT INTO orders (id) VALUES (?)', [orderId]);
  await new MysqlQueuePublisher(tx).publish('outbox', msg);
});

new MysqlQueueSubscriber(pool);

// Kysely (MySQL dialect):
import { fromKysely, kyselyTx } from '@millstream/mysql/kysely';
new MysqlQueueSubscriber(fromKysely(db));
await db.transaction().execute((trx) => new MysqlQueuePublisher(kyselyTx(trx)).publish('outbox', msg));
```

Anything else can implement `MysqlDriver` (`query(text, params)` with `?` placeholders, plus
`transaction(fn, opts)` on a single connection).

- `MysqlQueuePublisher(executor)`: `publish` is a multi-row INSERT. Pass the transaction's executor (or
  `contextExecutor(driver)`) to write an outbox atomically with your data. Honours `_millstream_delayed_until`.
- `MysqlQueueSubscriber(driver)`: claims rows with `FOR UPDATE SKIP LOCKED` and deletes them on ack. Many subscribers
  run in parallel; unordered, no consumer groups. Each message gets its own transaction, so handler writes through
  `txFromContext()` commit atomically with the ack. Nack rolls back to a savepoint and redelivers.
- `withTransaction(driver, fn)`.
- `inbox({ consumer })`: transactional dedup. `pruneInbox(driver, { olderThanMs })` deletes old records in batches; run
  it from a scheduled job.
- `mysqlDelayedRequeuer(...)`: retry later without blocking the queue.
- `initializeSchema(driver, { queueTopics, inbox })`, or the `*SchemaSql()` helpers for your migrations.

## Semantics worth knowing

- **Queue type only.** There is no ordered, log-type subscriber with consumer groups. `AUTO_INCREMENT` order isn't
  commit order, and MySQL has no equivalent of PostgreSQL's `pg_snapshot_xmin` to tell which transactions have
  finished. Reading `id > last_acked` would silently skip a row whose transaction commits late, and the only fix is
  locking reads that block producers. For fan-out, relay the queue to a broker (`@millstream/forwarder`) and fan
  out there.
- **Claims run under `READ COMMITTED`.** The claim transaction locks only the claimed row: no gap or next-key locks,
  so publishers are never blocked, and rows the scan skipped (for example ones that weren't due yet) stay
  claimable by other subscribers. Handler writes that join the subscriber transaction run at that isolation level
  too. Business transactions use your server default.
- **`delayed_until` is UTC.** It's written from `_millstream_delayed_until` and compared with `UTC_TIMESTAMP(6)`,
  independent of the session time zone.
- **Inbox keys are at most 255 characters.** Longer consumers or keys are rejected rather than truncated into
  collisions. A duplicate only aborts the INSERT statement, not the transaction (InnoDB).
- **Ack deadline.** An unanswered message holds a row lock and a pooled connection. After `ackDeadlineMs`
  (default 30s) the transaction rolls back and the message is redelivered.
- **No wake-ups.** MySQL has no `LISTEN/NOTIFY`, so subscribers poll (`pollIntervalMs`, default 1s).
