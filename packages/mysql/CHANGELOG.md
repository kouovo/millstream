# @millstream/mysql

## 0.3.0

### Patch Changes

- Updated dependencies [97d9ca5]
- Updated dependencies [dd9f92e]
  - @millstream/core@0.3.0

## 0.2.0

### Minor Changes

- 76e20ce: Add `@millstream/mysql`: MySQL 8 queue-type Pub/Sub (`FOR UPDATE SKIP LOCKED`, delete on ack) for mysql2 and Kysely, with transactional outbox publishing, `inbox()` dedup, `withTransaction` and `mysqlDelayedRequeuer`.
- 9ff93d8: `@millstream/bullmq`: consumer groups (fan-out). `BullMQPublisher({ consumerGroups })` adds each message once per group to the group's own queue; `BullMQSubscriber({ consumerGroup })` reads it. Groups retry and fail independently, and `jobOptions` receives the `consumerGroup`.
  
  `@millstream/pg` / `@millstream/mysql`: `pruneInbox(driver, { olderThanMs, consumer?, batchSize? })` deletes old inbox records in batches. The inbox table gets a `processed_at` index; existing PostgreSQL schemas should add it in a migration (`CREATE INDEX millstream_inbox_processed_at ON millstream_inbox (processed_at)`). MySQL timestamps now default to UTC.
