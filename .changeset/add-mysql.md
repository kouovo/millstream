---
'@millstream/mysql': minor
---

Add `@millstream/mysql`: MySQL 8 queue-type Pub/Sub (`FOR UPDATE SKIP LOCKED`, delete on ack) for mysql2 and Kysely, with transactional outbox publishing, `inbox()` dedup, `withTransaction` and `mysqlDelayedRequeuer`.
