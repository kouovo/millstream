---
'@millstream/graphile-worker': minor
'@millstream/rabbitmq': minor
'@millstream/nats': minor
'@millstream/redis-stream': minor
---

New transports: `@millstream/graphile-worker` (Graphile Worker: jobs added with SQL inside your transaction, publish order kept, named queues, `run_at` delays, nack → in-place redelivery or Graphile retries), `@millstream/rabbitmq` (amqplib: publisher confirms, durable queues, consumer groups via fanout exchanges, nack → requeue or dead-letter), `@millstream/nats` (JetStream: `Nats-Msg-Id` dedup, durable consumers as consumer groups) and `@millstream/redis-stream` (ioredis: `XREADGROUP` consumer groups, `XACK` on ack, `XAUTOCLAIM` recovery). All pass the `@millstream/test-kit` contract suite.
