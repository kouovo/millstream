# @millstream/graphile-worker

## 0.5.0

### Patch Changes

- Updated dependencies [a11948b]
  - @millstream/core@0.5.0

## 0.4.0

### Patch Changes

- @millstream/core@0.4.0

## 0.3.0

### Minor Changes

- d761e25: New transports: `@millstream/graphile-worker` (Graphile Worker: jobs added with SQL inside your transaction, publish order kept, named queues, `run_at` delays, nack → in-place redelivery or Graphile retries), `@millstream/rabbitmq` (amqplib: publisher confirms, durable queues, consumer groups via fanout exchanges, nack → requeue or dead-letter), `@millstream/nats` (JetStream: `Nats-Msg-Id` dedup, durable consumers as consumer groups) and `@millstream/redis-stream` (ioredis: `XREADGROUP` consumer groups, `XACK` on ack, `XAUTOCLAIM` recovery). All pass the `@millstream/test-kit` contract suite.

### Patch Changes

- Updated dependencies [97d9ca5]
- Updated dependencies [dd9f92e]
  - @millstream/core@0.3.0
