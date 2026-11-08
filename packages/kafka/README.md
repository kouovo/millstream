# @millstream/kafka

Apache Kafka `Publisher` / `Subscriber` for [millstream](../../README.md), built on
[`@platformatic/kafka`](https://github.com/platformatic/kafka) (pure TypeScript, no native addon). Peer dependency:
`@platformatic/kafka@^1.30`.

```ts
import { KafkaPublisher, KafkaSubscriber } from '@millstream/kafka';

const publisher = new KafkaPublisher({
  bootstrapBrokers: ['localhost:9092'],
  partitionKey: (topic, msg) => msg.metadata.order_id, // same key → same partition → ordered
});

router.addConsumerHandler({
  name: 'billing',
  subscribeTopic: 'orders.created',
  subscriber: new KafkaSubscriber({
    bootstrapBrokers: ['localhost:9092'],
    consumerGroup: 'billing',
    partitionConcurrency: 4,
  }),
  concurrency: 4,
  handler: async (msg) => {
    /* … */
  },
});
```

## Semantics

- **Ack → commit** `offset + 1` of that partition. Unanswered messages are never committed; after a restart or
  rebalance they come back (at-least-once — dedupe by `msg.uuid`, e.g. with `inbox()` from `@millstream/pg`).
- **Nack → in-place redelivery** of a fresh copy after `nackResendMs` (Kafka has no per-message nack). A poison
  message therefore blocks **its partition only**: add `poisonQueue` / `retry` middlewares on the Router.
- **Ordering**: strict per partition. `partitionConcurrency: n` processes up to `n` partitions in parallel, each still
  sequential; give the Router handler the same `concurrency`.
- **New consumer groups** start at the earliest offset (`fallbackMode: 'latest'` to change).
- **uuid & metadata** travel as headers (`_watermill_message_uuid` + one header per metadata key). Records produced by
  other clients get a deterministic uuid `kafka:<topic>:<partition>:<offset>`.
- `kafkaInfo(msg)` returns topic / partition / offset / key / timestamp of a consumed message.

## Producer notes

- The publisher's producer is **idempotent by default**. When part of a multi-partition Produce request fails (for
  example `NOT_LEADER_OR_FOLLOWER` right after topic creation or during a leader change), `@platformatic/kafka`
  resends the whole request, including partitions the broker already acknowledged. Without idempotence those records
  are appended again, while `send()` still succeeds. With idempotence the broker discards the resends.
- Sends are **serialised per producer**: with several idempotent sends in flight, one that overtakes another's retry
  can land out of order ([platformatic/kafka#472](https://github.com/platformatic/kafka/issues/472)). For throughput,
  pass many messages to one `publish()` call.
- Requires `@platformatic/kafka` ≥ 2.3: older versions silently dropped concurrent idempotent sends
  ([#326](https://github.com/platformatic/kafka/issues/326)).
