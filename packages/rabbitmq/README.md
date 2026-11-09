# @millstream/rabbitmq

RabbitMQ (AMQP 0-9-1) `Publisher` / `Subscriber` for [millstream](../../README.md), built on
[`amqplib`](https://github.com/amqp-node/amqplib). Peer dependency: `amqplib@^1.2 || ^2`.

```ts
import { RabbitMQPublisher, RabbitMQSubscriber } from '@millstream/rabbitmq';

const url = 'amqp://guest:guest@localhost:5672';
const publisher = new RabbitMQPublisher({ url, consumerGroups: (topic) => registry[topic] }); // e.g. ['billing']

router.addConsumerHandler({
  name: 'billing',
  subscribeTopic: 'orders.created',
  subscriber: new RabbitMQSubscriber({ url, consumerGroup: 'billing', concurrency: 4 }),
  concurrency: 4,
  handler: async (msg) => {
    /* … */
  },
});
```

Pass `{ connection }` instead of `{ url }` to share an `amqplib` connection (you close it then).

## Topology

| Thing       | Default                                                     | Option                         |
| ----------- | ----------------------------------------------------------- | ------------------------------ |
| Exchange    | `<topic>`, `fanout`, durable                                | `exchangeName`, `exchangeType` |
| Queue       | `<topic>` (no group) or `<topic>.<group>`, durable          | `queueName`, `queueOptions`    |
| Binding     | queue → exchange, binding key = topic (ignored by `fanout`) | `bindingKey`                   |
| Routing key | the topic                                                   | `routingKey` (publisher)       |

Subscribers declare their exchange, queue and binding. RabbitMQ drops messages that no queue is bound for, so the
publisher declares queues too before the first publish to a topic:

- `consumerGroups` returns `undefined` (default): the topic's shared queue `<topic>`;
- it returns `['billing', 'email']`: one queue per group, so every group gets every message even before its first
  subscriber starts;
- it returns `[]`: nothing (plain pub/sub: only running subscribers' queues receive).

Publisher and subscribers must declare queues with the same `queueOptions` (quorum queues, dead-lettering, TTL, …),
or RabbitMQ refuses the declaration with `PRECONDITION_FAILED`.

## Semantics

- **Publisher confirms.** `publish()` resolves once the broker has taken every message (persisted, for durable
  queues). Messages are persistent; `messageId` = `msg.uuid`, metadata = headers.
- **Ack → `basic.ack`.** **Nack → requeue** after `nackResendMs` (default 100ms), so retries belong to Router
  middlewares. With `nack: 'reject'` a nacked message is rejected without requeue and dead-lettered through the queue's
  `x-dead-letter-exchange` (`queueOptions: () => ({ deadLetterExchange: 'dlx' })`), or **dropped** if it has none.
  This is Watermill AMQP's `NoRequeueOnNack`.
- **Ordering**: per queue, with one consumer and `concurrency: 1` (the default prefetch). Competing consumers or
  `concurrency > 1` process messages in parallel.
- **Abort while holding a message** → requeued immediately. Messages unacked when a channel or connection dies are
  requeued by RabbitMQ (at-least-once — dedupe by `msg.uuid`).
- **Failures**: a closed channel or connection is logged and the subscription re-established after `reconnectMs`
  (an owned connection is reopened).
- RabbitMQ's `consumer_timeout` (30 min by default) closes the channel of a consumer that holds a message longer;
  keep handlers shorter or raise it on the broker.
- `_millstream_delayed_until` has no native counterpart (needs the delayed-message plugin); use a delayed requeuer.
- `rabbitmqInfo(msg)` returns exchange / routing key / delivery tag / `redelivered`. To exchange messages with Go
  Watermill services, set `uuidHeader: WATERMILL_UUID_HEADER` on both sides.
