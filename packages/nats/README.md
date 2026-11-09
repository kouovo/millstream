# @millstream/nats

NATS JetStream `Publisher` / `Subscriber` for [millstream](../../README.md), built on the
[`@nats-io/*` v3 modules](https://github.com/nats-io/nats.js). Peer dependencies: `@nats-io/jetstream@^3`,
`@nats-io/nats-core@^3` (plus a transport such as `@nats-io/transport-node` to connect).

```ts
import { connect } from '@nats-io/transport-node';
import { NatsPublisher, NatsSubscriber } from '@millstream/nats';

const connection = await connect({ servers: 'localhost:4222' });
const publisher = new NatsPublisher({ connection });

router.addConsumerHandler({
  name: 'billing',
  subscribeTopic: 'orders.created',
  subscriber: new NatsSubscriber({ connection, consumerGroup: 'billing' }),
  handler: async (msg) => {
    /* … */
  },
});
```

The connection belongs to you: `close()` doesn't close it.

## Streams and consumers

- **Topic = subject.** By default each topic gets its own stream named after it (`orders.created` →
  `orders_created`, created on first use with `subjects: [topic]`, like Watermill's `AutoProvision`).
  `autoProvision: (topic) => ({ max_age, num_replicas, … })` adds stream settings. With streams managed elsewhere
  (one `ORDERS` stream for `orders.>`), pass `stream: () => 'ORDERS'` and `autoProvision: false`.
- **`consumerGroup`** → a durable pull consumer `<group>_<topic>` (`durableName` to change). Each group gets every
  message; subscribers of one group compete for them; progress survives restarts. A durable consumer created
  earlier with other settings (e.g. a new `concurrency`) is updated.
- **No group** → an ephemeral consumer per `subscribe()` that starts at the beginning of the stream
  (`deliverPolicy: 'new'` to only get new messages), deleted when the subscription ends.

## Semantics

- **Publish** waits for every PubAck. `Nats-Msg-Id` = `msg.uuid`, so the stream drops a re-published message
  within its `duplicate_window` (2 min by default) — a forwarder retrying after a crash doesn't duplicate.
  Metadata = headers.
- **Ack → `+ACK`**, confirmed by the server (`ackAck`). **Nack → `-NAK`** with a `nackResendMs` delay (default
  100ms). `consumerConfig: { max_deliver, backoff }` hands retries to JetStream instead.
- **Ordering**: `max_ack_pending = concurrency`, so with the default `concurrency: 1` a consumer is strictly ordered
  and holds the next message until the current one is acked.
- **Long handlers**: the server redelivers a message not acked within `ackWaitMs` (30s). Held messages are kept alive
  with `working()`, so this only matters when a process dies.
- **Abort while holding a message** → `-NAK` right away (redelivered to the group's next subscriber).
- `natsInfo(msg)` returns stream / stream sequence / delivery count. Messages published without an id get a
  deterministic uuid `nats:<stream>:<seq>`. Set `uuidHeader: WATERMILL_UUID_HEADER` to exchange messages with Go
  Watermill services.
