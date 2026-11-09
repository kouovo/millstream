# @millstream/redis-stream

[Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) `Publisher` / `Subscriber` for
[millstream](../../README.md), built on [`ioredis`](https://github.com/redis/ioredis). Peer dependency:
`ioredis@^5 || ^6`; Redis ≥ 6.2 (`XAUTOCLAIM`).

```ts
import { Redis } from 'ioredis';
import { RedisStreamPublisher, RedisStreamSubscriber } from '@millstream/redis-stream';

const client = new Redis('redis://localhost:6379');
const publisher = new RedisStreamPublisher({ client, maxLen: 100_000 });

router.addConsumerHandler({
  name: 'billing',
  subscribeTopic: 'orders.created',
  subscriber: new RedisStreamSubscriber({ client, consumerGroup: 'billing' }),
  handler: async (msg) => {
    /* … */
  },
});
```

The client belongs to you. Each subscription opens a `duplicate()` of it for blocking reads.

## Semantics

- **Topic = stream key** (`streamName` to change). `publish()` is one `XADD` per message inside a `MULTI`, with fields
  `uuid`, `payload` (binary-safe) and `metadata` (JSON). `maxLen` trims the stream (`MAXLEN ~ n`) — trimmed entries
  are gone even for groups that haven't read them.
- **`consumerGroup`** → `XREADGROUP`: each group gets every message, subscribers of one group compete for them, and
  progress lives in Redis. New groups start at `startId` (`'0'`: everything, default; `'$'`: only new entries).
- **No group** → `XREAD` from `startId` with the position in memory: every subscription sees the whole stream.
- **Ack → `XACK`.** **Nack → in-place redelivery** of a fresh copy after `nackResendMs` (the entry stays pending), so
  a poison message blocks its subscription: add `poisonQueue` / `retry` middlewares on the Router.
- **Ordering**: strict with `concurrency: 1` and one consumer per group. `concurrency: n` handles up to `n` entries in
  parallel.
- **Crashes**: entries pending on a dead consumer for `claimIdleMs` (30s) are claimed with `XAUTOCLAIM` by the
  group's other subscribers (checked every `claimIntervalMs`), which can reorder them. Entries being handled have their
  idle time reset, so slow handlers aren't claimed. **Abort while holding an entry** makes it claimable at once.
- A subscription's consumer (`millstream-<uuid>`, or `consumerName`) is removed from the group when it stops with
  nothing pending.
- `redisStreamInfo(msg)` returns stream / entry id / group. Entries added by other clients get the uuid
  `redis:<stream>:<id>`, and their fields (as JSON) as the payload.
