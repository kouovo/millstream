# @millstream/bullmq

[BullMQ](https://docs.bullmq.io) (Redis) `Publisher` / `Subscriber` for [millstream](../../README.md). Peer
dependency: `bullmq@^5`.

One topic = one BullMQ queue (`queueName(topic)` to change). Subscribers pull jobs with BullMQ's manual processing API
and extend the job lock while the handler runs.

```ts
import { BullMQPublisher, BullMQSubscriber } from '@millstream/bullmq';

const connection = { host: 'localhost', port: 6379 };
const publisher = new BullMQPublisher({ connection });

router.addConsumerHandler({
  name: 'send-email',
  subscribeTopic: 'email.requested',
  subscriber: new BullMQSubscriber({ connection, concurrency: 5 }),
  concurrency: 5,
  handler: async (msg) => {
    /* … */
  },
});
```

## Who owns retries: pick one layer

Like Watermill's adapters, a Nack maps to BullMQ's native primitives, and BullMQ's own features are passed through
rather than re-implemented:

| `nack`                  | What a Nack (or ack-deadline timeout) does                                                                                   | Retry policy lives in                                         |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `'redeliver'` (default) | Back to the queue after `nackResendMs`, **without** consuming an attempt                                                     | Router middlewares (`retry`, `poisonQueue`, `dropOnError`, …) |
| `'fail'`                | `moveToFailed`: BullMQ's `attempts` / `backoff` apply; exhausted jobs stay in the **failed set** (dashboards, `job.retry()`) | BullMQ job options                                            |

`'fail'` is the counterpart of Watermill AMQP's `NoRequeueOnNack` plus a dead-letter exchange. Don't combine it with
Router `retry`, or the retry counts multiply.

```ts
// BullMQ owns retries (e.g. to keep an existing Bull Board workflow):
const publisher = new BullMQPublisher({
  connection,
  jobOptions: () => ({ attempts: 5, backoff: { type: 'exponential', delay: 1_000 }, removeOnComplete: 1_000 }),
});
const subscriber = new BullMQSubscriber({ connection, nack: 'fail' });
```

## Permanent errors: skip the remaining attempts

With `nack: 'fail'`, every failure consumes an attempt. To stop retrying an error that can never succeed (invalid
input, a business rule), discard the job in a middleware. BullMQ then moves it straight to the failed set:

```ts
const failPermanently =
  (isPermanent: (e: unknown) => boolean): Middleware =>
  (next) =>
  async (msg) => {
    try {
      return await next(msg);
    } catch (e) {
      if (isPermanent(e)) bullmqJob(msg)?.discard();
      throw e;
    }
  };
```

## Mapping

- **jobId = `msg.uuid`.** BullMQ ignores a job whose id already exists, so re-publishing the same message (for example
  a forwarder retrying after a crash) is deduplicated while the earlier job is retained. Ids BullMQ rejects are escaped:
  `:` → `_`, and pure integers get an `id-` prefix.
- **`_millstream_delayed_until`** (`withDelay(msg, ms)`, `delayOnError`) becomes a native `delay`.
- **`jobOptions(topic, msg, { consumerGroup })`** passes native options through (attempts, backoff, priority, removeOnComplete, …),
  merged over jobId/delay. `workerOptions` does the same for the worker (lockDuration, limiter, prefix, …).
- **Job data** is `{ uuid, payload (base64), metadata }`, with the job name set to the topic. Pass a custom
  `marshaler` to read or write plain BullMQ jobs. Jobs the marshaler can't read are moved to the failed set instead of
  blocking the queue.
- **Abort while holding a job** → `moveToWait`: it's available again immediately and doesn't count as an attempt.
  A crashed consumer's jobs return after `lockDuration` through BullMQ's stalled-job check.
- `bullmqJob(msg)` returns the underlying `Job` (attemptsMade, timestamps, …).

## Consumer groups (fan-out)

A queue is **shared work**: competing consumers, no broadcast, no global order. To deliver every message to several
consumers, list the topic's consumer groups on the publisher. Each message is then added once per group, to the
group's own queue (`<queue>.<group>`):

```ts
const publisher = new BullMQPublisher({
  connection,
  consumerGroups: (topic) => registry[topic], // e.g. ['billing', 'email']
  jobOptions: (topic, msg, { consumerGroup }) => ({ attempts: consumerGroup === 'billing' ? 10 : 3 }),
});

new BullMQSubscriber({ connection, consumerGroup: 'billing', nack: 'fail' }); // reads `<topic>.billing`
```

- **Every group retries, fails and is replayed independently**, with its own attempts, backoff and failed set.
  Subscribers of the same group compete for its jobs.
- **`undefined`** means the topic isn't grouped and uses one shared queue (the default). **`[]`** means the topic has
  no consumers, so nothing is added.
- **The group list is static configuration.** Unlike Kafka or a PostgreSQL log, BullMQ keeps no per-group offset. A
  group only receives messages published while it's listed, and removing a group from the list stops its queue from
  growing. Derive the list from your handler registry and deploy the publisher with it.
- **Publishing isn't atomic across groups.** A failed publish is retried as a whole, and the jobId (`msg.uuid`)
  deduplicates the groups that already got the message.
