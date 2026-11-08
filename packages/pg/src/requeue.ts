import {
  addRequeuer,
  contextExecutor,
  delayOnError,
  type DelayOnErrorOptions,
  type Handler,
  type Middleware,
  poisonQueue,
  type Publisher,
  type Router,
} from '@millstream/core';
import { type SqlDriverLike, toSqlDriver } from './driver.js';
import { PgPublisher, PgQueuePublisher } from './publisher.js';
import { PgQueueSubscriber, type PgSubscriberOptions } from './subscriber.js';

export interface PgDelayedRequeuerOptions {
  driver: SqlDriverLike;
  router: Router;
  /** Queue-type topic holding delayed messages. Default 'requeue'. */
  requeueTopic?: string;
  delay?: DelayOnErrorOptions;
  /** Requeues before giving up; requires `deadLetter`. */
  maxRetries?: number;
  deadLetter?: { publisher: Publisher; topic: string };
  /** Where requeued messages go. Default: log-type PgPublisher joining the requeuer's tx. */
  publisher?: Publisher;
  subscriberOptions?: PgSubscriberOptions;
}

/**
 * Delayed retry without head-of-line blocking (Watermill `NewPostgreSQLDelayedRequeuer`):
 *
 *   handler fails → DelayOnError stamps `delayed_until` → PoisonQueue writes the message to
 *   the delayed queue table (inside the subscriber's tx) and swallows the error → original
 *   acked, the topic keeps flowing → once due, the Requeuer moves it back to its topic
 *   (publish + delete in one transaction).
 *
 * Trade-off: the requeued message goes to the tail — per-key ordering is broken.
 * Returns middlewares to put on the handlers that should get delayed retries.
 */
export function pgDelayedRequeuer(opts: PgDelayedRequeuerOptions): { middlewares: Middleware[]; handler: Handler } {
  const requeueTopic = opts.requeueTopic ?? 'requeue';
  const driver = toSqlDriver(opts.driver);
  const exec = contextExecutor(driver);
  const handler = addRequeuer(opts.router, {
    handlerName: `requeuer_${requeueTopic}`,
    requeueTopic,
    subscriber: new PgQueueSubscriber(driver, opts.subscriberOptions),
    publisher: opts.publisher ?? new PgPublisher(exec),
    maxRetries: opts.maxRetries,
    deadLetter: opts.deadLetter,
  });
  return {
    // PoisonQueue outermost, DelayOnError inside it.
    middlewares: [poisonQueue(new PgQueuePublisher(exec), requeueTopic), delayOnError(opts.delay)],
    handler,
  };
}
