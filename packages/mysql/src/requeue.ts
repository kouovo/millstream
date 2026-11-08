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
import { type MysqlDriverLike, toMysqlDriver } from './driver.js';
import { MysqlQueuePublisher } from './publisher.js';
import { MysqlQueueSubscriber, type MysqlSubscriberOptions } from './subscriber.js';

export interface MysqlDelayedRequeuerOptions {
  driver: MysqlDriverLike;
  router: Router;
  /** Queue-type topic holding delayed messages. Default 'requeue'. */
  requeueTopic?: string;
  delay?: DelayOnErrorOptions;
  /** Requeues before giving up; requires `deadLetter`. */
  maxRetries?: number;
  deadLetter?: { publisher: Publisher; topic: string };
  /** Where requeued messages go. Default: MysqlQueuePublisher joining the requeuer's tx. */
  publisher?: Publisher;
  subscriberOptions?: MysqlSubscriberOptions;
}

/**
 * Delayed retry without head-of-line blocking:
 *
 *   handler fails → DelayOnError stamps `delayed_until` → PoisonQueue writes the message to
 *   the delayed queue table (inside the subscriber's tx) and swallows the error → original
 *   acked → once due, the Requeuer moves it back to its topic (publish + delete in one tx).
 *
 * Returns middlewares to put on the handlers that should get delayed retries.
 */
export function mysqlDelayedRequeuer(opts: MysqlDelayedRequeuerOptions): {
  middlewares: Middleware[];
  handler: Handler;
} {
  const requeueTopic = opts.requeueTopic ?? 'requeue';
  const driver = toMysqlDriver(opts.driver);
  const exec = contextExecutor(driver);
  const handler = addRequeuer(opts.router, {
    handlerName: `requeuer_${requeueTopic}`,
    requeueTopic,
    subscriber: new MysqlQueueSubscriber(driver, opts.subscriberOptions),
    publisher: opts.publisher ?? new MysqlQueuePublisher(exec),
    maxRetries: opts.maxRetries,
    deadLetter: opts.deadLetter,
  });
  return {
    // PoisonQueue outermost, DelayOnError inside it.
    middlewares: [poisonQueue(new MysqlQueuePublisher(exec), requeueTopic), delayOnError(opts.delay)],
    handler,
  };
}
