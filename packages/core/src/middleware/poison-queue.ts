import { handlerContext } from '../context.js';
import type { Publisher } from '../pubsub.js';
import type { Middleware } from '../router.js';

export const POISON_REASON = 'reason_poisoned';
export const POISON_TOPIC = 'topic_poisoned';
export const POISON_HANDLER = 'handler_poisoned';

export interface PoisonQueueOptions {
  /** Default: everything. Errors rejected by the filter propagate (→ nack). */
  shouldGoToPoisonQueue?: (err: unknown) => boolean;
}

/**
 * On failure: publish a copy (with `reason_poisoned`, `topic_poisoned`, `handler_poisoned`
 * metadata) to `topic` and swallow the error so the original gets acked — this
 * unblocks the head of an ordered queue. If publishing fails, both errors propagate
 * (→ nack), so nothing is lost.
 */
export function poisonQueue(publisher: Publisher, topic: string, opts: PoisonQueueOptions = {}): Middleware {
  return (next) => async (msg) => {
    try {
      return await next(msg);
    } catch (err) {
      if (opts.shouldGoToPoisonQueue && !opts.shouldGoToPoisonQueue(err)) throw err;
      const ctx = handlerContext();
      const poisoned = msg.copy();
      poisoned.metadata[POISON_REASON] = err instanceof Error ? err.message : String(err);
      poisoned.metadata[POISON_TOPIC] = ctx?.subscribeTopic ?? '';
      poisoned.metadata[POISON_HANDLER] = ctx?.handlerName ?? '';
      try {
        await publisher.publish(topic, poisoned);
      } catch (pubErr) {
        throw new AggregateError([err, pubErr], 'cannot publish to poison queue', { cause: pubErr });
      }
      return;
    }
  };
}
