import { handlerContext } from '../context.js';
import type { Message } from '../message.js';
import type { Middleware } from '../router.js';

export interface DropEvent {
  message: Message;
  error: unknown;
  handlerName?: string;
  topic?: string;
}

export interface DropOnErrorOptions {
  /** Report the dropped message (log / count it). A throwing hook is ignored. */
  onDrop?: (event: DropEvent) => void;
  /** Default: drop on every error. Errors rejected by the filter propagate (→ nack → redelivery). */
  shouldDrop?: (error: unknown) => boolean;
}

/**
 * Best-effort handling: on failure, report and **ack** the message instead of nacking it,
 * so it is not redelivered and doesn't block the messages after it. Like `poisonQueue`,
 * minus storing the message — use it only where losing a message is acceptable
 * (post-commit notifications, cache invalidation, logging, fan-out to websockets).
 *
 * Compose with `timeout` (skip stuck handlers) and `retry` (a few attempts first):
 * `[dropOnError({ onDrop }), timeout(5_000), retry({ maxRetries: 2 })]`.
 */
export function dropOnError(opts: DropOnErrorOptions = {}): Middleware {
  return (next) => async (msg) => {
    try {
      return await next(msg);
    } catch (error) {
      if (opts.shouldDrop && !opts.shouldDrop(error)) throw error;
      const ctx = handlerContext();
      try {
        opts.onDrop?.({ message: msg, error, handlerName: ctx?.handlerName, topic: ctx?.subscribeTopic });
      } catch {
        // A failing report must not turn the drop into a nack.
      }
      return;
    }
  };
}
