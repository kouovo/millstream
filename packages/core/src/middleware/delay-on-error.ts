import type { Message } from '../message.js';
import type { Middleware } from '../router.js';

export const DELAYED_UNTIL = '_millstream_delayed_until';
export const DELAYED_FOR = '_millstream_delayed_for';
export const REQUEUER_RETRIES = '_millstream_requeuer_retries';

export interface DelayOnErrorOptions {
  initialIntervalMs?: number;
  maxIntervalMs?: number;
  multiplier?: number;
}

/**
 * On error, stamps `_millstream_delayed_until` (ISO) / `_millstream_delayed_for` (ms) and
 * rethrows. Pair with an outer PoisonQueue that publishes to a delayed queue, plus a
 * Requeuer — see `pg/requeue.ts`.
 */
export function delayOnError(opts: DelayOnErrorOptions = {}): Middleware {
  const initial = opts.initialIntervalMs ?? 10_000;
  const max = opts.maxIntervalMs ?? 3_600_000;
  const mult = opts.multiplier ?? 2;
  return (next) => async (msg) => {
    try {
      return await next(msg);
    } catch (e) {
      const retries = Number(msg.metadata[REQUEUER_RETRIES] ?? 0);
      const delay = Math.min(max, Math.round(initial * mult ** retries));
      msg.metadata[DELAYED_FOR] = String(delay);
      msg.metadata[DELAYED_UNTIL] = new Date(Date.now() + delay).toISOString();
      throw e;
    }
  };
}

/** Explicit delay for publishing (Go: `delay.Message`). */
export function withDelay(msg: Message, delayMs: number): Message {
  msg.metadata[DELAYED_FOR] = String(delayMs);
  msg.metadata[DELAYED_UNTIL] = new Date(Date.now() + delayMs).toISOString();
  return msg;
}
