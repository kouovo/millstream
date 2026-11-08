import { handlerContext } from '../context.js';
import type { Middleware } from '../router.js';
import { sleep } from '../util.js';

/** Global rate limit: at most `count` messages per `perMs` across all callers of this middleware. */
export function throttle(count: number, perMs: number): Middleware {
  const interval = perMs / count;
  let nextSlot = 0;
  return (next) => async (msg) => {
    const now = Date.now();
    const slot = Math.max(now, nextSlot);
    nextSlot = slot + interval;
    if (slot > now) await sleep(slot - now, handlerContext()?.signal);
    return next(msg);
  };
}
