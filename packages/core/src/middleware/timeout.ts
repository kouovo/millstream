import { handlerContext, withContext } from '../context.js';
import type { Middleware } from '../router.js';

export class HandlerTimeoutError extends Error {
  constructor(ms: number) {
    super(`handler timed out after ${ms}ms`);
    this.name = 'HandlerTimeoutError';
  }
}

/**
 * Gives the handler a deadline via `signalFromContext()`. JS cannot kill a Promise,
 * so this is cooperative: the handler must watch the signal. The middleware itself
 * stops waiting and fails the message when the deadline passes.
 */
export function timeout(ms: number): Middleware {
  return (next) => async (msg) => {
    const parent = handlerContext()?.signal;
    const deadline = AbortSignal.timeout(ms);
    const signal = parent ? AbortSignal.any([parent, deadline]) : deadline;
    let onAbort!: () => void;
    const timedOut = new Promise<never>((_, reject) => {
      onAbort = () => reject(new HandlerTimeoutError(ms));
      deadline.addEventListener('abort', onAbort, { once: true });
    });
    try {
      return await Promise.race([withContext({ signal }, () => next(msg)), timedOut]);
    } finally {
      deadline.removeEventListener('abort', onAbort);
    }
  };
}
