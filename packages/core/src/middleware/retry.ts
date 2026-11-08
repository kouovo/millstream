import { handlerContext, txFromContext } from '../context.js';
import type { Executor } from '../executor.js';
import type { Middleware } from '../router.js';
import { sleep } from '../util.js';

export class PermanentError extends Error {
  constructor(override readonly cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause));
    this.name = 'PermanentError';
  }
}

export interface RetryOptions {
  maxRetries: number;
  initialIntervalMs?: number;
  maxIntervalMs?: number;
  multiplier?: number;
  /** 0..1, e.g. 0.5 → interval * [0.5, 1.5]. */
  randomizationFactor?: number;
  /** Give up early; `PermanentError` is never retried. */
  shouldRetry?: (err: unknown, attempt: number) => boolean;
  onRetry?: (err: unknown, attempt: number, delayMs: number) => void;
}

/**
 * In-process retry with exponential backoff. The message stays "held" while retrying
 * (an ordered subscription stalls; a SQL subscriber keeps its transaction open).
 * When a transaction is present each attempt runs inside a SAVEPOINT, so a failed
 * attempt's writes are rolled back before the next one.
 */
export function retry(opts: RetryOptions): Middleware {
  const initial = opts.initialIntervalMs ?? 100;
  const max = opts.maxIntervalMs ?? 10_000;
  const mult = opts.multiplier ?? 2;
  const rand = opts.randomizationFactor ?? 0;
  return (next) => async (msg) => {
    let interval = initial;
    for (let attempt = 0; ; attempt++) {
      try {
        const tx = txFromContext();
        return tx ? await withSavepoint(tx, async () => next(msg)) : await next(msg);
      } catch (e) {
        if (e instanceof PermanentError) throw e.cause;
        if (attempt >= opts.maxRetries || (opts.shouldRetry && !opts.shouldRetry(e, attempt))) throw e;
        const delay = Math.min(max, interval * (1 + rand * (Math.random() * 2 - 1)));
        opts.onRetry?.(e, attempt + 1, delay);
        await sleep(delay, handlerContext()?.signal);
        interval = Math.min(max, interval * mult);
      }
    }
  };
}

let savepointSeq = 0;
/** Run `fn` inside a SAVEPOINT on `tx`; roll back to it if `fn` throws. */
export async function withSavepoint<T>(tx: Executor, fn: () => Promise<T>): Promise<T> {
  const name = `wm_sp_${++savepointSeq}`;
  await tx.query(`SAVEPOINT ${name}`);
  try {
    const r = await fn();
    await tx.query(`RELEASE SAVEPOINT ${name}`);
    return r;
  } catch (e) {
    await tx.query(`ROLLBACK TO SAVEPOINT ${name}`);
    throw e;
  }
}
