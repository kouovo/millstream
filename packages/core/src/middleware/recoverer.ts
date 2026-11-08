import type { Middleware } from '../router.js';

export class PanicError extends Error {
  constructor(override readonly cause: unknown) {
    super(`handler panicked: ${cause instanceof Error ? cause.message : String(cause)}`);
    this.name = 'PanicError';
  }
}

/**
 * Turns anything thrown (including non-Error values and sync throws) into an Error,
 * so outer Retry / PoisonQueue middlewares can see it. The Router nacks anyway.
 */
export const recoverer: Middleware = (next) => async (msg) => {
  try {
    return await next(msg);
  } catch (e) {
    throw e instanceof Error ? e : new PanicError(e);
  }
};
