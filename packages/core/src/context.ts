import { AsyncLocalStorage } from 'node:async_hooks';
import type { Executor } from './executor.js';
import type { Message } from './message.js';

/**
 * Per-message, in-process context (Go: `msg.Context()` + router ctx values).
 * Metadata travels across processes; this only lives inside the current process.
 */
export interface HandlerContext {
  handlerName: string;
  subscribeTopic: string;
  publishTopic?: string;
  message: Message;
  /** Fires on router close or a Timeout middleware deadline. Handlers should honour it. */
  signal: AbortSignal;
  /** Transaction the message was delivered in (SQL subscribers), or opened by a middleware. */
  tx?: Executor;
}

const store = new AsyncLocalStorage<Partial<HandlerContext>>();

export function handlerContext(): Partial<HandlerContext> | undefined {
  return store.getStore();
}

export function runWithContext<T>(ctx: Partial<HandlerContext>, fn: () => T): T {
  return store.run(ctx, fn);
}

/** Run `fn` with the current context extended by `patch`. */
export function withContext<T>(patch: Partial<HandlerContext>, fn: () => T): T {
  return store.run({ ...store.getStore(), ...patch }, fn);
}

/** Go: `sql.TxFromContext(msg.Context())`. */
export function txFromContext(): Executor | undefined {
  return store.getStore()?.tx;
}

export function signalFromContext(): AbortSignal | undefined {
  return store.getStore()?.signal;
}

/**
 * An Executor that uses the transaction in the current context if present, else `fallback`.
 * Lets a publisher created once at startup still write inside the subscriber's / business tx:
 * `new PgPublisher(contextExecutor(pool))`.
 */
export function contextExecutor(fallback: Executor): Executor {
  return {
    query: (text, params) => (txFromContext() ?? fallback).query(text, params),
  };
}
