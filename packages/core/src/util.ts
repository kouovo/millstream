import type { AckResult, Message } from './message.js';

export class AbortedError extends Error {
  constructor() {
    super('aborted');
    this.name = 'AbortedError';
  }
}

export function isAbortError(e: unknown): boolean {
  return e instanceof AbortedError || (e instanceof Error && e.name === 'AbortError');
}

/** Sleep that rejects with AbortedError when the signal fires. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new AbortedError());
    const onAbort = () => {
      clearTimeout(timer);
      reject(new AbortedError());
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/** Resolve `p`, or reject with AbortedError when the signal fires first. */
export function raceAbort<T>(p: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return p;
  if (signal.aborted) return Promise.reject(new AbortedError());
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new AbortedError());
    signal.addEventListener('abort', onAbort, { once: true });
    p.then(
      (v) => {
        signal.removeEventListener('abort', onAbort);
        resolve(v);
      },
      (e) => {
        signal.removeEventListener('abort', onAbort);
        reject(e);
      },
    );
  });
}

export type SettleOutcome = AckResult | 'timeout' | 'aborted';

/** Wait for a message's Ack/Nack, bounded by an optional deadline and abort signal. */
export function waitSettled(msg: Message, deadlineMs: number | null, signal?: AbortSignal): Promise<SettleOutcome> {
  return new Promise((resolve) => {
    let timer: NodeJS.Timeout | undefined;
    const finish = (r: SettleOutcome) => {
      if (timer) clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      resolve(r);
    };
    const onAbort = () => finish('aborted');
    if (signal?.aborted) return finish('aborted');
    signal?.addEventListener('abort', onAbort, { once: true });
    if (deadlineMs != null && deadlineMs > 0) timer = setTimeout(() => finish('timeout'), deadlineMs);
    msg.settled.then(finish);
  });
}

/** Counting semaphore — the `p-limit` style concurrency cap used by the Router. */
export class Semaphore {
  #available: number;
  #waiters: Array<() => void> = [];

  constructor(n: number) {
    if (!(n >= 1)) throw new Error('semaphore size must be >= 1');
    this.#available = n;
  }

  acquire(): Promise<void> {
    if (this.#available > 0) {
      this.#available--;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.#waiters.push(resolve));
  }

  release(): void {
    const next = this.#waiters.shift();
    if (next) next();
    else this.#available++;
  }
}

/** Minimal unbounded async queue with abort-aware `shift`. */
export class AsyncQueue<T> {
  #items: T[] = [];
  #waiters: Array<(v: T) => void> = [];

  push(item: T): void {
    const w = this.#waiters.shift();
    if (w) w(item);
    else this.#items.push(item);
  }

  get size(): number {
    return this.#items.length;
  }

  /** Remove and return everything buffered. */
  drain(): T[] {
    return this.#items.splice(0);
  }

  shift(signal?: AbortSignal): Promise<T> {
    if (this.#items.length) return Promise.resolve(this.#items.shift() as T);
    if (signal?.aborted) return Promise.reject(new AbortedError());
    return new Promise<T>((resolve, reject) => {
      const waiter = (v: T) => {
        signal?.removeEventListener('abort', onAbort);
        resolve(v);
      };
      const onAbort = () => {
        const i = this.#waiters.indexOf(waiter);
        if (i >= 0) this.#waiters.splice(i, 1);
        reject(new AbortedError());
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      this.#waiters.push(waiter);
    });
  }
}

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void;
  info(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

export const nopLogger: Logger = { debug() {}, info() {}, error() {} };

export const consoleLogger: Logger = {
  debug: (m, f) => console.debug(m, f ?? ''),
  info: (m, f) => console.info(m, f ?? ''),
  error: (m, f) => console.error(m, f ?? ''),
};

export type Deliver = (msg: Message, ackDeadlineMs?: number | null) => Promise<SettleOutcome>;

/**
 * Build a Subscriber's AsyncIterable from a *producer loop*.
 *
 * `run(deliver, signal)` hands each message over with `await deliver(msg)` and gets the
 * Ack/Nack (or timeout/abort) back. The loop runs on its own once iteration starts, so:
 * - an Ack is acted on immediately (e.g. a SQL commit), not on the consumer's next `next()`;
 * - aborting the signal or `return()`-ing the iterator stops the loop right away, even
 *   while the consumer holds a message (no transaction left hanging at a `yield`).
 * Ordering / backpressure come from the loop awaiting `deliver` before producing the next.
 * Messages that time out before being pulled are withdrawn from the hand-off buffer.
 */
export function producerSubscription(
  run: (deliver: Deliver, signal: AbortSignal) => Promise<void>,
  signal?: AbortSignal,
  onEnd?: () => void,
): AsyncIterable<Message> {
  return {
    [Symbol.asyncIterator]: () => producerGenerator(run, signal, onEnd),
  };
}

async function* producerGenerator(
  run: (deliver: Deliver, signal: AbortSignal) => Promise<void>,
  signal: AbortSignal | undefined,
  onEnd: (() => void) | undefined,
): AsyncGenerator<Message> {
  const controller = new AbortController();
  const stop = anySignal(signal, controller.signal);
  const buffer: Message[] = [];
  let wake: (() => void) | undefined;
  let ended = false;
  let failure: { error: unknown } | undefined;
  const notify = () => {
    const w = wake;
    wake = undefined;
    w?.();
  };

  const deliver: Deliver = async (msg, ackDeadlineMs = null) => {
    if (stop.aborted) return 'aborted';
    buffer.push(msg);
    notify();
    const r = await waitSettled(msg, ackDeadlineMs, stop);
    if (r === 'timeout' || r === 'aborted') {
      const i = buffer.indexOf(msg);
      if (i >= 0) buffer.splice(i, 1);
    }
    return r;
  };

  const loop = run(deliver, stop)
    .catch((error) => {
      if (!isAbortError(error) && !stop.aborted) failure = { error };
    })
    .finally(() => {
      ended = true;
      notify();
    });

  stop.addEventListener('abort', notify, { once: true });
  try {
    for (;;) {
      const next = buffer.shift();
      if (next) {
        yield next;
        continue;
      }
      if (failure) throw failure.error;
      if (ended || stop.aborted) return;
      await new Promise<void>((resolve) => (wake = resolve));
    }
  } finally {
    stop.removeEventListener('abort', notify);
    controller.abort();
    await loop;
    onEnd?.();
  }
}

/** Signal that fires when any input fires (filters out undefined). */
export function anySignal(...signals: Array<AbortSignal | undefined>): AbortSignal {
  return AbortSignal.any(signals.filter((s): s is AbortSignal => !!s));
}
