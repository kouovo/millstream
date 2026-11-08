import { type Message, newMessage } from '@millstream/core';

/** `n` messages with payloads `${prefix}-0 … ${prefix}-(n-1)` and metadata `{ i }`. */
export const msgs = (n: number, prefix = 'm'): Message[] =>
  Array.from({ length: n }, (_, i) => newMessage(`${prefix}-${i}`, { i: String(i) }));

export const payloads = (ms: Message[]): string[] => ms.map((m) => m.payload.toString());

export function nextWithTimeout<T>(it: AsyncIterator<T>, ms: number): Promise<IteratorResult<T> | 'timeout'> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    it.next().finally(() => clearTimeout(timer)),
    new Promise<'timeout'>((resolve) => (timer = setTimeout(() => resolve('timeout'), Math.max(ms, 0)))),
  ]);
}

/** Pull `n` messages, acking each one (or handing it to `onMessage`). */
export async function collect(
  source: AsyncIterable<Message> | AsyncIterator<Message>,
  n: number,
  opts: { timeoutMs?: number; onMessage?: (m: Message, i: number) => void | Promise<void> } = {},
): Promise<Message[]> {
  const it = Symbol.asyncIterator in source ? source[Symbol.asyncIterator]() : source;
  const out: Message[] = [];
  const deadline = Date.now() + (opts.timeoutMs ?? 10_000);
  while (out.length < n) {
    const r = await nextWithTimeout(it, deadline - Date.now());
    if (r === 'timeout') throw new Error(`timeout: got ${out.length}/${n} messages`);
    if (r.done) break;
    out.push(r.value);
    if (opts.onMessage) await opts.onMessage(r.value, out.length - 1);
    else r.value.ack();
  }
  return out;
}

export async function eventually(fn: () => boolean | Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await fn())) {
    if (Date.now() > deadline) throw new Error('condition not met in time');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

export const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
