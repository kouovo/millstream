import type { Message } from './message.js';
import { AsyncQueue, isAbortError } from './util.js';

/**
 * Turn a push-style SDK (kafkajs `eachMessage`, amqplib `consume`, BullMQ Worker…)
 * into the pull-style `AsyncIterable<Message>` a Subscriber returns.
 *
 * The SDK callback enqueues the message and then *waits until it is settled*,
 * so the SDK does not push the next one (or is throttled by its prefetch) —
 * backpressure flows from the Router back to the broker. Callbacks still waiting
 * when the subscription stops are nacked.
 *
 * ```ts
 * subscribe(topic, signal) {
 *   return fromPush(async (onMessage) => {
 *     await consumer.run({ eachMessage: async ({ message }) => {
 *       const m = toMessage(message);
 *       if ((await onMessage(m)) === 'nack') throw new Error('nack'); // kafkajs retries / seeks
 *     }});
 *     return () => consumer.stop();
 *   }, signal);
 * }
 * ```
 */
export async function* fromPush(
  start: (onMessage: (m: Message) => Promise<'ack' | 'nack'>) => Promise<(() => Promise<void> | void) | void>,
  signal?: AbortSignal,
): AsyncGenerator<Message> {
  const queue = new AsyncQueue<Message>();
  const pending = new Set<Message>();
  const stop = await start(async (m) => {
    if (signal?.aborted) {
      m.nack();
      return 'nack';
    }
    pending.add(m);
    queue.push(m);
    try {
      return await m.settled;
    } finally {
      pending.delete(m);
    }
  });
  try {
    // `signal` is flipped from outside (abort), so check it on every turn.
    for (;;) {
      if (signal?.aborted) return;
      yield await queue.shift(signal);
    }
  } catch (e) {
    if (!isAbortError(e)) throw e;
  } finally {
    for (const m of pending) m.nack();
    await stop?.();
  }
}
