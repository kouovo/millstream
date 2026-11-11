import { handlerContext } from './context.js';
import type { Message } from './message.js';
import { type Features, type Publisher, type Subscriber, PubSubClosedError } from './pubsub.js';
import {
  AsyncQueue,
  type Deliver,
  Semaphore,
  anySignal,
  isAbortError,
  producerSubscription,
  raceAbort,
  sleep,
} from './util.js';

export interface MemoryPubSubOptions {
  /** Keep every published message and replay it to subscribers that join later. */
  persistent?: boolean;
  /**
   * `publish` resolves only after every current subscriber acked (sync event bus mode).
   * Publishing from inside one of this pub/sub's handlers doesn't wait (it would wait for itself).
   */
  blockPublishUntilSubscriberAck?: boolean;
  /** Delay before redelivering a nacked message. Default 0 (Watermill GoChannel: immediate). */
  nackResendMs?: number;
  /**
   * Max unacked messages in flight per subscription.
   * - `Infinity` (default): Watermill GoChannel behaviour, every message is handed out right away and
   *   waits for its own ack/nack; the consumer (e.g. the Router handler's `concurrency`) is the only limit.
   * - `1`: ordered, the next message waits for the current ack.
   *   Pair it with the Router handler's default `concurrency: 1`.
   * - `N`: at most N unacked at once.
   *
   * Above 1 delivery is unordered: a nacked message is redelivered while the others keep flowing.
   */
  concurrency?: number;
}

interface Delivery {
  msg: Message;
  /** Releases a `blockPublishUntilSubscriberAck` publisher: acked, or the subscription ended. */
  done: () => void;
}

/**
 * In-process Pub/Sub — the counterpart of Watermill's GoChannel.
 *
 * Every subscriber gets its own copy of each message (ack state cannot be shared).
 * Like GoChannel, delivery is parallel and unordered by default: every message is handed
 * out right away and a nack redelivers a fresh copy without holding back the others.
 * Set `concurrency: 1` for ordered delivery (the next message waits for the current ack).
 * Retry limits/backoff are a Router middleware concern, not the transport's.
 */
export class MemoryPubSub implements Publisher, Subscriber {
  static readonly features: Features = {
    ordered: false, // default `concurrency: Infinity`; `concurrency: 1` is ordered
    persistent: false,
    consumerGroups: false,
    newSubscriberReceivesOldMessages: false,
    exactlyOnceWithTx: false,
  };

  readonly #opts: MemoryPubSubOptions;
  readonly #subs = new Map<string, Set<AsyncQueue<Delivery>>>();
  readonly #history = new Map<string, Message[]>();
  readonly #closing = new AbortController();
  /** Messages handed out by this pub/sub — to detect publishing from inside our own handlers. */
  readonly #delivered = new WeakSet<Message>();

  constructor(opts: MemoryPubSubOptions = {}) {
    const c = opts.concurrency;
    if (c !== undefined && !(c === Infinity || (Number.isInteger(c) && c >= 1))) {
      throw new Error('MemoryPubSub concurrency must be an integer >= 1 or Infinity');
    }
    this.#opts = opts;
  }

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('MemoryPubSub');
    const acked: Promise<void>[] = [];
    for (const msg of messages) {
      if (this.#opts.persistent) {
        let h = this.#history.get(topic);
        if (!h) this.#history.set(topic, (h = []));
        h.push(msg.copy());
      }
      for (const queue of this.#subs.get(topic) ?? []) {
        acked.push(new Promise<void>((done) => queue.push({ msg: msg.copy(), done })));
      }
    }
    const current = handlerContext()?.message;
    const reentrant = current !== undefined && this.#delivered.has(current);
    if (this.#opts.blockPublishUntilSubscriberAck && !reentrant) {
      await raceAbort(Promise.all(acked), this.#closing.signal).catch((error) => {
        if (!isAbortError(error)) throw error;
      });
    }
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('MemoryPubSub');
    const queue = new AsyncQueue<Delivery>();
    // Registered synchronously, so nothing published after `subscribe()` returns is missed.
    let set = this.#subs.get(topic);
    if (!set) this.#subs.set(topic, (set = new Set()));
    set.add(queue);
    for (const m of this.#opts.persistent ? (this.#history.get(topic) ?? []) : []) {
      queue.push({ msg: m.copy(), done: () => {} });
    }
    const stop = anySignal(signal, this.#closing.signal);
    const unregister = () => {
      set.delete(queue);
      for (const d of queue.drain()) d.done(); // don't leave blocked publishers waiting forever
    };
    stop.addEventListener('abort', unregister, { once: true });

    // One message until acked (GoChannel's per-message goroutine): nack → resend a fresh copy.
    const send = async (delivery: Delivery, deliver: Deliver, loopSignal: AbortSignal) => {
      let msg = delivery.msg;
      try {
        for (;;) {
          this.#delivered.add(msg);
          const r = await deliver(msg);
          if (r !== 'nack') return; // acked, or subscription stopping
          if (this.#opts.nackResendMs) await sleep(this.#opts.nackResendMs, loopSignal);
          msg = msg.copy();
        }
      } catch (error) {
        if (!isAbortError(error)) throw error;
      } finally {
        delivery.done();
      }
    };

    return producerSubscription(
      async (deliver, loopSignal) => {
        // Up to `concurrency` messages in flight: Infinity (default) = no limit (GoChannel), 1 = ordered.
        const slots = new Semaphore(this.#opts.concurrency ?? Infinity);
        const inflight = new Set<Promise<void>>();
        try {
          while (!loopSignal.aborted) {
            await raceAbort(slots.acquire(), loopSignal);
            const delivery = await queue.shift(loopSignal).catch((error: unknown) => {
              slots.release();
              throw error;
            });
            const p = send(delivery, deliver, loopSignal).finally(() => {
              slots.release();
              inflight.delete(p);
            });
            inflight.add(p);
          }
        } finally {
          await Promise.allSettled(inflight); // each resolves as soon as `deliver` sees the abort
        }
      },
      stop,
      unregister,
    );
  }

  async close(): Promise<void> {
    this.#closing.abort();
    this.#subs.clear();
  }
}
