import { handlerContext } from './context.js';
import type { Message } from './message.js';
import { type Features, type Publisher, type Subscriber, PubSubClosedError } from './pubsub.js';
import { AsyncQueue, anySignal, isAbortError, producerSubscription, raceAbort, sleep } from './util.js';

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
 * Unlike GoChannel's default, delivery is ordered per subscription: the next message
 * is not handed out until the current one is acked; a nack redelivers a fresh copy.
 * Retry limits/backoff are a Router middleware concern, not the transport's.
 */
export class MemoryPubSub implements Publisher, Subscriber {
  static readonly features: Features = {
    ordered: true,
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

    return producerSubscription(
      async (deliver, loopSignal) => {
        while (!loopSignal.aborted) {
          const delivery = await queue.shift(loopSignal);
          let msg = delivery.msg;
          for (;;) {
            this.#delivered.add(msg);
            const r = await deliver(msg);
            if (r === 'ack') {
              delivery.done();
              break;
            }
            if (r !== 'nack') {
              delivery.done(); // subscription stopping
              return;
            }
            if (this.#opts.nackResendMs) await sleep(this.#opts.nackResendMs, loopSignal);
            msg = msg.copy();
          }
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
