import type { Message } from './message.js';

export interface Publisher {
  publish(topic: string, ...messages: Message[]): Promise<void>;
  close(): Promise<void>;
}

/**
 * `subscribe` registers the subscription synchronously and returns an AsyncIterable.
 *
 * Contract (Watermill `TestNoAck`): implementations declaring `ordered` must not
 * yield the next message before the current one is acked. Since an async generator
 * only resumes when the consumer calls `next()`, this also gives natural backpressure.
 * A nacked message must be redelivered (as a fresh copy).
 */
export interface Subscriber {
  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message>;
  close(): Promise<void>;
}

/** Capabilities a Pub/Sub implementation declares; drives the contract test-suite. */
export interface Features {
  /** Messages are delivered in publish order (per subscription). */
  ordered: boolean;
  /** Messages survive subscriber restarts. */
  persistent: boolean;
  /** Subscribers with different consumer groups each get all messages; same group shares progress. */
  consumerGroups: boolean;
  /** Subscriber created after publish still receives older messages. */
  newSubscriberReceivesOldMessages: boolean;
  /** Handler writes via `msg.tx` commit atomically with the ack. */
  exactlyOnceWithTx: boolean;
}

export class PubSubClosedError extends Error {
  constructor(what = 'pub/sub') {
    super(`${what} is closed`);
    this.name = 'PubSubClosedError';
  }
}
