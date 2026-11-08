import type { Message } from '../message.js';
import type { Publisher, Subscriber } from '../pubsub.js';
import type { PublisherDecorator, SubscriberDecorator } from './types.js';

export type MessageTransform = (msg: Message, topic: string) => void;

/**
 * Wrap a subscriber so every received message goes through `transform` (in place) before
 * the consumer sees it. Subscription stays synchronous and ack identity is preserved: the
 * consumer gets the very same Message object the inner subscriber yielded.
 */
export function transformSubscriber(inner: Subscriber, transform: MessageTransform): Subscriber {
  return {
    subscribe(topic, signal) {
      const iterable = inner.subscribe(topic, signal); // keep registration synchronous
      return {
        [Symbol.asyncIterator]() {
          const it = iterable[Symbol.asyncIterator]();
          return {
            async next() {
              const r = await it.next();
              if (!r.done) transform(r.value, topic);
              return r;
            },
            return: async (value?: unknown) => (await it.return?.(value)) ?? { done: true as const, value: undefined },
          };
        },
      };
    },
    close: () => inner.close(),
  };
}

/** Wrap a publisher so every message goes through `transform` (in place) before publishing. */
export function transformPublisher(inner: Publisher, transform: MessageTransform): Publisher {
  return {
    publish(topic, ...messages) {
      for (const m of messages) transform(m, topic);
      return inner.publish(topic, ...messages);
    },
    close: () => inner.close(),
  };
}

/** Watermill: `message.MessageTransformSubscriberDecorator`. */
export function messageTransformSubscriberDecorator(transform: MessageTransform): SubscriberDecorator {
  return (subscriber) => transformSubscriber(subscriber, transform);
}

/** Watermill: `message.MessageTransformPublisherDecorator`. */
export function messageTransformPublisherDecorator(transform: MessageTransform): PublisherDecorator {
  return (publisher) => transformPublisher(publisher, transform);
}
