import type { Publisher, Subscriber } from '../pubsub.js';

/** Which Router handler a decorator is being applied for. */
export interface DecoratorInfo {
  handlerName: string;
  subscribeTopic: string;
  publishTopic?: string;
}

/**
 * Wraps a handler's publisher (Watermill: `message.PublisherDecorator`). Applied by
 * `router.addPublisherDecorators()` when each handler starts. `info` is an addition over
 * Watermill: label metrics / spans by handler without digging into message context.
 */
export type PublisherDecorator = (publisher: Publisher, info: DecoratorInfo) => Publisher;

/** Wraps a handler's subscriber (Watermill: `message.SubscriberDecorator`). */
export type SubscriberDecorator = (subscriber: Subscriber, info: DecoratorInfo) => Subscriber;
