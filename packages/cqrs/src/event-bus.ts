import { CORRELATION_ID, type Message, type Publisher, handlerContext } from '@millstream/core';
import { type AnyEventDefinition, type EventInput, type EventItem, type PublishOptions, parseEvent } from './event.js';
import { type EventMarshaler, jsonEventMarshaler } from './marshaler.js';

export interface EventBusOptions {
  /**
   * Decides the delivery semantics — the bus itself is just marshal + topic + publish:
   * - in-process, best effort: a `MemoryPubSub` (+ `dropOnError` on the processor's handlers);
   * - reliable (outbox): `new ForwarderPublisher(new PgPublisher(contextExecutor(db)))`,
   *   so publishing inside `withTransaction` joins the business transaction.
   */
  publisher: Publisher;
  /** Topic of an event. Default: the event name. Must match the EventProcessor's. */
  topic?: (eventName: string) => string;
  marshaler?: EventMarshaler;
  /** Validate payloads with the event's schema before publishing. Default true. */
  validate?: boolean;
  /**
   * Copy `correlation_id` from the message being handled (when publishing from inside a
   * Router handler) onto the new events. Default true.
   */
  propagateCorrelationId?: boolean;
  /** Last hook before publishing (add metadata, tracing headers, logging). */
  onPublish?: (p: {
    event: AnyEventDefinition;
    payload: unknown;
    message: Message;
    topic: string;
  }) => void | Promise<void>;
}

/**
 * Typed facade over a Publisher (Watermill's `cqrs.EventBus`). It keeps no handler
 * registry — the EventProcessor wires handlers to subscriptions on a Router.
 */
export class EventBus {
  readonly #opts: EventBusOptions;
  readonly #marshaler: EventMarshaler;

  constructor(opts: EventBusOptions) {
    this.#opts = opts;
    this.#marshaler = opts.marshaler ?? jsonEventMarshaler;
  }

  topicOf(eventName: string): string {
    return this.#opts.topic?.(eventName) ?? eventName;
  }

  /** Publish one event; resolves with the published message (its `uuid` identifies the event). */
  async publish<E extends AnyEventDefinition>(
    event: E,
    payload: EventInput<E>,
    opts: PublishOptions = {},
  ): Promise<Message> {
    const [message] = await this.publishMany([{ ...opts, event, payload }]);
    return message!;
  }

  /**
   * Publish several events in order. Consecutive events for the same topic go out in one
   * `publish` call (one INSERT for SQL publishers, one batch for Kafka/BullMQ).
   */
  async publishMany(items: ReadonlyArray<EventItem<any, any>>): Promise<Message[]> {
    const prepared: Array<{ topic: string; message: Message }> = [];
    for (const item of items) {
      const payload = this.#opts.validate === false ? item.payload : await parseEvent(item.event, item.payload);
      const metadata = { ...this.#inheritedMetadata(), ...item.metadata };
      const message = this.#marshaler.marshal(item.event.name, payload, { uuid: item.uuid, metadata });
      const topic = this.topicOf(item.event.name);
      await this.#opts.onPublish?.({ event: item.event, payload, message, topic });
      prepared.push({ topic, message });
    }
    for (let i = 0; i < prepared.length;) {
      const topic = prepared[i]!.topic;
      let j = i;
      while (j < prepared.length && prepared[j]!.topic === topic) j++;
      await this.#opts.publisher.publish(topic, ...prepared.slice(i, j).map((p) => p.message));
      i = j;
    }
    return prepared.map((p) => p.message);
  }

  #inheritedMetadata(): Record<string, string> {
    if (this.#opts.propagateCorrelationId === false) return {};
    const id = handlerContext()?.message?.metadata[CORRELATION_ID];
    return id ? { [CORRELATION_ID]: id } : {};
  }
}
