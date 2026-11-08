import {
  type Handler,
  type Message,
  type Middleware,
  PermanentError,
  type Router,
  type Subscriber,
} from '@millstream/core';
import {
  type AnyEventDefinition,
  type EventDefinition,
  type EventPayload,
  InvalidEventError,
  parseEvent,
} from './event.js';
import { type EventMarshaler, jsonEventMarshaler } from './marshaler.js';

export type EventHandler<TPayload> = (payload: TPayload, msg: Message) => void | Promise<void>;

export interface EventProcessorOptions {
  router: Router;
  /**
   * Subscriber per handler. For durable transports use the handler name as the consumer
   * group, so every handler gets every event and progresses / fails on its own:
   * `({ handlerName }) => new PgSubscriber(db, { consumerGroup: handlerName })`.
   * In-process: just pass the `MemoryPubSub` itself.
   */
  subscriber: Subscriber | ((p: { handlerName: string; topic: string }) => Subscriber);
  /** Topic of an event. Default: the event name. Must match the EventBus's. */
  topic?: (eventName: string) => string;
  marshaler?: EventMarshaler;
  /** Middlewares for every event handler (inside the router-level ones). */
  middlewares?: Middleware[];
  /**
   * A message on the handler's topic that isn't one of its events (topics shared by several
   * event types): `'ack'` skips it (default), `'nack'` fails it.
   */
  onUnknownEvent?: 'ack' | 'nack';
}

export interface HandlerOptions {
  middlewares?: Middleware[];
  concurrency?: number;
}

export class UnknownEventError extends Error {
  constructor(name: string | undefined, handler: string) {
    super(`handler "${handler}" received an unknown event ${name ? `"${name}"` : '(no name)'}`);
    this.name = 'UnknownEventError';
  }
}

interface Route {
  event: AnyEventDefinition;
  handler: EventHandler<any>;
}

/** Build a route for `EventProcessor.group`. */
export function on<E extends AnyEventDefinition>(event: E, handler: EventHandler<EventPayload<E>>): Route {
  return { event, handler };
}

/**
 * Wires typed event handlers to Router handlers (Watermill's `cqrs.EventProcessor` and
 * `EventGroupProcessor`). Each handler is its own Router handler: own subscription, own
 * middlewares, own failures. Events failing schema validation become `PermanentError`s
 * (no pointless `retry`) — pair handlers with `poisonQueue` or `dropOnError`.
 */
export class EventProcessor {
  readonly #opts: EventProcessorOptions;
  readonly #marshaler: EventMarshaler;

  constructor(opts: EventProcessorOptions) {
    this.#opts = opts;
    this.#marshaler = opts.marshaler ?? jsonEventMarshaler;
  }

  topicOf(eventName: string): string {
    return this.#opts.topic?.(eventName) ?? eventName;
  }

  /** One handler for one event type. `handlerName` must be unique (and stable: it is the consumer group). */
  on<E extends AnyEventDefinition>(
    event: E,
    handlerName: string,
    handler: EventHandler<EventPayload<E>>,
    opts: HandlerOptions = {},
  ): Handler {
    return this.#add(handlerName, [{ event, handler }], opts);
  }

  /**
   * Several event types on **one** subscription, dispatched by name, in order — for events
   * whose relative order matters (Created → Updated of one aggregate). All of them must map
   * to the same topic (e.g. `topic: (name) => name.split('.')[0]`). The price: one failing
   * event blocks the whole group.
   */
  group(groupName: string, routes: Route[], opts: HandlerOptions = {}): Handler {
    if (!routes.length) throw new Error(`event group "${groupName}" has no handlers`);
    return this.#add(groupName, routes, opts);
  }

  #add(name: string, routes: Route[], opts: HandlerOptions): Handler {
    const topics = new Set(routes.map((r) => this.topicOf(r.event.name)));
    if (topics.size !== 1) {
      throw new Error(
        `handler "${name}": events map to different topics (${[...topics].join(', ')}); one subscription needs one topic`,
      );
    }
    const byName = new Map<string, Route>();
    for (const route of routes) {
      if (byName.has(route.event.name))
        throw new Error(`handler "${name}": event "${route.event.name}" registered twice`);
      byName.set(route.event.name, route);
    }
    const topic = [...topics][0]!;
    const subscriber =
      typeof this.#opts.subscriber === 'function'
        ? this.#opts.subscriber({ handlerName: name, topic })
        : this.#opts.subscriber;

    const handle = async (msg: Message): Promise<void> => {
      const eventName = this.#marshaler.nameOf(msg);
      const route = eventName === undefined ? undefined : byName.get(eventName);
      if (!route) {
        if (this.#opts.onUnknownEvent === 'nack') throw new UnknownEventError(eventName, name);
        return; // not ours: ack and move on
      }
      let payload: unknown;
      try {
        payload = await parseEvent(route.event as EventDefinition<unknown>, this.#marshaler.unmarshal(msg));
      } catch (error) {
        // Malformed or invalid: retrying cannot fix it.
        throw new PermanentError(
          error instanceof InvalidEventError
            ? error
            : new InvalidEventError(route.event.name, [{ message: String(error) }]),
        );
      }
      await route.handler(payload, msg);
    };

    return this.#opts.router.addConsumerHandler({
      name,
      subscribeTopic: topic,
      subscriber,
      concurrency: opts.concurrency,
      middlewares: [...(this.#opts.middlewares ?? []), ...(opts.middlewares ?? [])],
      handler: handle,
    });
  }
}
