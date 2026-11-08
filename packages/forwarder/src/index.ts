import {
  type Handler,
  type Logger,
  Message,
  type Middleware,
  newUUID,
  type Publisher,
  type Router,
  type Subscriber,
} from '@millstream/core';

export const DEFAULT_FORWARDER_TOPIC = 'forwarder_topic';

interface Envelope {
  destination_topic: string;
  uuid: string;
  payload: string; // base64
  metadata: Record<string, string>;
}

export function wrapInEnvelope(destinationTopic: string, msg: Message): Message {
  const env: Envelope = {
    destination_topic: destinationTopic,
    uuid: msg.uuid,
    payload: msg.payload.toString('base64'),
    metadata: msg.metadata,
  };
  // The outer message gets a fresh uuid; the original one is restored on unwrap. Its metadata is
  // copied to the outer message too (never read back), so trace context / correlation ids are
  // visible to the relay's middlewares, e.g. its consumer span continues the business trace.
  return new Message(newUUID(), Buffer.from(JSON.stringify(env)), { ...msg.metadata });
}

export class InvalidEnvelopeError extends Error {
  constructor(reason: string) {
    super(`cannot unwrap forwarder envelope: ${reason}`);
    this.name = 'InvalidEnvelopeError';
  }
}

export function unwrapEnvelope(outer: Message): { destinationTopic: string; message: Message } {
  let env: Partial<Envelope>;
  try {
    env = JSON.parse(outer.payload.toString('utf8'));
  } catch (e) {
    throw new InvalidEnvelopeError((e as Error).message);
  }
  if (!env || typeof env.destination_topic !== 'string' || !env.destination_topic) {
    throw new InvalidEnvelopeError('missing destination_topic');
  }
  if (typeof env.uuid !== 'string' || typeof env.payload !== 'string') {
    throw new InvalidEnvelopeError('missing uuid or payload');
  }
  return {
    destinationTopic: env.destination_topic,
    message: new Message(env.uuid, Buffer.from(env.payload, 'base64'), env.metadata ?? {}),
  };
}

/**
 * Business-side decorator: `publish(anyTopic, msg)` → envelope → `inner.publish(forwarderTopic)`.
 * Wrap a transactional SQL publisher to get an outbox: all business topics share one table.
 */
export class ForwarderPublisher implements Publisher {
  constructor(
    private readonly inner: Publisher,
    private readonly forwarderTopic = DEFAULT_FORWARDER_TOPIC,
  ) {}

  publish(topic: string, ...messages: Message[]): Promise<void> {
    return this.inner.publish(this.forwarderTopic, ...messages.map((m) => wrapInEnvelope(topic, m)));
  }

  close(): Promise<void> {
    return this.inner.close();
  }
}

export interface ForwarderOptions {
  /** Subscriber reading the outbox (e.g. a PgSubscriber). */
  subscriber: Subscriber;
  /** The real broker. */
  publisher: Publisher;
  forwarderTopic?: string;
  handlerName?: string;
  /** Log and drop envelopes that can't be parsed instead of blocking the queue. */
  ackWhenCannotUnwrap?: boolean;
  middlewares?: Middleware[];
  logger?: Logger;
}

/**
 * Relay side: a single consumer handler on the router — subscribe the outbox,
 * unwrap, publish to the destination topic. At-least-once: downstream must dedupe by uuid.
 */
export function addForwarder(router: Router, opts: ForwarderOptions): Handler {
  return router.addConsumerHandler({
    name: opts.handlerName ?? 'forwarder',
    subscribeTopic: opts.forwarderTopic ?? DEFAULT_FORWARDER_TOPIC,
    subscriber: opts.subscriber,
    middlewares: opts.middlewares,
    handler: async (outer) => {
      let unwrapped;
      try {
        unwrapped = unwrapEnvelope(outer);
      } catch (e) {
        if (opts.ackWhenCannotUnwrap) {
          (opts.logger ?? router.logger).error('dropping invalid envelope', { uuid: outer.uuid, error: e });
          return;
        }
        throw e;
      }
      await opts.publisher.publish(unwrapped.destinationTopic, unwrapped.message);
    },
  });
}
