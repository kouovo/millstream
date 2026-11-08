import type { Message } from './message.js';
import { DELAYED_FOR, DELAYED_UNTIL, POISON_TOPIC, REQUEUER_RETRIES } from './middleware/index.js';
import type { Publisher, Subscriber } from './pubsub.js';
import type { Handler, Router } from './router.js';

export interface RequeuerOptions {
  /** Subscriber on the (delayed) requeue topic — it must only yield messages that are due. */
  subscriber: Subscriber;
  requeueTopic: string;
  /** Publisher for the original topics. */
  publisher: Publisher;
  /** Default: the `topic_poisoned` metadata written by PoisonQueue. */
  generatePublishTopic?: (msg: Message) => string;
  /** After this many requeues the message goes to `deadLetter` instead. */
  maxRetries?: number;
  deadLetter?: { publisher: Publisher; topic: string };
  handlerName?: string;
}

/**
 * Moves due messages from the requeue topic back to their original topic,
 * incrementing `_millstream_requeuer_retries`. A plain Router handler — no new mechanism.
 */
export function addRequeuer(router: Router, opts: RequeuerOptions): Handler {
  const topicOf = opts.generatePublishTopic ?? ((m: Message) => m.metadata[POISON_TOPIC] ?? '');
  return router.addConsumerHandler({
    name: opts.handlerName ?? 'requeuer',
    subscribeTopic: opts.requeueTopic,
    subscriber: opts.subscriber,
    handler: async (msg) => {
      const retries = Number(msg.metadata[REQUEUER_RETRIES] ?? 0) + 1;
      const out = msg.copy();
      out.metadata[REQUEUER_RETRIES] = String(retries);
      delete out.metadata[DELAYED_UNTIL];
      delete out.metadata[DELAYED_FOR];
      if (opts.maxRetries != null && retries > opts.maxRetries) {
        if (!opts.deadLetter) throw new Error(`message ${msg.uuid} exceeded ${opts.maxRetries} requeues`);
        await opts.deadLetter.publisher.publish(opts.deadLetter.topic, out);
        return;
      }
      const topic = topicOf(msg);
      if (!topic) throw new Error(`message ${msg.uuid} has no topic to requeue to`);
      await opts.publisher.publish(topic, out);
    },
  });
}
