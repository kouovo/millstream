import {
  DELAYED_UNTIL,
  type Features,
  type Logger,
  Message,
  type Publisher,
  PubSubClosedError,
  type SettleOutcome,
  type Subscriber,
  anySignal,
  nopLogger,
  producerSubscription,
} from '@millstream/core';
import {
  type ConnectionOptions,
  type Job,
  type JobsOptions,
  Queue,
  type QueueOptions,
  Worker,
  type WorkerOptions,
} from 'bullmq';
import { randomUUID } from 'node:crypto';

type Deliver = (msg: Message, ackDeadlineMs?: number | null) => Promise<SettleOutcome>;

// ---------------------------------------------------------------- marshaling

/** What a message becomes in BullMQ, and back (Watermill's Marshaler, BullMQ flavour). */
export interface BullMQMarshaler {
  marshal(topic: string, msg: Message): { name: string; data: unknown; opts?: JobsOptions };
  unmarshal(job: Job): Message;
}

interface Envelope {
  uuid: string;
  /** base64 */
  payload: string;
  metadata: Record<string, string>;
}

/**
 * BullMQ custom job ids must not be integers nor contain `:`; anything else is used as is.
 * Used as the default jobId, so re-publishing the same message is ignored by BullMQ
 * while the previous job is still retained (dedup for at-least-once producers like the forwarder).
 */
export function toJobId(uuid: string): string {
  const id = uuid.replaceAll(':', '_');
  return /^\d+$/.test(id) ? `id-${id}` : id;
}

/** Default marshaler: job data is `{ uuid, payload (base64), metadata }`, job name = topic. */
export const defaultMarshaler: BullMQMarshaler = {
  marshal(topic, msg) {
    const data: Envelope = { uuid: msg.uuid, payload: msg.payload.toString('base64'), metadata: msg.metadata };
    const until = msg.metadata[DELAYED_UNTIL];
    const delay = until ? Math.max(0, Date.parse(until) - Date.now()) : 0;
    return { name: topic, data, opts: { jobId: toJobId(msg.uuid), ...(delay > 0 ? { delay } : {}) } };
  },
  unmarshal(job) {
    const data = job.data as Partial<Envelope> | null;
    if (!data || typeof data.uuid !== 'string' || typeof data.payload !== 'string') {
      throw new Error(`job ${job.id} is not a millstream envelope`);
    }
    return new Message(data.uuid, Buffer.from(data.payload, 'base64'), data.metadata ?? {});
  },
};

const jobs = new WeakMap<Message, Job>();

/** The BullMQ job behind a consumed message (in-process only). */
export function bullmqJob(msg: Message): Job | undefined {
  return jobs.get(msg);
}

// ---------------------------------------------------------------- consumer groups

export class InvalidConsumerGroupError extends Error {
  constructor(group: string) {
    super(`invalid consumer group "${group}": must be non-empty and must not contain ":"`);
    this.name = 'InvalidConsumerGroupError';
  }
}

/**
 * Queue of one consumer group: `<queue>.<group>`. A BullMQ queue is shared work, so a group
 * (a broadcast subscription) is emulated with its own queue that the publisher writes to.
 */
export function consumerGroupQueueName(queue: string, group: string): string {
  if (!group || group.includes(':')) throw new InvalidConsumerGroupError(group);
  return `${queue}.${group}`;
}

// ---------------------------------------------------------------- Publisher

export interface BullMQPublisherOptions {
  connection: ConnectionOptions;
  /** Queue for a topic. Default: the topic itself. */
  queueName?: (topic: string) => string;
  /**
   * Fan-out: the consumer groups of a topic. Each message is added once per group, to the
   * group's own queue (`consumerGroupQueueName`); a `BullMQSubscriber` with the same
   * `consumerGroup` reads it. Every group retries / fails independently.
   *
   * - returns `undefined` → the topic is not grouped: one shared queue (default behaviour);
   * - returns `[]` → the topic has no consumers: nothing is added.
   *
   * The list is static configuration (derive it from your handler registry): a group only
   * receives messages published while it is listed, and an unlisted group's queue stops growing.
   */
  consumerGroups?: (topic: string) => readonly string[] | undefined;
  /**
   * Native job options per message (attempts, backoff, priority, removeOnComplete, …),
   * merged over the marshaler's (jobId, delay). Like Watermill's `Generate*` hooks.
   * `consumerGroup` is set when publishing for a group.
   */
  jobOptions?: (topic: string, msg: Message, ctx: { consumerGroup?: string }) => JobsOptions;
  /** Extra queue options (prefix, defaultJobOptions, …). */
  queueOptions?: Omit<QueueOptions, 'connection'>;
  marshaler?: BullMQMarshaler;
  logger?: Logger;
}

/** One BullMQ queue per topic (or per topic and consumer group); `publish` is an `addBulk` per queue. */
export class BullMQPublisher implements Publisher {
  readonly #opts: BullMQPublisherOptions;
  readonly #queues = new Map<string, Queue>();
  readonly #logger: Logger;
  #closed = false;

  constructor(opts: BullMQPublisherOptions) {
    this.#opts = opts;
    this.#logger = opts.logger ?? nopLogger;
  }

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('BullMQPublisher');
    if (!messages.length) return;
    const marshaler = this.#opts.marshaler ?? defaultMarshaler;
    const queue = this.#opts.queueName?.(topic) ?? topic;
    const groups = this.#opts.consumerGroups?.(topic);
    const bulk = (consumerGroup?: string) =>
      messages.map((m) => {
        const job = marshaler.marshal(topic, m);
        const extra = this.#opts.jobOptions?.(topic, m, { consumerGroup });
        return { name: job.name, data: job.data, opts: { ...job.opts, ...extra } };
      });
    if (!groups) {
      await this.#queue(queue).addBulk(bulk());
      return;
    }
    // Not atomic across queues: a failed publish is retried as a whole, and the jobId (uuid)
    // makes the groups that already got the message ignore the repeat.
    await Promise.all(
      [...new Set(groups)].map((group) => this.#queue(consumerGroupQueueName(queue, group)).addBulk(bulk(group))),
    );
  }

  #queue(name: string): Queue {
    let q = this.#queues.get(name);
    if (!q) {
      q = new Queue(name, { ...this.#opts.queueOptions, connection: this.#opts.connection });
      q.on('error', (error) => this.#logger.error('bullmq queue error', { queue: name, error }));
      this.#queues.set(name, q);
    }
    return q;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await Promise.allSettled([...this.#queues.values()].map((q) => q.close()));
  }
}

// ---------------------------------------------------------------- Subscriber

export class NackedError extends Error {
  constructor(reason: 'nack' | 'timeout') {
    super(reason === 'nack' ? 'message nacked by its handler' : 'handler did not answer within the ack deadline');
    this.name = 'NackedError';
  }
}

export interface BullMQSubscriberOptions {
  connection: ConnectionOptions;
  /** Queue for a topic. Default: the topic itself. */
  queueName?: (topic: string) => string;
  /**
   * Read the queue of this consumer group (`consumerGroupQueueName`) instead of the topic's
   * shared queue. The publisher must list the group in its `consumerGroups`.
   * Subscribers of the same group compete for its jobs.
   */
  consumerGroup?: string;
  /** Jobs processed in parallel per subscription. Default 1. */
  concurrency?: number;
  /**
   * What a Nack (or ack-deadline timeout) does — pick **one** layer to own retries:
   * - `'redeliver'` (default, Watermill semantics): back to the queue after `nackResendMs`
   *   without consuming an attempt; retry policy lives in Router middlewares.
   * - `'fail'`: `moveToFailed` — BullMQ's `attempts` / `backoff` (set them via the publisher's
   *   `jobOptions`) apply, and exhausted jobs stay in the failed set for dashboards / `job.retry()`.
   *   Like Watermill AMQP's `NoRequeueOnNack` + a dead-letter exchange.
   */
  nack?: 'redeliver' | 'fail';
  /** Delay before a nacked job is available again (`nack: 'redeliver'`). Default 100ms. */
  nackResendMs?: number;
  /** No answer within this time → treated as a nack. Default: wait forever (the job lock is extended). */
  ackDeadlineMs?: number | null;
  /** Extra worker options (lockDuration, drainDelay, prefix, limiter, …). */
  workerOptions?: Omit<WorkerOptions, 'connection' | 'autorun' | 'concurrency'>;
  marshaler?: BullMQMarshaler;
  logger?: Logger;
}

/**
 * Pulls jobs with BullMQ's manual processing API (`getNextJob` / `moveTo*`), extending
 * the job lock while the handler runs. Ack → completed. Abort while holding a job → back
 * to wait immediately (not counted as an attempt). Jobs that can't be unmarshaled → failed.
 */
export class BullMQSubscriber implements Subscriber {
  static readonly features: Features = {
    ordered: false, // competing workers + delayed redelivery reorder jobs
    persistent: true,
    consumerGroups: false, // a queue is shared work, not a broadcast
    newSubscriberReceivesOldMessages: true,
    exactlyOnceWithTx: false,
  };

  readonly #opts: BullMQSubscriberOptions;
  readonly #logger: Logger;
  readonly #closing = new AbortController();

  constructor(opts: BullMQSubscriberOptions) {
    this.#opts = opts;
    if (opts.consumerGroup !== undefined) consumerGroupQueueName('', opts.consumerGroup); // validate early
    this.#logger = opts.logger ?? nopLogger;
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('BullMQSubscriber');
    return producerSubscription(
      (deliver, stop) => this.#run(topic, deliver, stop),
      anySignal(signal, this.#closing.signal),
    );
  }

  async close(): Promise<void> {
    this.#closing.abort();
  }

  async #run(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const queue = this.#opts.queueName?.(topic) ?? topic;
    const group = this.#opts.consumerGroup;
    const name = group === undefined ? queue : consumerGroupQueueName(queue, group);
    const worker = new Worker(name, null, { ...this.#opts.workerOptions, connection: this.#opts.connection });
    worker.on('error', (error) => this.#logger.error('bullmq worker error', { queue: name, error }));
    void worker.startStalledCheckTimer(); // jobs of crashed consumers go back to wait after lockDuration

    const inflight = new Set<Promise<void>>();
    // Let in-flight jobs be released (they need the connection) before closing the worker,
    // which also interrupts the blocking `getNextJob` calls.
    const onAbort = () => void Promise.allSettled(inflight).then(() => worker.close());
    stop.addEventListener('abort', onAbort, { once: true });
    try {
      const slots = Array.from({ length: Math.max(1, this.#opts.concurrency ?? 1) }, async () => {
        while (!stop.aborted) {
          const token = randomUUID();
          let job: Job | undefined;
          try {
            job = await worker.getNextJob(token, { block: true });
          } catch (error) {
            if (stop.aborted) return;
            this.#logger.error('bullmq getNextJob failed', { queue: name, error });
            await new Promise((r) => setTimeout(r, 1000));
            continue;
          }
          if (!job) continue; // drainDelay elapsed, or closing
          const handling = this.#handle(worker, job, token, deliver, stop);
          inflight.add(handling);
          await handling.finally(() => inflight.delete(handling));
        }
      });
      await Promise.all(slots);
    } finally {
      stop.removeEventListener('abort', onAbort);
      await Promise.allSettled(inflight);
      await worker.close().catch(() => {});
    }
  }

  async #handle(worker: Worker, job: Job, token: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const lockMs = worker.opts.lockDuration ?? 30_000;
    const keepAlive = setInterval(
      () =>
        void job
          .extendLock(token, lockMs)
          .catch((error) => this.#logger.error('bullmq extendLock failed', { id: job.id, error })),
      Math.max(100, lockMs / 2),
    );
    try {
      let msg: Message;
      try {
        msg = (this.#opts.marshaler ?? defaultMarshaler).unmarshal(job);
      } catch (error) {
        // A poison job: never deliverable, so fail it (visible in the failed set).
        await job.moveToFailed(error instanceof Error ? error : new Error(String(error)), token, false);
        return;
      }
      jobs.set(msg, job);
      if (stop.aborted) {
        await job.moveToWait(token);
        return;
      }
      const r = await deliver(msg, this.#opts.ackDeadlineMs ?? null);
      if (r === 'ack') {
        await job.moveToCompleted(undefined, token, false);
      } else if (r === 'aborted') {
        await job.moveToWait(token); // not processed; not an attempt
      } else if (this.#opts.nack === 'fail') {
        await job.moveToFailed(new NackedError(r), token, false); // BullMQ attempts/backoff decide
      } else {
        const resend = this.#opts.nackResendMs ?? 100;
        if (resend > 0) await job.moveToDelayed(Date.now() + resend, token);
        else await job.moveToWait(token);
      }
    } catch (error) {
      // Lost the lock (stalled → someone else may own it now) or the connection: the job is
      // retried by BullMQ's stalled-job handling, so only log.
      this.#logger.error('bullmq job state transition failed', { id: job.id, error });
    } finally {
      clearInterval(keepAlive);
    }
  }
}
