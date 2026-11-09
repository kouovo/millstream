import {
  AbortedError,
  DELAYED_UNTIL,
  type Deliver,
  type Executor,
  type Features,
  type Logger,
  Message,
  type Publisher,
  PubSubClosedError,
  type Subscriber,
  anySignal,
  nopLogger,
  producerSubscription,
  sleep,
} from '@millstream/core';
import { type Job, Logger as GraphileLogger, type Runner, type RunnerOptions, type Task, run } from 'graphile-worker';

// ---------------------------------------------------------------- marshaling

/** What a message becomes as a Graphile Worker job payload, and back. */
export interface GraphileWorkerMarshaler {
  marshal(topic: string, msg: Message): unknown;
  unmarshal(job: Job): Message;
}

interface Envelope {
  uuid: string;
  /** UTF-8 text, or base64 when `encoding` says so. */
  payload: string;
  encoding?: 'base64';
  metadata: Record<string, string>;
}

function isEnvelope(x: unknown): x is Envelope {
  const e = x as Partial<Envelope> | null;
  return !!e && typeof e === 'object' && typeof e.uuid === 'string' && typeof e.payload === 'string';
}

/**
 * Default marshaler: payload `{ uuid, payload, metadata }` — the payload as text when it is valid
 * UTF-8, else base64 with `encoding: 'base64'`. Jobs added by plain Graphile Worker code (any other
 * JSON payload) are read too: the JSON becomes the message payload and the uuid is `graphile:<job id>`.
 */
export const defaultMarshaler: GraphileWorkerMarshaler = {
  marshal(_topic, msg) {
    const text = msg.payload.toString('utf8');
    const utf8 = Buffer.from(text, 'utf8').equals(msg.payload);
    const envelope: Envelope = utf8
      ? { uuid: msg.uuid, payload: text, metadata: msg.metadata }
      : { uuid: msg.uuid, payload: msg.payload.toString('base64'), encoding: 'base64', metadata: msg.metadata };
    return envelope;
  },
  unmarshal(job) {
    const p = job.payload;
    if (isEnvelope(p)) {
      const payload = p.encoding === 'base64' ? Buffer.from(p.payload, 'base64') : Buffer.from(p.payload, 'utf8');
      return new Message(p.uuid, payload, p.metadata ?? {});
    }
    return new Message(`graphile:${job.id}`, JSON.stringify(p ?? null), {});
  },
};

const jobs = new WeakMap<Message, Job>();

/** The Graphile Worker job behind a consumed message (in-process only): id, attempts, max_attempts, … */
export function graphileJob(msg: Message): Job | undefined {
  return jobs.get(msg);
}

// ---------------------------------------------------------------- task identifiers

export class InvalidConsumerGroupError extends Error {
  constructor(group: string) {
    super(`invalid consumer group "${group}": must be non-empty`);
    this.name = 'InvalidConsumerGroupError';
  }
}

/** Task of one consumer group: `<task>.<group>`. A task's jobs are shared work, so a group gets its own task. */
export function consumerGroupTaskIdentifier(task: string, group: string): string {
  if (!group) throw new InvalidConsumerGroupError(group);
  return `${task}.${group}`;
}

function quoteIdent(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

// ---------------------------------------------------------------- Publisher

/** Per-message job settings (Graphile's `TaskSpec`). */
export interface GraphileJobSpec {
  /** Named queue: its jobs run one at a time, in order. */
  queueName?: string;
  runAt?: Date;
  maxAttempts?: number;
  /** Identifies the job for replacing / deduplicating it; see Graphile's `job_key` docs. */
  jobKey?: string;
  /** `'replace'` (default), `'preserve_run_at'` (debounce) or `'unsafe_dedupe'`. */
  jobKeyMode?: 'replace' | 'preserve_run_at' | 'unsafe_dedupe';
  /** Lower runs sooner. */
  priority?: number;
  flags?: string[];
}

export interface GraphileWorkerPublisherOptions {
  /** Graphile Worker schema. Default `graphile_worker`. */
  schema?: string;
  /** Task identifier of a topic. Default: the topic itself. */
  taskIdentifier?: (topic: string) => string;
  /**
   * Fan-out: the consumer groups of a topic. Each message becomes one job per group, for the group's own
   * task (`consumerGroupTaskIdentifier`); a `GraphileWorkerSubscriber` with that `consumerGroup` runs it.
   * `undefined` → one shared task (default); `[]` → no job is added.
   */
  consumerGroups?: (topic: string) => readonly string[] | undefined;
  /** Job settings per message, merged over the defaults (`runAt` from `_millstream_delayed_until`). */
  jobSpec?: (topic: string, msg: Message, ctx: { consumerGroup?: string }) => GraphileJobSpec;
  marshaler?: GraphileWorkerMarshaler;
}

/**
 * Adds jobs with `graphile_worker.add_job` through any `Executor` — pass the transaction of your business
 * writes (or `contextExecutor(pool)`) and the jobs are enqueued atomically with them, no outbox relay needed.
 * The schema must exist: run Graphile's migrations first (a subscriber's runner does it on start,
 * or call `runMigrations` from `graphile-worker`).
 */
export class GraphileWorkerPublisher implements Publisher {
  readonly #db: Executor;
  readonly #opts: GraphileWorkerPublisherOptions;
  #closed = false;

  constructor(db: Executor, opts: GraphileWorkerPublisherOptions = {}) {
    this.#db = db;
    this.#opts = opts;
  }

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('GraphileWorkerPublisher');
    if (!messages.length) return;
    const o = this.#opts;
    const marshaler = o.marshaler ?? defaultMarshaler;
    const task = o.taskIdentifier?.(topic) ?? topic;
    const groups = o.consumerGroups?.(topic);
    const targets: Array<string | undefined> = groups ? [...new Set(groups)] : [undefined];
    const specs = targets.flatMap((consumerGroup) =>
      messages.map((m) => {
        const until = m.metadata[DELAYED_UNTIL];
        const spec: GraphileJobSpec = {
          ...(until ? { runAt: new Date(until) } : {}),
          ...o.jobSpec?.(topic, m, { consumerGroup }),
        };
        return {
          identifier: consumerGroup === undefined ? task : consumerGroupTaskIdentifier(task, consumerGroup),
          payload: marshaler.marshal(topic, m),
          queue_name: spec.queueName ?? null,
          run_at: spec.runAt?.toISOString() ?? null,
          max_attempts: spec.maxAttempts ?? null,
          job_key: spec.jobKey ?? null,
          priority: spec.priority ?? null,
          flags: spec.flags ?? null,
          job_key_mode: spec.jobKeyMode ?? null,
        };
      }),
    );
    if (!specs.length) return;
    const schema = quoteIdent(o.schema ?? 'graphile_worker');
    // One `add_job` per message, laterally over the specs in array order (one statement: atomic).
    // Graphile picks jobs by (priority, run_at) without a tie-breaker, so jobs without an explicit
    // run_at get strictly increasing ones (clock_timestamp() + i µs): they run in publish order.
    // Parameters as text, cast server-side: every driver binds them the same way.
    await this.#db.query(
      `SELECT count(*) FROM json_array_elements($1::text::json) WITH ORDINALITY AS s(spec, n),
       LATERAL ${schema}.add_job(
         identifier => s.spec->>'identifier',
         payload => s.spec->'payload',
         queue_name => s.spec->>'queue_name',
         run_at => coalesce((s.spec->>'run_at')::timestamptz, clock_timestamp() + (s.n - 1) * interval '1 microsecond'),
         max_attempts => (s.spec->>'max_attempts')::int,
         job_key => s.spec->>'job_key',
         priority => (s.spec->>'priority')::int,
         flags => CASE WHEN json_typeof(s.spec->'flags') = 'array'
                  THEN array(SELECT json_array_elements_text(s.spec->'flags')) END,
         job_key_mode => s.spec->>'job_key_mode'
       ) AS j`,
      [JSON.stringify(specs)],
    );
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}

// ---------------------------------------------------------------- Subscriber

export class NackedError extends Error {
  constructor(reason: 'nack' | 'timeout') {
    super(reason === 'nack' ? 'message nacked by its handler' : 'handler did not answer within the ack deadline');
    this.name = 'NackedError';
  }
}

export interface GraphileWorkerSubscriberOptions {
  /** `pg.Pool` for the runner (yours), or `connectionString` to let Graphile create one. */
  pgPool?: RunnerOptions['pgPool'];
  connectionString?: string;
  /** Graphile Worker schema. Default `graphile_worker`. */
  schema?: string;
  /** Task identifier of a topic. Default: the topic itself. */
  taskIdentifier?: (topic: string) => string;
  /** Run the jobs of this consumer group's task (`consumerGroupTaskIdentifier`); the publisher must list the group. */
  consumerGroup?: string;
  /** Jobs run in parallel per subscription. Default 1. */
  concurrency?: number;
  /** How often to look for jobs that LISTEN/NOTIFY didn't announce (future `run_at`, retries). Default 2000ms. */
  pollInterval?: number;
  /**
   * What a Nack (or ack-deadline timeout) does — pick **one** layer to own retries:
   * - `'redeliver'` (default, Watermill semantics): redelivered in place after `nackResendMs` while the job
   *   stays locked, without consuming an attempt; retry policy lives in Router middlewares.
   * - `'fail'`: the job fails — Graphile retries it with exponential backoff up to `max_attempts`
   *   (25 by default), then keeps it as permanently failed.
   */
  nack?: 'redeliver' | 'fail';
  /** Wait before redelivering a nacked message in place (`nack: 'redeliver'`). Default 100ms. */
  nackResendMs?: number;
  /** No answer within this time → treated as a nack. Default: wait forever. */
  ackDeadlineMs?: number | null;
  /** Extra runner options (preset, events, useNodeTime, …). */
  runnerOptions?: Omit<
    RunnerOptions,
    | 'taskList'
    | 'taskDirectory'
    | 'crontab'
    | 'crontabFile'
    | 'parsedCronItems'
    | 'pgPool'
    | 'connectionString'
    | 'schema'
    | 'concurrency'
    | 'pollInterval'
    | 'noHandleSignals'
  >;
  marshaler?: GraphileWorkerMarshaler;
  logger?: Logger;
}

/** Graphile's log output through a millstream `Logger` (silent with the default `nopLogger`). */
function graphileLogger(logger: Logger): GraphileLogger {
  return new GraphileLogger(() => (level, message, meta) => {
    if (level === 'error' || level === 'warning') logger.error(message, meta);
    else if (level === 'info') logger.info(message, meta);
    else logger.debug(message, meta);
  });
}

/**
 * One Graphile Worker runner per subscription, with a task for the topic (`taskIdentifier`) that hands each
 * job over and waits for the answer. Ack → the job completes. Abort while holding a job → it fails with
 * `AbortedError` and runs again after Graphile's backoff — the same as Graphile's own shutdown does.
 */
export class GraphileWorkerSubscriber implements Subscriber {
  static readonly features: Features = {
    ordered: true, // with concurrency 1, one subscriber and nack: 'redeliver' (Graphile retries reorder)
    persistent: true,
    consumerGroups: false, // a task's jobs are shared work; fan-out comes from the publisher's consumerGroups
    newSubscriberReceivesOldMessages: true,
    exactlyOnceWithTx: false,
  };

  readonly #opts: GraphileWorkerSubscriberOptions;
  readonly #logger: Logger;
  readonly #closing = new AbortController();

  constructor(opts: GraphileWorkerSubscriberOptions) {
    if (!opts.pgPool && !opts.connectionString) throw new Error('pgPool or connectionString is required');
    if (opts.consumerGroup !== undefined) consumerGroupTaskIdentifier('t', opts.consumerGroup); // validate early
    this.#opts = opts;
    this.#logger = opts.logger ?? nopLogger;
  }

  subscribe(topic: string, signal?: AbortSignal): AsyncIterable<Message> {
    if (this.#closing.signal.aborted) throw new PubSubClosedError('GraphileWorkerSubscriber');
    return producerSubscription(
      (deliver, stop) => this.#run(topic, deliver, stop),
      anySignal(signal, this.#closing.signal),
    );
  }

  async close(): Promise<void> {
    this.#closing.abort();
  }

  async #run(topic: string, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const o = this.#opts;
    const base = o.taskIdentifier?.(topic) ?? topic;
    const task = o.consumerGroup === undefined ? base : consumerGroupTaskIdentifier(base, o.consumerGroup);
    const handler: Task = (_payload, helpers) => this.#handle(helpers.job, deliver, stop);
    const runner: Runner = await run({
      logger: graphileLogger(this.#logger),
      ...o.runnerOptions,
      ...(o.pgPool ? { pgPool: o.pgPool } : { connectionString: o.connectionString }),
      ...(o.schema ? { schema: o.schema } : {}),
      ...(o.pollInterval ? { pollInterval: o.pollInterval } : {}),
      concurrency: Math.max(1, o.concurrency ?? 1),
      noHandleSignals: true,
      taskList: { [task]: handler },
    });
    const stopped = new Promise<void>((resolve) => {
      if (stop.aborted) resolve();
      else stop.addEventListener('abort', () => resolve(), { once: true });
    });
    try {
      // The runner's promise only settles once stopped — or rejects when it fails for good.
      await Promise.race([stopped, runner.promise]);
    } finally {
      await runner.stop().catch(() => {});
      await runner.promise.catch(() => {});
    }
  }

  async #handle(job: Job, deliver: Deliver, stop: AbortSignal): Promise<void> {
    const o = this.#opts;
    let msg = (o.marshaler ?? defaultMarshaler).unmarshal(job);
    for (;;) {
      jobs.set(msg, job);
      const r = await deliver(msg, o.ackDeadlineMs ?? null);
      if (r === 'ack') return;
      if (r === 'aborted' || stop.aborted) throw new AbortedError();
      if (o.nack === 'fail') throw new NackedError(r);
      if (r === 'timeout') this.#logger.error('ack deadline exceeded, redelivering', { uuid: msg.uuid });
      await sleep(o.nackResendMs ?? 100, stop).catch(() => {
        throw new AbortedError();
      });
      msg = msg.copy();
    }
  }
}
