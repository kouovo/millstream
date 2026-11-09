import { runWithContext, type HandlerContext } from './context.js';
import type { Message } from './message.js';
import type { DecoratorInfo, PublisherDecorator, SubscriberDecorator } from './decorators/types.js';
import type { RouterPlugin } from './plugins/types.js';
import type { Publisher, Subscriber } from './pubsub.js';
import { Semaphore, isAbortError, nopLogger, type Logger } from './util.js';

/** Returns the produced messages (published to `publishTopic` before the input is acked). */
export type HandlerFn = (msg: Message) => Promise<Message[] | void> | Message[] | void;
export type ConsumerHandlerFn = (msg: Message) => Promise<void> | void;
export type Middleware = (next: HandlerFn) => HandlerFn;

export class OutputInNoPublisherHandlerError extends Error {
  constructor(handler: string) {
    super(`handler "${handler}" returned messages but has no publisher`);
    this.name = 'OutputInNoPublisherHandlerError';
  }
}

export interface HandlerConfig {
  name: string;
  subscribeTopic: string;
  subscriber: Subscriber;
  publishTopic?: string;
  publisher?: Publisher;
  handler: HandlerFn;
  /** Max messages processed in parallel. Default 1 (the only setting that preserves order). */
  concurrency?: number;
  /** Handler-level middlewares, applied inside router-level ones. */
  middlewares?: Middleware[];
}

export interface ConsumerHandlerConfig extends Omit<HandlerConfig, 'handler' | 'publishTopic' | 'publisher'> {
  handler: ConsumerHandlerFn;
}

export interface RouterOptions {
  /** How long `close()` waits for in-flight messages. Default 30s (Watermill CloseTimeout). */
  closeTimeoutMs?: number;
  logger?: Logger;
}

/** Wrap middlewares so the first one is the outermost (Watermill order). */
export function compose(handler: HandlerFn, middlewares: Middleware[]): HandlerFn {
  return middlewares.reduceRight<HandlerFn>((next, mw) => mw(next), handler);
}

export class Handler {
  readonly #controller = new AbortController();
  #stopping = false;
  readonly inflight: Set<Promise<void>> = new Set();
  /** Resolves when the subscription loop has exited and in-flight messages are done. */
  done!: Promise<void>;

  /** The handler's publisher after router-level decorators were applied (set when it starts). */
  publisher?: Publisher;

  constructor(readonly config: HandlerConfig) {}

  get name(): string {
    return this.config.name;
  }

  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  get stopping(): boolean {
    return this.#stopping;
  }

  /** Phase 1 of shutdown: stop taking new messages, keep in-flight ones going. */
  beginStop(): void {
    this.#stopping = true;
  }

  /** Phase 2: abort the subscription (and signal in-flight handlers). */
  abort(): void {
    this.#stopping = true;
    this.#controller.abort();
  }

  /** Stop just this handler (Go: `Handler.Stop()`). */
  async stop(timeoutMs = 30_000): Promise<void> {
    this.beginStop();
    await waitWithTimeout(Promise.allSettled(this.inflight), timeoutMs);
    this.abort();
    await this.done;
  }
}

/**
 * Message router — the "HTTP server" of messages.
 *
 * Per handler: one subscription loop; each message runs through the middleware chain,
 * produced messages are published, and only then the input is acked (at-least-once).
 * Any error / throw → nack.
 */
export class Router {
  readonly #handlers = new Map<string, Handler>();
  readonly #middlewares: Middleware[] = [];
  readonly #plugins: RouterPlugin[] = [];
  readonly #publisherDecorators: PublisherDecorator[] = [];
  readonly #subscriberDecorators: SubscriberDecorator[] = [];
  readonly #closeTimeoutMs: number;
  readonly #logger: Logger;
  #started = false;
  #starting = false;
  #closed = false;
  #closing?: Promise<void>;
  #resolveAllStopped!: () => void;
  readonly #allStopped = new Promise<void>((r) => (this.#resolveAllStopped = r));
  #resolveRunning!: () => void;
  /** Resolves once every handler has subscribed. */
  readonly running: Promise<void> = new Promise<void>((r) => (this.#resolveRunning = r));

  constructor(opts: RouterOptions = {}) {
    this.#closeTimeoutMs = opts.closeTimeoutMs ?? 30_000;
    this.#logger = opts.logger ?? nopLogger;
  }

  get logger(): Logger {
    return this.#logger;
  }

  /** Resolves once the router has fully stopped (after `close()` or when every handler exited). */
  get closed(): Promise<void> {
    return this.#allStopped;
  }

  /** Plugins run in order when `run()` starts, before any handler subscribes. */
  addPlugin(...plugins: RouterPlugin[]): this {
    if (this.#started || this.#starting) throw new Error('plugins must be added before run()');
    this.#plugins.push(...plugins);
    return this;
  }

  /**
   * Wrap the publisher of every handler (applied when each handler starts, including handlers
   * added at runtime). The first added is the innermost — closest to the real publisher.
   */
  addPublisherDecorators(...decorators: PublisherDecorator[]): this {
    this.#publisherDecorators.push(...decorators);
    return this;
  }

  /** Wrap the subscriber of every handler. The first added is the innermost. */
  addSubscriberDecorators(...decorators: SubscriberDecorator[]): this {
    this.#subscriberDecorators.push(...decorators);
    return this;
  }

  /** Router-level middlewares; the first added is the outermost. */
  use(...mw: Middleware[]): this {
    this.#middlewares.push(...mw);
    return this;
  }

  addHandler(config: HandlerConfig): Handler {
    if (this.#closed) throw new Error('router is closed');
    if (this.#handlers.has(config.name)) throw new Error(`handler "${config.name}" already exists`);
    const h = new Handler(config);
    this.#handlers.set(config.name, h);
    if (this.#started) this.#start(h); // Go: RunHandlers — add handlers at runtime
    return h;
  }

  addConsumerHandler(config: ConsumerHandlerConfig): Handler {
    const { handler, ...rest } = config;
    return this.addHandler({
      ...rest,
      handler: async (msg) => {
        await handler(msg);
      },
    });
  }

  handlers(): Handler[] {
    return [...this.#handlers.values()];
  }

  /**
   * Start every handler. Resolves when the router has stopped — after `close()`,
   * after `signal` aborts, or when all handlers exited on their own (so the
   * process doesn't hang around "alive but consuming nothing").
   */
  async run(signal?: AbortSignal): Promise<void> {
    if (this.#started || this.#starting) throw new Error('router already running');
    if (this.#closed) throw new Error('router is closed');
    this.#starting = true;
    try {
      for (const plugin of this.#plugins) await plugin(this);
    } finally {
      this.#starting = false;
    }
    if (this.#closed) return; // a plugin closed it
    this.#started = true;
    signal?.addEventListener('abort', () => void this.close(), { once: true });
    if (signal?.aborted) void this.close();
    for (const h of this.#handlers.values()) this.#start(h);
    this.#resolveRunning();
    this.#watchAllStopped();
    await this.#allStopped;
    await this.#closing;
  }

  /** Stop pulling → wait for in-flight messages (up to closeTimeoutMs) → abort subscriptions. */
  close(): Promise<void> {
    this.#closing ??= this.#doClose();
    return this.#closing;
  }

  async #doClose(): Promise<void> {
    this.#closed = true;
    const handlers = [...this.#handlers.values()];
    for (const h of handlers) h.beginStop();
    const inflight = handlers.flatMap((h) => [...h.inflight]);
    const finished = await waitWithTimeout(Promise.allSettled(inflight), this.#closeTimeoutMs);
    if (!finished) this.#logger.error('router close timeout, aborting in-flight handlers');
    for (const h of handlers) h.abort();
    await Promise.allSettled(handlers.map((h) => h.done));
    this.#resolveAllStopped();
  }

  #watchAllStopped(): void {
    const handlers = [...this.#handlers.values()];
    if (!handlers.length) return;
    void Promise.allSettled(handlers.map((h) => h.done)).then(() => {
      // Handlers added at runtime may still be running.
      if ([...this.#handlers.values()].every((h) => handlers.includes(h))) {
        if (!this.#closed) this.#logger.info('all handlers stopped, closing router');
        void this.close();
      } else {
        this.#watchAllStopped();
      }
    });
  }

  #start(h: Handler): void {
    const cfg = h.config;
    const wrapped = compose(cfg.handler, [...this.#middlewares, ...(cfg.middlewares ?? [])]);
    const info: DecoratorInfo = {
      handlerName: cfg.name,
      subscribeTopic: cfg.subscribeTopic,
      publishTopic: cfg.publishTopic,
    };
    const subscriber = this.#subscriberDecorators.reduce((sub, decorate) => decorate(sub, info), cfg.subscriber);
    h.publisher =
      cfg.publisher && this.#publisherDecorators.reduce((pub, decorate) => decorate(pub, info), cfg.publisher);
    const iterable = subscriber.subscribe(cfg.subscribeTopic, h.signal);
    h.done = this.#loop(h, iterable, wrapped).finally(() => this.#handlers.delete(h.name));
  }

  async #loop(h: Handler, iterable: AsyncIterable<Message>, wrapped: HandlerFn): Promise<void> {
    const limiter = new Semaphore(h.config.concurrency ?? 1);
    const it = iterable[Symbol.asyncIterator]();
    try {
      for (;;) {
        // Abortable: a stuck handler may hold every slot forever.
        const acquired = await Promise.race([
          limiter.acquire().then(() => true),
          abortedPromise(h.signal).then(() => false),
        ]);
        if (!acquired) break;
        if (h.stopping) {
          limiter.release();
          break;
        }
        const r = await it.next();
        if (r.done) break;
        if (h.stopping) {
          // Left un-acked: persistent subscribers redeliver it later.
          break;
        }
        const p = this.#handleMessage(h, r.value, wrapped).finally(() => {
          limiter.release();
          h.inflight.delete(p);
        });
        h.inflight.add(p);
      }
    } catch (e) {
      if (!isAbortError(e)) this.#logger.error('subscription failed', { handler: h.name, error: e });
    } finally {
      await it.return?.().catch(() => {});
      // Wait for in-flight messages — unless the handler was aborted (close timeout exceeded):
      // a handler ignoring its signal must not keep `close()` / `run()` hanging forever.
      await Promise.race([Promise.allSettled(h.inflight), abortedPromise(h.signal)]);
    }
  }

  async #handleMessage(h: Handler, msg: Message, wrapped: HandlerFn): Promise<void> {
    const cfg = h.config;
    const ctx: HandlerContext = {
      handlerName: cfg.name,
      subscribeTopic: cfg.subscribeTopic,
      publishTopic: cfg.publishTopic,
      message: msg,
      signal: h.signal,
      tx: msg.tx,
    };
    try {
      await runWithContext(ctx, async () => {
        const produced = await wrapped(msg);
        if (produced && produced.length) {
          if (!h.publisher || !cfg.publishTopic) throw new OutputInNoPublisherHandlerError(cfg.name);
          await h.publisher.publish(cfg.publishTopic, ...produced); // publish first…
        }
      });
      msg.ack(); // …then ack
    } catch (error) {
      this.#logger.error('handler failed, nacking', { handler: cfg.name, uuid: msg.uuid, error });
      msg.nack();
    }
  }
}

function abortedPromise(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
}

async function waitWithTimeout(p: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<false>((r) => (timer = setTimeout(() => r(false), ms)));
  try {
    return await Promise.race([p.then(() => true as const), timeout]);
  } finally {
    clearTimeout(timer);
  }
}
