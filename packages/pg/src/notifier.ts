import { type Logger, nopLogger, sleep } from '@millstream/core';

/** A driver's LISTEN capability. `onNotify` is also called after a reconnect (notifications may have been lost). */
export interface NotificationSource {
  listen(channel: string, onNotify: () => void): Promise<void>;
  close(): Promise<void>;
}

/**
 * LISTEN/NOTIFY wake-up hints for subscribers.
 *
 * Only a hint: a notification may arrive while an older transaction still holds the
 * xmin watermark back (the next query finds nothing), and notifications are lost while
 * disconnected. Subscribers therefore keep polling; this just cuts the average latency
 * from `pollIntervalMs` down to milliseconds.
 */
export class PgNotifier {
  readonly #channels = new Set<string>();
  readonly #generation = new Map<string, number>();
  readonly #waiters = new Map<string, Set<() => void>>();
  readonly #logger: Logger;

  constructor(
    private readonly source: NotificationSource,
    opts: { logger?: Logger } = {},
  ) {
    this.#logger = opts.logger ?? nopLogger;
  }

  /** node-postgres: one dedicated `Client` (pass a factory, e.g. `() => new pg.Client(url)`), reconnects with backoff. */
  static nodePostgres(newClient: () => NodePostgresListenClient, opts: { logger?: Logger } = {}): PgNotifier {
    return new PgNotifier(nodePostgresSource(newClient, opts.logger ?? nopLogger), opts);
  }

  /** postgres.js: uses `sql.listen` (postgres.js keeps a dedicated connection and re-listens on reconnect). */
  static postgresJs(sql: PostgresJsListenLike, opts: { logger?: Logger } = {}): PgNotifier {
    return new PgNotifier(postgresJsSource(sql), opts);
  }

  /** Current generation of a channel; pass it to `wait` to not miss notifications in between. */
  generation(channel: string): number {
    return this.#generation.get(channel) ?? 0;
  }

  async listen(channel: string): Promise<void> {
    if (this.#channels.has(channel)) return;
    this.#channels.add(channel);
    try {
      await this.source.listen(channel, () => this.#bump(channel));
    } catch (error) {
      this.#channels.delete(channel);
      this.#logger.error('LISTEN failed, falling back to polling', { channel, error });
    }
  }

  /** Resolve after a notification newer than `since`, after `ms`, or on abort. */
  wait(channel: string, since: number, ms: number, signal?: AbortSignal): Promise<void> {
    if (this.generation(channel) !== since) return Promise.resolve();
    return new Promise((resolve) => {
      let set = this.#waiters.get(channel);
      if (!set) this.#waiters.set(channel, (set = new Set()));
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', done);
        set.delete(done);
        resolve();
      };
      const timer = setTimeout(done, ms);
      signal?.addEventListener('abort', done, { once: true });
      set.add(done);
    });
  }

  #bump(channel: string): void {
    this.#generation.set(channel, this.generation(channel) + 1);
    for (const w of this.#waiters.get(channel) ?? []) w();
  }

  async close(): Promise<void> {
    for (const set of this.#waiters.values()) for (const w of set) w();
    await this.source.close();
  }
}

// ---------------------------------------------------------------- node-postgres

export interface NodePostgresListenClient {
  connect(): Promise<unknown>;
  query(text: string): Promise<unknown>;
  end(): Promise<void>;
  on(event: 'notification', fn: (n: { channel: string }) => void): unknown;
  on(event: 'error', fn: (err: Error) => void): unknown;
}

function nodePostgresSource(newClient: () => NodePostgresListenClient, logger: Logger): NotificationSource {
  const handlers = new Map<string, () => void>();
  let client: NodePostgresListenClient | undefined;
  let connecting: Promise<NodePostgresListenClient> | undefined;
  let closed = false;

  const ensure = (): Promise<NodePostgresListenClient> => {
    if (client) return Promise.resolve(client);
    connecting ??= (async () => {
      const c = newClient();
      c.on('notification', (n) => handlers.get(n.channel)?.());
      c.on('error', (error) => {
        logger.error('notifier connection error', { error });
        if (client === c) client = undefined;
        c.end().catch(() => {});
        void reconnect();
      });
      await c.connect();
      for (const ch of handlers.keys()) await c.query(`LISTEN "${ch}"`);
      client = c;
      return c;
    })().finally(() => (connecting = undefined));
    return connecting;
  };

  const reconnect = async () => {
    // `closed` is flipped by close() from outside this loop.
    for (let delay = 200; ; delay = Math.min(delay * 2, 10_000)) {
      if (closed) return;
      try {
        await sleep(delay);
        await ensure();
        for (const h of handlers.values()) h(); // wake everyone: notifications may have been lost
        return;
      } catch (error) {
        logger.error('notifier reconnect failed', { error });
      }
    }
  };

  return {
    async listen(channel, onNotify) {
      handlers.set(channel, onNotify);
      const c = await ensure();
      await c.query(`LISTEN "${channel}"`);
    },
    async close() {
      closed = true;
      const c = client ?? (await connecting?.catch(() => undefined));
      client = undefined;
      await c?.end().catch(() => {});
    },
  };
}

// ---------------------------------------------------------------- postgres.js

export interface PostgresJsListenLike {
  listen(channel: string, fn: (payload: string) => void, onlisten?: () => void): Promise<{ unlisten(): Promise<void> }>;
}

function postgresJsSource(sql: PostgresJsListenLike): NotificationSource {
  const subs: Array<{ unlisten(): Promise<void> }> = [];
  return {
    async listen(channel, onNotify) {
      // `onlisten` fires on every (re)connect → wake waiters after a reconnect.
      subs.push(await sql.listen(channel, () => onNotify(), onNotify));
    },
    async close() {
      await Promise.allSettled(subs.splice(0).map((s) => s.unlisten()));
    },
  };
}
