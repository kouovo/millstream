import { DELAYED_UNTIL, type Executor, type Message, type Publisher, PubSubClosedError } from '@millstream/core';
import { notifyChannel, queueNotifyChannel, tables } from './schema.js';

export interface PgPublisherOptions {
  /** Also `pg_notify` the topic channel (delivered only on commit). Default true. */
  notify?: boolean;
}

/**
 * Multi-row `VALUES` with every parameter sent as text and cast server-side, so all
 * drivers bind them the same way (no driver-specific array / json serialisation —
 * e.g. postgres.js would JSON-encode a string bound to a `jsonb` parameter again).
 */
function values(messages: Message[], columns: (m: Message) => unknown[], casts: string[]) {
  const params: unknown[] = [];
  const rows = messages.map((m) => {
    const vals = columns(m);
    return `(${vals.map((v, i) => (params.push(v), `$${params.length}${casts[i]}`)).join(', ')})`;
  });
  return { sql: rows.join(', '), params };
}

function withNotify(insert: string, params: unknown[], channel: string, notify: boolean | undefined) {
  if (notify === false) return { text: insert, params };
  return {
    text: `WITH ins AS (${insert} RETURNING 1) SELECT pg_notify($${params.length + 1}, count(*)::text) FROM ins`,
    params: [...params, channel],
  };
}

/**
 * Log-type publisher: `publish` is just a multi-row INSERT, tagged with
 * `pg_current_xact_id()`. Pass a driver/pool for standalone publishing, or the
 * transaction's executor (or `contextExecutor(driver)`) to write an outbox
 * atomically with business data.
 */
export class PgPublisher implements Publisher {
  #closed = false;
  constructor(
    private readonly db: Executor,
    private readonly opts: PgPublisherOptions = {},
  ) {}

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('PgPublisher');
    if (!messages.length) return;
    const v = values(messages, (m) => [m.uuid, m.payload, JSON.stringify(m.metadata)], [
      '::text',
      '::bytea',
      '::text::jsonb',
    ]);
    // VALUES rows keep their order → bigserial offsets follow the given order.
    const insert = `INSERT INTO ${tables.messages(topic)} (uuid, payload, metadata) VALUES ${v.sql}`;
    const q = withNotify(insert, v.params, notifyChannel(topic), this.opts.notify);
    await this.db.query(q.text, q.params);
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}

/**
 * Queue-type publisher. `_watermill_delayed_until` metadata is copied into the
 * indexed `delayed_until` column: the row stays invisible to subscribers until then.
 */
export class PgQueuePublisher implements Publisher {
  #closed = false;
  constructor(
    private readonly db: Executor,
    private readonly opts: PgPublisherOptions = {},
  ) {}

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('PgQueuePublisher');
    if (!messages.length) return;
    const v = values(
      messages,
      (m) => [m.uuid, m.payload, JSON.stringify(m.metadata), m.metadata[DELAYED_UNTIL] ?? null],
      ['::text', '::bytea', '::text::jsonb', '::text::timestamptz'],
    );
    const insert = `INSERT INTO ${tables.queue(topic)} (uuid, payload, metadata, delayed_until) VALUES ${v.sql}`;
    const q = withNotify(insert, v.params, queueNotifyChannel(topic), this.opts.notify);
    await this.db.query(q.text, q.params);
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}
