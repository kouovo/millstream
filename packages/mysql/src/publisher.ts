import { DELAYED_UNTIL, type Executor, type Message, type Publisher, PubSubClosedError } from '@millstream/core';
import { tables } from './schema.js';

/** ISO timestamp → `DATETIME(6)` literal in UTC (the column is compared with `UTC_TIMESTAMP(6)`). */
export function toUtcDatetime(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) throw new Error(`invalid ${DELAYED_UNTIL}: "${iso}"`);
  return d.toISOString().replace('T', ' ').replace('Z', '');
}

/**
 * Queue-type publisher: `publish` is a multi-row INSERT. Pass a driver/pool for standalone
 * publishing, or the transaction's executor (or `contextExecutor(driver)`) to write an outbox
 * atomically with business data. `_millstream_delayed_until` metadata is copied into the
 * `delayed_until` column: the row stays invisible to subscribers until then.
 */
export class MysqlQueuePublisher implements Publisher {
  #closed = false;
  constructor(private readonly db: Executor) {}

  async publish(topic: string, ...messages: Message[]): Promise<void> {
    if (this.#closed) throw new PubSubClosedError('MysqlQueuePublisher');
    if (!messages.length) return;
    const params: unknown[] = [];
    const rows = messages.map((m) => {
      const until = m.metadata[DELAYED_UNTIL];
      params.push(m.uuid, m.payload, JSON.stringify(m.metadata), until ? toUtcDatetime(until) : null);
      return '(?, ?, ?, ?)';
    });
    // VALUES rows keep their order → AUTO_INCREMENT ids follow the given order.
    await this.db.query(
      `INSERT INTO ${tables.queue(topic)} (uuid, payload, metadata, delayed_until) VALUES ${rows.join(', ')}`,
      params,
    );
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}
