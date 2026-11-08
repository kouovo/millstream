import { type MysqlDriverLike, toMysqlDriver } from './driver.js';

const TOPIC_RE = /^[A-Za-z0-9_.-]{1,40}$/;

export class InvalidTopicError extends Error {
  constructor(topic: string) {
    super(`invalid topic "${topic}": must match ${TOPIC_RE} (it becomes part of a table name)`);
    this.name = 'InvalidTopicError';
  }
}

/** Topics are spliced into SQL identifiers, so they are whitelisted. */
export function validateTopic(topic: string): string {
  if (!TOPIC_RE.test(topic)) throw new InvalidTopicError(topic);
  return topic;
}

const q = (ident: string) => `\`${ident}\``;

export const tables = {
  /** Queue-type (claim + delete) table. */
  queue: (topic: string) => q(`millstream_queue_${validateTopic(topic)}`),
  inbox: q('millstream_inbox'),
};

/**
 * Queue-type topic (task-queue-like): rows are claimed with `FOR UPDATE SKIP LOCKED`
 * (MySQL 8.0+) and deleted on ack. No consumer groups, no global order, horizontally parallel.
 * `delayed_until` (UTC) makes it usable as the delayed requeue store.
 */
export function queueTopicStatements(topic: string): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${tables.queue(topic)} (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  uuid VARCHAR(255) NOT NULL,
  payload LONGBLOB NOT NULL,
  metadata JSON NOT NULL,
  delayed_until DATETIME(6) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6))
) ENGINE=InnoDB`,
  ];
}

export function inboxStatements(): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${tables.inbox} (
  consumer VARCHAR(255) NOT NULL,
  uuid VARCHAR(255) NOT NULL,
  processed_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  PRIMARY KEY (consumer, uuid),
  KEY millstream_inbox_processed_at (processed_at)
) ENGINE=InnoDB`,
  ];
}

const toSql = (statements: string[]) => statements.map((s) => `${s};`).join('\n');
/** DDL for your migration tool. */
export const queueTopicSchemaSql = (topic: string): string => toSql(queueTopicStatements(topic));
export const inboxSchemaSql = (): string => toSql(inboxStatements());

export interface SchemaOptions {
  queueTopics?: string[];
  inbox?: boolean;
}

const SCHEMA_LOCK = 'millstream_schema';

/**
 * Create tables. Meant for migrations / startup — never inside a business transaction
 * (DDL implicitly commits in MySQL). Serialised across processes by a named lock.
 */
export async function initializeSchema(driver: MysqlDriverLike, opts: SchemaOptions): Promise<void> {
  const statements = [
    ...(opts.queueTopics ?? []).flatMap(queueTopicStatements),
    ...(opts.inbox ? inboxStatements() : []),
  ];
  if (!statements.length) return;
  // A transaction only to pin one connection: GET_LOCK is per session.
  await toMysqlDriver(driver).transaction(async (tx) => {
    const { rows } = await tx.query('SELECT GET_LOCK(?, 30) AS ok', [SCHEMA_LOCK]);
    if (Number(rows[0]?.ok) !== 1) throw new Error('timed out waiting for the millstream schema lock');
    try {
      for (const statement of statements) await tx.query(statement);
    } finally {
      await tx.query('SELECT RELEASE_LOCK(?)', [SCHEMA_LOCK]);
    }
  });
}
