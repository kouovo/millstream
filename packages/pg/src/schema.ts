import type { Executor } from '@millstream/core';
import { type SqlDriverLike, toSqlDriver } from './driver.js';

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

const q = (ident: string) => `"${ident}"`;

export const tables = {
  /** Log-type message table. */
  messages: (topic: string) => q(`millstream_${validateTopic(topic)}`),
  /** Per-consumer-group progress for a log-type topic. */
  offsets: (topic: string) => q(`millstream_offsets_${validateTopic(topic)}`),
  /** Queue-type (claim + delete) table. */
  queue: (topic: string) => q(`millstream_queue_${validateTopic(topic)}`),
  inbox: q('millstream_inbox'),
};

/** NOTIFY channel for a topic (used as a wake-up hint, never as the source of truth). */
export const notifyChannel = (topic: string) => `millstream_${validateTopic(topic)}`;
export const queueNotifyChannel = (topic: string) => `millstream_queue_${validateTopic(topic)}`;

/**
 * Log-type topic (Kafka-like): messages are kept, each consumer group tracks
 * `(last_processed_transaction_id, offset_acked)`.
 * `transaction_id xid8` + PK `(transaction_id, offset)` is what makes the
 * xmin-watermark read safe; needs PostgreSQL 13+.
 */
export function logTopicStatements(topic: string): string[] {
  const m = tables.messages(topic);
  const o = tables.offsets(topic);
  return [
    `CREATE TABLE IF NOT EXISTS ${m} (
  "offset" BIGSERIAL,
  uuid TEXT NOT NULL,
  payload BYTEA NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}',
  transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (transaction_id, "offset")
)`,
    `CREATE TABLE IF NOT EXISTS ${o} (
  consumer_group TEXT PRIMARY KEY,
  offset_acked BIGINT NOT NULL DEFAULT 0,
  last_processed_transaction_id XID8 NOT NULL DEFAULT '0'
)`,
  ];
}

/**
 * Queue-type topic (task-queue-like): rows are claimed with `FOR UPDATE SKIP LOCKED`
 * and deleted on ack. No consumer groups, no global order, horizontally parallel.
 * `delayed_until` makes it usable as the delayed requeue store.
 */
export function queueTopicStatements(topic: string): string[] {
  const t = tables.queue(topic);
  return [
    `CREATE TABLE IF NOT EXISTS ${t} (
  id BIGSERIAL PRIMARY KEY,
  uuid TEXT NOT NULL,
  payload BYTEA NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}',
  delayed_until TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`,
    `CREATE INDEX IF NOT EXISTS ${q(`millstream_queue_${topic}_due`)} ON ${t} (delayed_until, id)`,
  ];
}

export function inboxStatements(): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${tables.inbox} (
  consumer TEXT NOT NULL,
  uuid TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, uuid)
)`,
    `CREATE INDEX IF NOT EXISTS ${q('millstream_inbox_processed_at')} ON ${tables.inbox} (processed_at)`,
  ];
}

const toSql = (statements: string[]) => statements.map((s) => `${s};`).join('\n');
/** DDL for your migration tool. */
export const logTopicSchemaSql = (topic: string): string => toSql(logTopicStatements(topic));
export const queueTopicSchemaSql = (topic: string): string => toSql(queueTopicStatements(topic));
export const inboxSchemaSql = (): string => toSql(inboxStatements());

export interface SchemaOptions {
  logTopics?: string[];
  queueTopics?: string[];
  inbox?: boolean;
}

/**
 * Create tables. Meant for migrations / startup — never inside a business transaction
 * (Watermill refuses auto-init in a tx for the same reason). Runs in its own transaction,
 * serialised by an advisory lock (concurrent CREATE ... IF NOT EXISTS can race).
 */
export async function initializeSchema(driver: SqlDriverLike, opts: SchemaOptions): Promise<void> {
  const statements = [
    ...(opts.logTopics ?? []).flatMap(logTopicStatements),
    ...(opts.queueTopics ?? []).flatMap(queueTopicStatements),
    ...(opts.inbox ? inboxStatements() : []),
  ];
  if (!statements.length) return;
  await toSqlDriver(driver).transaction(async (tx: Executor) => {
    await tx.query('SELECT pg_advisory_xact_lock(7212443501)');
    for (const statement of statements) await tx.query(statement);
  });
}
