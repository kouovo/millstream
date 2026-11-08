import { randomUUID } from 'node:crypto';
import type { Executor } from './executor.js';

export type Metadata = Record<string, string>;
export type AckResult = 'ack' | 'nack';

/**
 * A message with an Ack/Nack "answer" — the Node counterpart of Watermill's
 * `*message.Message` whose ack/noAck channels get closed exactly once.
 *
 * - `ack()` / `nack()` are idempotent and mutually exclusive (same state machine as Go).
 * - `settled` resolves once the consumer answered; subscribers await it.
 * - `tx` is an in-process side channel (Go: `msg.Context()`), never serialized.
 *   SQL subscribers attach their transaction here so handlers can join it.
 */
export class Message {
  readonly uuid: string;
  payload: Buffer;
  metadata: Metadata;
  /** In-process only: transaction of the subscriber that delivered this message. */
  tx?: Executor;

  readonly settled: Promise<AckResult>;
  #resolve!: (r: AckResult) => void;
  #state: 'pending' | AckResult = 'pending';

  constructor(uuid: string, payload: Buffer | string, metadata: Metadata = {}) {
    this.uuid = uuid;
    this.payload = typeof payload === 'string' ? Buffer.from(payload) : payload;
    this.metadata = { ...metadata };
    this.settled = new Promise<AckResult>((resolve) => (this.#resolve = resolve));
  }

  /** Returns false if the message was already nacked. Repeated acks are a no-op. */
  ack(): boolean {
    if (this.#state === 'nack') return false;
    if (this.#state === 'pending') {
      this.#state = 'ack';
      this.#resolve('ack');
    }
    return true;
  }

  /** Returns false if the message was already acked. Repeated nacks are a no-op. */
  nack(): boolean {
    if (this.#state === 'ack') return false;
    if (this.#state === 'pending') {
      this.#state = 'nack';
      this.#resolve('nack');
    }
    return true;
  }

  get state(): 'pending' | AckResult {
    return this.#state;
  }

  /** A fresh, un-answered copy (same uuid/payload/metadata). Used for redelivery and fan-out. */
  copy(): Message {
    return new Message(this.uuid, Buffer.from(this.payload), this.metadata);
  }

  /** Content equality (uuid + payload + metadata), ignoring ack state. */
  equals(other: Message): boolean {
    if (this.uuid !== other.uuid || !this.payload.equals(other.payload)) return false;
    const a = Object.keys(this.metadata);
    const b = Object.keys(other.metadata);
    return a.length === b.length && a.every((k) => this.metadata[k] === other.metadata[k]);
  }
}

export function newMessage(payload: Buffer | string, metadata: Metadata = {}, uuid: string = randomUUID()): Message {
  return new Message(uuid, payload, metadata);
}

export function newUUID(): string {
  return randomUUID();
}
