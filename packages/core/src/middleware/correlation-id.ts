import type { Message } from '../message.js';
import type { Middleware } from '../router.js';

export const CORRELATION_ID = 'correlation_id';

export function setCorrelationId(msg: Message, id: string): void {
  msg.metadata[CORRELATION_ID] = id;
}

export function correlationIdOf(msg: Message): string | undefined {
  return msg.metadata[CORRELATION_ID];
}

/** Copies the input's correlation_id onto every produced message. */
export const correlationId: Middleware = (next) => async (msg) => {
  const produced = await next(msg);
  const id = correlationIdOf(msg);
  if (produced && id) for (const m of produced) m.metadata[CORRELATION_ID] ??= id;
  return produced;
};
