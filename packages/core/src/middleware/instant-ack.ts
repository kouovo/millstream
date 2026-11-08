import type { Middleware } from '../router.js';

/** Ack before the handler runs: throughput over reliability (failures lose the message). */
export const instantAck: Middleware = (next) => async (msg) => {
  msg.ack();
  return next(msg);
};
