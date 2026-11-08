import type { Message } from '../message.js';
import type { HandlerFn, Middleware } from '../router.js';

/** Apply a middleware only to messages matching `pred`. */
export function when(pred: (msg: Message) => boolean, mw: Middleware): Middleware {
  return (next: HandlerFn) => {
    const wrapped = mw(next);
    return (msg) => (pred(msg) ? wrapped(msg) : next(msg));
  };
}
