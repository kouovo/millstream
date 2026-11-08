import type { RouterPlugin } from './types.js';

export interface SignalsHandlerOptions {
  /** Default `['SIGINT', 'SIGTERM']`. */
  signals?: NodeJS.Signals[];
  /** Called with the signal before closing (e.g. to log it). */
  onSignal?: (signal: NodeJS.Signals) => void;
}

/**
 * Gracefully close the router on SIGINT / SIGTERM (Watermill: `plugin.SignalsHandler`).
 *
 * While installed, the listeners replace Node's default "exit on signal": the process exits
 * once the router has drained and nothing else keeps it alive. The listeners are removed
 * when the router closes, so a second signal after that gets Node's default behaviour.
 */
export function signalsHandler(opts: SignalsHandlerOptions = {}): RouterPlugin {
  const signals = opts.signals ?? ['SIGINT', 'SIGTERM'];
  return (router) => {
    const onSignal = (signal: NodeJS.Signals) => {
      opts.onSignal?.(signal);
      router.logger.info('signal received, closing router', { signal });
      void router.close();
    };
    for (const s of signals) process.on(s, onSignal);
    void router.closed.then(() => {
      for (const s of signals) process.off(s, onSignal);
    });
  };
}
