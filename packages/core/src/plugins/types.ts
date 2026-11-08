import type { Router } from '../router.js';

/**
 * Runs when the router starts, before any handler subscribes (Watermill: `message.RouterPlugin`).
 * A throwing plugin makes `run()` fail. Clean up after `router.closed` resolves.
 */
export type RouterPlugin = (router: Router) => void | Promise<void>;
