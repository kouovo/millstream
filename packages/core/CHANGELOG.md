# @millstream/core

## 0.5.0

### Minor Changes

- a11948b: **Breaking:** `MemoryPubSub` is now parallel and unordered by default, matching Watermill GoChannel: every message is handed out right away and waits for its own ack/nack, so a Router handler's `concurrency` actually takes effect (it previously had no effect on `MemoryPubSub`). `MemoryPubSub.features.ordered` is now `false`.
  
  New `concurrency` option picks the delivery mode: `Infinity` (default), `1` for the previous ordered, one-at-a-time behaviour (`new MemoryPubSub({ concurrency: 1 })`), or `N` to cap unacked messages per subscription.

## 0.4.0

No changes in this release.

## 0.3.0

### Patch Changes

- 97d9ca5: All `@millstream/*` packages now share one version and are released together. `@millstream/core`: the README links to `@millstream/otel` for tracing and metrics.
- dd9f92e: Packages are now built with tsdown (Rolldown): each entry point ships as one bundled ESM file with a rolled-up `.d.ts`, instead of one file per source module. Public exports are unchanged.
