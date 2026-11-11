---
'@millstream/core': minor
---

**Breaking:** `MemoryPubSub` is now parallel and unordered by default, matching Watermill GoChannel: every message is handed out right away and waits for its own ack/nack, so a Router handler's `concurrency` actually takes effect (it previously had no effect on `MemoryPubSub`). `MemoryPubSub.features.ordered` is now `false`.

New `concurrency` option picks the delivery mode: `Infinity` (default), `1` for the previous ordered, one-at-a-time behaviour (`new MemoryPubSub({ concurrency: 1 })`), or `N` to cap unacked messages per subscription.
