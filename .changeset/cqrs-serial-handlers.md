---
'@millstream/cqrs': minor
---

`EventProcessor.on` and `on()` (for groups) accept an array of handlers that run in series on one subscription. The first failure stops the chain and fails the message.
