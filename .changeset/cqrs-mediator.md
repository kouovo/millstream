---
'@millstream/cqrs': minor
---

New `@millstream/cqrs/mediator` entry: in-process, synchronous commands and queries. `defineCommand` / `defineQuery` (Standard Schema validation, `.returns<R>()` for the result type), and a `Mediator` with exactly one handler per name, `execute` / `query` resolving with the handler's result, and `(ctx, next)` middlewares (e.g. run commands in a transaction so outbox events commit with them).
