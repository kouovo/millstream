# @millstream/cqrs

## 0.4.0

### Minor Changes

- 423fe0f: New `@millstream/cqrs/mediator` entry: in-process, synchronous commands and queries. `defineCommand` / `defineQuery` (Standard Schema validation, `.returns<R>()` for the result type), and a `Mediator` with exactly one handler per name, `execute` / `query` resolving with the handler's result, and `(ctx, next)` middlewares (e.g. run commands in a transaction so outbox events commit with them).

### Patch Changes

- @millstream/core@0.4.0

## 0.3.0

### Minor Changes

- 0e3b55c: `EventProcessor.on` and `on()` (for groups) accept an array of handlers that run in series on one subscription. The first failure stops the chain and fails the message.

### Patch Changes

- Updated dependencies [97d9ca5]
- Updated dependencies [dd9f92e]
  - @millstream/core@0.3.0
