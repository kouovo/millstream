# @millstream/cqrs

## 0.3.0

### Minor Changes

- 0e3b55c: `EventProcessor.on` and `on()` (for groups) accept an array of handlers that run in series on one subscription. The first failure stops the chain and fails the message.

### Patch Changes

- Updated dependencies [97d9ca5]
- Updated dependencies [dd9f92e]
  - @millstream/core@0.3.0
