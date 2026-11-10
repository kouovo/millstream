# @millstream/forwarder

## 0.4.0

### Patch Changes

- @millstream/core@0.4.0

## 0.3.0

### Patch Changes

- Updated dependencies [97d9ca5]
- Updated dependencies [dd9f92e]
  - @millstream/core@0.3.0

## 0.2.0

### Patch Changes

- 7cd1d83: Add `@millstream/otel`: OpenTelemetry producer/consumer spans following the messaging semantic conventions, W3C trace context propagation through message metadata, and `messaging.*` metrics (`instrumentRouter`, `instrumentPublisher`, `otelMiddleware`, `otelPublisherDecorator`).
  
  `@millstream/forwarder`: the envelope message now carries a copy of the original message's metadata, so the relay's middlewares (e.g. tracing) see the business trace context.
