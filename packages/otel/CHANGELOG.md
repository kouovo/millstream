# @millstream/otel

## 0.2.0

### Minor Changes

- 7cd1d83: Add `@millstream/otel`: OpenTelemetry producer/consumer spans following the messaging semantic conventions, W3C trace context propagation through message metadata, and `messaging.*` metrics (`instrumentRouter`, `instrumentPublisher`, `otelMiddleware`, `otelPublisherDecorator`).
  
  `@millstream/forwarder`: the envelope message now carries a copy of the original message's metadata, so the relay's middlewares (e.g. tracing) see the business trace context.
