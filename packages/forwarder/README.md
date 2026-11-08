# @millstream/forwarder

Outbox forwarder for [millstream](../../README.md).

- `ForwarderPublisher(inner, forwarderTopic)`: `publish(anyTopic, msg)` wraps the message in an envelope and publishes
  it to a single outbox topic. Wrap a transactional `PgPublisher` with it.
- `addForwarder(router, { subscriber, publisher })`: relays outbox → destination topic on any `Publisher`. The original
  uuid and metadata are preserved. Delivery is at-least-once, so downstream should dedupe by uuid.
