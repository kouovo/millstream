// Runs the published artifacts (dist) on plain Node with node:test — no TS, no vitest.
import assert from 'node:assert/strict';
import { after, it } from 'node:test';
import { MemoryPubSub, Router, newMessage } from '@millstream/core';
import { instrumentPublisher, instrumentRouter } from '@millstream/otel';
import { context } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { InMemorySpanExporter, SimpleSpanProcessor, TracerProvider } from '@opentelemetry/sdk-trace';

context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
const exporter = new InMemorySpanExporter();
const tracerProvider = new TracerProvider({ spanProcessors: [new SimpleSpanProcessor({ exporter })] });
const opts = { tracerProvider, propagator: new W3CTraceContextPropagator(), metrics: false };
after(() => tracerProvider.shutdown());

it('propagates a trace from publisher to consumer (dist)', async () => {
  const bus = new MemoryPubSub();
  const router = instrumentRouter(new Router(), opts);
  let resolve;
  const handled = new Promise((r) => (resolve = r));
  router.addConsumerHandler({ name: 'h', subscribeTopic: 't', subscriber: bus, handler: () => resolve() });
  void router.run();
  await router.running;
  await instrumentPublisher(bus, opts).publish('t', newMessage('x'));
  await handled;
  await router.close();
  const [send, process] = ['send t', 'process t'].map((n) => exporter.getFinishedSpans().find((s) => s.name === n));
  assert.ok(send && process);
  assert.equal(process.parentSpanContext?.spanId, send.spanContext().spanId);
});
