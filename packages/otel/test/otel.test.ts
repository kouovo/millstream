import { MemoryPubSub, type Message, Router, newMessage } from '@millstream/core';
import { ForwarderPublisher, addForwarder } from '@millstream/forwarder';
import { SpanKind, SpanStatusCode, context, trace } from '@opentelemetry/api';
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { MeterProvider, MetricReader } from '@opentelemetry/sdk-metrics';
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor, TracerProvider } from '@opentelemetry/sdk-trace';
import { eventually } from '@millstream/test-kit';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import {
  ATTR,
  type OtelMiddlewareOptions,
  extractTraceContext,
  injectTraceContext,
  instrumentPublisher,
  instrumentRouter,
} from '../src/index.js';

class TestReader extends MetricReader {
  protected async onForceFlush(): Promise<void> {}
  protected async onShutdown(): Promise<void> {}
}

const exporter = new InMemorySpanExporter();
const tracerProvider = new TracerProvider({ spanProcessors: [new SimpleSpanProcessor({ exporter })] });
const reader = new TestReader();
const meterProvider = new MeterProvider({ readers: [reader] });
const propagator = new W3CTraceContextPropagator();
const opts: OtelMiddlewareOptions = { tracerProvider, meterProvider, propagator, system: 'memory' };
const tracer = tracerProvider.getTracer('test');
const contextManager = new AsyncLocalStorageContextManager();

beforeAll(() => {
  context.setGlobalContextManager(contextManager.enable());
});
afterAll(async () => {
  context.disable();
  await tracerProvider.shutdown();
  await meterProvider.shutdown();
});

const routers: Router[] = [];
const newRouter = () => {
  const r = instrumentRouter(new Router({ closeTimeoutMs: 1000 }), opts);
  routers.push(r);
  return r;
};
afterEach(async () => {
  await Promise.all(routers.splice(0).map((r) => r.close()));
  exporter.reset();
});

const spans = () => exporter.getFinishedSpans();
const span = (name: string) => {
  const s = spans().find((x) => x.name === name);
  if (!s) throw new Error(`no span "${name}" in [${spans().map((x) => x.name)}]`);
  return s;
};
const parentOf = (s: ReadableSpan) => s.parentSpanContext?.spanId;
const inRequest = <T>(fn: () => Promise<T>) => tracer.startActiveSpan('request', (s) => fn().finally(() => s.end()));

describe('@millstream/otel', () => {
  it('producer → consumer: one trace, the consumer span is a child of the send span', async () => {
    const bus = new MemoryPubSub();
    const router = newRouter();
    let active: string | undefined;
    router.addConsumerHandler({
      name: 'h',
      subscribeTopic: 'orders',
      subscriber: bus,
      handler: () => void (active = trace.getActiveSpan()?.spanContext().spanId),
    });
    void router.run();
    await router.running;

    const pub = instrumentPublisher(bus, opts);
    const msg = newMessage('x');
    await inRequest(() => pub.publish('orders', msg));
    await eventually(() => spans().some((s) => s.name === 'process orders'));

    const request = span('request');
    const send = span('send orders');
    const process = span('process orders');
    expect(send.kind).toBe(SpanKind.PRODUCER);
    expect(process.kind).toBe(SpanKind.CONSUMER);
    expect(parentOf(send)).toBe(request.spanContext().spanId);
    expect(parentOf(process)).toBe(send.spanContext().spanId);
    expect(new Set(spans().map((s) => s.spanContext().traceId)).size).toBe(1);
    expect(active).toBe(process.spanContext().spanId); // handler runs inside the consumer span
    expect(msg.metadata.traceparent).toContain(send.spanContext().spanId);
    expect(send.attributes).toMatchObject({
      [ATTR.SYSTEM]: 'memory',
      [ATTR.OPERATION_TYPE]: 'send',
      [ATTR.DESTINATION]: 'orders',
      [ATTR.MESSAGE_ID]: msg.uuid,
    });
    expect(process.attributes).toMatchObject({
      [ATTR.OPERATION_TYPE]: 'process',
      [ATTR.DESTINATION]: 'orders',
      [ATTR.SUBSCRIPTION]: 'h',
      [ATTR.MESSAGE_ID]: msg.uuid,
    });
    await bus.close();
  });

  it('handler output is published as a child of the consumer span', async () => {
    const bus = new MemoryPubSub();
    const router = newRouter();
    router.addHandler({
      name: 'upper',
      subscribeTopic: 'in',
      subscriber: bus,
      publishTopic: 'out',
      publisher: bus,
      handler: (m) => [newMessage(m.payload.toString().toUpperCase())],
    });
    router.addConsumerHandler({ name: 'sink', subscribeTopic: 'out', subscriber: bus, handler: () => {} });
    void router.run();
    await router.running;

    await instrumentPublisher(bus, opts).publish('in', newMessage('a'));
    await eventually(() => spans().some((s) => s.name === 'process out'));

    expect(parentOf(span('process in'))).toBe(span('send in').spanContext().spanId);
    expect(parentOf(span('send out'))).toBe(span('process in').spanContext().spanId);
    expect(parentOf(span('process out'))).toBe(span('send out').spanContext().spanId);
    expect(new Set(spans().map((s) => s.spanContext().traceId)).size).toBe(1);
    await bus.close();
  });

  it('outbox: the trace continues through the forwarder relay', async () => {
    const outbox = new MemoryPubSub();
    const broker = new MemoryPubSub();
    const router = newRouter();
    addForwarder(router, {
      subscriber: outbox,
      publisher: instrumentPublisher(broker, opts),
      forwarderTopic: 'outbox',
    });
    router.addConsumerHandler({ name: 'consumer', subscribeTopic: 'orders', subscriber: broker, handler: () => {} });
    void router.run();
    await router.running;

    const business = instrumentPublisher(new ForwarderPublisher(outbox, 'outbox'), opts);
    await inRequest(() => business.publish('orders', newMessage('o1')));
    await eventually(() => spans().some((s) => s.name === 'process orders'));

    const [businessSend, relaySend] = spans()
      .filter((s) => s.name === 'send orders')
      .toSorted((a, b) => (a.startTime[0] - b.startTime[0]) * 1e9 + (a.startTime[1] - b.startTime[1]));
    expect(parentOf(businessSend!)).toBe(span('request').spanContext().spanId);
    expect(parentOf(span('process outbox'))).toBe(businessSend!.spanContext().spanId);
    expect(parentOf(relaySend!)).toBe(span('process outbox').spanContext().spanId);
    expect(parentOf(span('process orders'))).toBe(relaySend!.spanContext().spanId);
    expect(new Set(spans().map((s) => s.spanContext().traceId)).size).toBe(1);
    await outbox.close();
    await broker.close();
  });

  it('a failing handler marks the consumer span as an error', async () => {
    const bus = new MemoryPubSub();
    const router = newRouter();
    let calls = 0;
    router.addConsumerHandler({
      name: 'boom',
      subscribeTopic: 'jobs',
      subscriber: bus,
      handler: () => {
        if (++calls === 1) throw new TypeError('bad input');
      },
    });
    void router.run();
    await router.running;
    await bus.publish('jobs', newMessage('x'));
    await eventually(() => spans().filter((s) => s.name === 'process jobs').length === 2);

    const [failed, ok] = spans().filter((s) => s.name === 'process jobs');
    expect(failed!.status.code).toBe(SpanStatusCode.ERROR);
    expect(failed!.attributes[ATTR.ERROR_TYPE]).toBe('TypeError');
    expect(failed!.events.some((e) => e.name === 'exception')).toBe(true);
    expect(ok!.status.code).toBe(SpanStatusCode.UNSET);
    await bus.close();
  });

  it("consumerParent: 'link' starts a new trace linked to the producer", async () => {
    const bus = new MemoryPubSub();
    const router = new Router({ closeTimeoutMs: 1000 });
    routers.push(instrumentRouter(router, { ...opts, consumerParent: 'link' }));
    router.addConsumerHandler({ name: 'l', subscribeTopic: 'evt', subscriber: bus, handler: () => {} });
    void router.run();
    await router.running;
    await instrumentPublisher(bus, opts).publish('evt', newMessage('x'));
    await eventually(() => spans().some((s) => s.name === 'process evt'));

    const send = span('send evt');
    const process = span('process evt');
    expect(process.spanContext().traceId).not.toBe(send.spanContext().traceId);
    expect(process.links.map((l) => l.context.spanId)).toEqual([send.spanContext().spanId]);
    await bus.close();
  });

  it('batch publish: one span, every message carries its context', async () => {
    const bus = new MemoryPubSub();
    const messages = [newMessage('a'), newMessage('b'), newMessage('c')];
    await instrumentPublisher(bus, opts).publish('batch', ...messages);
    const send = span('send batch');
    expect(send.attributes[ATTR.BATCH_COUNT]).toBe(3);
    for (const m of messages) expect(m.metadata.traceparent).toContain(send.spanContext().spanId);
    await bus.close();
  });

  it('a failing publish is recorded and rethrown', async () => {
    const pub = instrumentPublisher(
      {
        publish: async () => {
          throw new Error('broker down');
        },
        close: async () => {},
      },
      opts,
    );
    await expect(pub.publish('x', newMessage('m'))).rejects.toThrow('broker down');
    expect(span('send x').status.code).toBe(SpanStatusCode.ERROR);
  });

  it('records messaging metrics', async () => {
    const bus = new MemoryPubSub();
    const router = newRouter();
    router.addConsumerHandler({ name: 'm', subscribeTopic: 'metered', subscriber: bus, handler: () => {} });
    void router.run();
    await router.running;
    await instrumentPublisher(bus, opts).publish('metered', newMessage('1'), newMessage('2'));
    await eventually(() => spans().filter((s) => s.name === 'process metered').length === 2);

    const { resourceMetrics } = await reader.collect();
    const all = resourceMetrics.scopeMetrics.flatMap((s) => s.metrics);
    const points = (name: string) =>
      all
        .filter((m) => m.descriptor.name === name)
        .flatMap((m) => m.dataPoints as Array<{ attributes: Record<string, unknown>; value: any }>)
        .filter((p) => p.attributes[ATTR.DESTINATION] === 'metered');
    expect(points('messaging.client.sent.messages')[0]!.value).toBe(2);
    expect(points('messaging.client.operation.duration')[0]!.value.count).toBe(1);
    const processed = points('messaging.process.duration')[0]!;
    expect(processed.value.count).toBe(2);
    expect(processed.attributes[ATTR.SUBSCRIPTION]).toBe('m');
    await bus.close();
  });

  it('injectTraceContext / extractTraceContext round-trip', async () => {
    let carried: Message | undefined;
    await inRequest(async () => {
      carried = newMessage('x');
      injectTraceContext(carried, context.active(), opts);
    });
    const sc = trace.getSpanContext(extractTraceContext(carried!, opts));
    expect(sc?.spanId).toBe(span('request').spanContext().spanId);
    expect(sc?.isRemote).toBe(true);
  });
});
