import {
  type DecoratorInfo,
  type Message,
  type Middleware,
  type Publisher,
  type PublisherDecorator,
  type Router,
  handlerContext,
} from '@millstream/core';
import {
  type Attributes,
  type Context,
  type Counter,
  type Histogram,
  type Link,
  type Meter,
  type MeterProvider,
  ROOT_CONTEXT,
  type Span,
  SpanKind,
  SpanStatusCode,
  type TextMapPropagator,
  type Tracer,
  type TracerProvider,
  context,
  defaultTextMapGetter,
  defaultTextMapSetter,
  isSpanContextValid,
  metrics,
  propagation,
  trace,
} from '@opentelemetry/api';

const INSTRUMENTATION_NAME = '@millstream/otel';

/** OpenTelemetry messaging semantic-convention attribute keys (kept local: no semconv dependency). */
export const ATTR = {
  SYSTEM: 'messaging.system',
  OPERATION_TYPE: 'messaging.operation.type',
  OPERATION_NAME: 'messaging.operation.name',
  DESTINATION: 'messaging.destination.name',
  SUBSCRIPTION: 'messaging.destination.subscription.name',
  MESSAGE_ID: 'messaging.message.id',
  BODY_SIZE: 'messaging.message.body.size',
  BATCH_COUNT: 'messaging.batch.message_count',
  ERROR_TYPE: 'error.type',
} as const;

export interface OtelOptions {
  /** Default: the global tracer provider. */
  tracerProvider?: TracerProvider;
  /** Default: the global meter provider. */
  meterProvider?: MeterProvider;
  /** Default: the global propagator (`propagation` API, usually W3C trace context + baggage). */
  propagator?: TextMapPropagator;
  /** `messaging.system` attribute, e.g. 'kafka', 'postgresql'. Default 'millstream'. */
  system?: string;
  /** Record metrics too. Default true. */
  metrics?: boolean;
}

export interface OtelMiddlewareOptions extends OtelOptions {
  /**
   * How a consumer span relates to the producer's trace context found in the message metadata:
   * - `'parent'` (default): child of the producer span — one trace end to end.
   * - `'link'`: a new trace with a span link to the producer (for long-lived / fan-in flows).
   */
  consumerParent?: 'parent' | 'link';
}

// ---------------------------------------------------------------- propagation

function inject(ctx: Context, msg: Message, opts: OtelOptions): void {
  if (opts.propagator) opts.propagator.inject(ctx, msg.metadata, defaultTextMapSetter);
  else propagation.inject(ctx, msg.metadata, defaultTextMapSetter);
}

function extract(ctx: Context, msg: Message, opts: OtelOptions): Context {
  return opts.propagator
    ? opts.propagator.extract(ctx, msg.metadata, defaultTextMapGetter)
    : propagation.extract(ctx, msg.metadata, defaultTextMapGetter);
}

/** Write the trace context (default: the active one) into the message metadata. */
export function injectTraceContext(msg: Message, ctx: Context = context.active(), opts: OtelOptions = {}): void {
  inject(ctx, msg, opts);
}

/** The trace context carried in the message metadata (a root context if there is none). */
export function extractTraceContext(msg: Message, opts: OtelOptions = {}): Context {
  return extract(ROOT_CONTEXT, msg, opts);
}

/**
 * Router handlers return produced messages; the Router publishes them *after* the handler
 * (and so after the consumer span's `context.with`) returned. The middleware remembers the
 * processing context per produced message so the publish span still becomes its child.
 */
const producedIn = new WeakMap<Message, Context>();

// ---------------------------------------------------------------- instruments

interface Instruments {
  tracer: Tracer;
  publishDuration?: Histogram;
  sent?: Counter;
  processDuration?: Histogram;
}

const meters = new WeakMap<Meter, Omit<Instruments, 'tracer'>>();

function instruments(opts: OtelOptions): Instruments {
  const tracer = (opts.tracerProvider ?? trace.getTracerProvider()).getTracer(INSTRUMENTATION_NAME);
  if (opts.metrics === false) return { tracer };
  const meter = (opts.meterProvider ?? metrics.getMeterProvider()).getMeter(INSTRUMENTATION_NAME);
  let m = meters.get(meter);
  if (!m) {
    m = {
      publishDuration: meter.createHistogram('messaging.client.operation.duration', {
        description: 'Duration of publishing messages.',
        unit: 's',
      }),
      sent: meter.createCounter('messaging.client.sent.messages', {
        description: 'Number of messages a producer attempted to send.',
        unit: '{message}',
      }),
      processDuration: meter.createHistogram('messaging.process.duration', {
        description: 'Duration of processing a message by a handler.',
        unit: 's',
      }),
    };
    meters.set(meter, m);
  }
  return { tracer, ...m };
}

function errorType(error: unknown): string {
  return error instanceof Error ? error.name || error.constructor.name : '_OTHER';
}

function fail(span: Span, error: unknown): string {
  const type = errorType(error);
  span.recordException(error instanceof Error ? error : String(error));
  span.setStatus({ code: SpanStatusCode.ERROR, message: error instanceof Error ? error.message : String(error) });
  span.setAttribute(ATTR.ERROR_TYPE, type);
  return type;
}

const seconds = (start: number) => (performance.now() - start) / 1000;

// ---------------------------------------------------------------- producer

/**
 * Parent of a publish span:
 * 1. the consumer span that produced the message (Router handler output),
 * 2. else the active span (publishing from your own code or inside a handler),
 * 3. else the trace context the message already carries (relaying, e.g. the outbox forwarder).
 */
function publishParent(msg: Message, opts: OtelOptions): Context {
  const produced = producedIn.get(msg);
  if (produced) return produced;
  const active = context.active();
  if (trace.getSpan(active)) return active;
  return extract(active, msg, opts);
}

/**
 * Wrap a publisher: a PRODUCER span `send <topic>` per `publish` call, the span's context
 * injected into every message's metadata, plus `messaging.client.*` metrics.
 *
 * Use it for publishers outside the Router (EventBus, outbox `ForwarderPublisher`, …);
 * Router handlers get it through `otelPublisherDecorator` / `instrumentRouter`.
 */
export function instrumentPublisher(inner: Publisher, opts: OtelOptions = {}): Publisher {
  const ins = instruments(opts);
  const system = opts.system ?? 'millstream';
  return {
    async publish(topic, ...messages) {
      if (!messages.length) return inner.publish(topic);
      const parent = publishParent(messages[0]!, opts);
      const attributes: Attributes = {
        [ATTR.SYSTEM]: system,
        [ATTR.OPERATION_TYPE]: 'send',
        [ATTR.OPERATION_NAME]: 'publish',
        [ATTR.DESTINATION]: topic,
        ...(messages.length === 1
          ? { [ATTR.MESSAGE_ID]: messages[0]!.uuid, [ATTR.BODY_SIZE]: messages[0]!.payload.length }
          : { [ATTR.BATCH_COUNT]: messages.length }),
      };
      const span = ins.tracer.startSpan(`send ${topic}`, { kind: SpanKind.PRODUCER, attributes }, parent);
      const ctx = trace.setSpan(parent, span);
      for (const m of messages) inject(ctx, m, opts);
      const start = performance.now();
      let error: string | undefined;
      try {
        await context.with(ctx, () => inner.publish(topic, ...messages));
      } catch (e) {
        error = fail(span, e);
        throw e;
      } finally {
        span.end();
        const metricAttrs: Attributes = {
          [ATTR.SYSTEM]: system,
          [ATTR.OPERATION_NAME]: 'publish',
          [ATTR.DESTINATION]: topic,
          ...(error ? { [ATTR.ERROR_TYPE]: error } : {}),
        };
        ins.publishDuration?.record(seconds(start), metricAttrs);
        ins.sent?.add(messages.length, metricAttrs);
      }
    },
    close: () => inner.close(),
  };
}

/** Router publisher decorator: every handler's output publisher is instrumented. */
export function otelPublisherDecorator(opts: OtelOptions = {}): PublisherDecorator {
  return (publisher: Publisher, _info: DecoratorInfo) => instrumentPublisher(publisher, opts);
}

// ---------------------------------------------------------------- consumer

/**
 * Router middleware: a CONSUMER span `process <topic>` per message, continuing the trace
 * context found in the message metadata, plus the `messaging.process.duration` metric.
 * The span is active while the handler runs, so its own publishes / DB calls nest under it.
 *
 * Put it first (outermost) so one span covers retries and the other middlewares.
 */
export function otelMiddleware(opts: OtelMiddlewareOptions = {}): Middleware {
  const ins = instruments(opts);
  const system = opts.system ?? 'millstream';
  const mode = opts.consumerParent ?? 'parent';
  return (next) => async (msg) => {
    const hc = handlerContext();
    const topic = hc?.subscribeTopic ?? 'unknown';
    const remote = extract(ROOT_CONTEXT, msg, opts);
    let parent = remote;
    const links: Link[] = [];
    if (mode === 'link') {
      parent = context.active();
      const sc = trace.getSpanContext(remote);
      if (sc && isSpanContextValid(sc)) links.push({ context: sc });
    }
    const attributes: Attributes = {
      [ATTR.SYSTEM]: system,
      [ATTR.OPERATION_TYPE]: 'process',
      [ATTR.OPERATION_NAME]: 'process',
      [ATTR.DESTINATION]: topic,
      [ATTR.MESSAGE_ID]: msg.uuid,
      [ATTR.BODY_SIZE]: msg.payload.length,
      ...(hc?.handlerName ? { [ATTR.SUBSCRIPTION]: hc.handlerName } : {}),
    };
    const span = ins.tracer.startSpan(`process ${topic}`, { kind: SpanKind.CONSUMER, attributes, links }, parent);
    const ctx = trace.setSpan(parent, span);
    const start = performance.now();
    let error: string | undefined;
    try {
      const produced = await context.with(ctx, () => next(msg));
      if (produced) for (const m of produced) producedIn.set(m, ctx);
      return produced;
    } catch (e) {
      error = fail(span, e);
      throw e;
    } finally {
      span.end();
      ins.processDuration?.record(seconds(start), {
        [ATTR.SYSTEM]: system,
        [ATTR.OPERATION_NAME]: 'process',
        [ATTR.DESTINATION]: topic,
        ...(hc?.handlerName ? { [ATTR.SUBSCRIPTION]: hc.handlerName } : {}),
        ...(error ? { [ATTR.ERROR_TYPE]: error } : {}),
      });
    }
  };
}

/**
 * `router.use(otelMiddleware(opts))` + `router.addPublisherDecorators(otelPublisherDecorator(opts))`.
 * Call it before other `router.use(...)` so the consumer span is the outermost middleware.
 */
export function instrumentRouter<R extends Router>(router: R, opts: OtelMiddlewareOptions = {}): R {
  router.use(otelMiddleware(opts));
  router.addPublisherDecorators(otelPublisherDecorator(opts));
  return router;
}
