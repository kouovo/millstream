import type { Metadata } from '@millstream/core';
import { formatIssues, validateWith } from './issues.js';
import type { InferInput, InferOutput, StandardSchemaIssue, StandardSchemaV1 } from './standard-schema.js';

/**
 * A named event type. The name is the wire contract: write it explicitly (TS types are
 * erased at runtime and bundlers mangle class names), version it (`order.placed.v1`),
 * and never change it once published.
 */
export interface EventDefinition<TPayload, TInput = TPayload> {
  readonly name: string;
  readonly schema?: StandardSchemaV1<TInput, TPayload>;
  /** Build an item for `EventBus.publishMany`. */
  of(payload: TInput, opts?: PublishOptions): EventItem<TPayload, TInput>;
  /** Type carrier only — never set at runtime. */
  readonly _types?: { readonly payload: TPayload; readonly input: TInput };
}

export type AnyEventDefinition = EventDefinition<any, any>;
/** What handlers receive (schema output). */
export type EventPayload<E extends AnyEventDefinition> = NonNullable<E['_types']>['payload'];
/** What publishers pass (schema input). */
export type EventInput<E extends AnyEventDefinition> = NonNullable<E['_types']>['input'];

export interface PublishOptions {
  /** Message uuid (default: random). Use a stable one to make re-publishing idempotent downstream. */
  uuid?: string;
  metadata?: Metadata;
}

export interface EventItem<TPayload = unknown, TInput = TPayload> extends PublishOptions {
  event: EventDefinition<TPayload, TInput>;
  payload: TInput;
}

/** Event with compile-time payload type only (no runtime validation). */
export function defineEvent<TPayload>(name: string): EventDefinition<TPayload>;
/** Event validated with a Standard Schema (zod, valibot, arktype, …) on publish and on consume. */
export function defineEvent<S extends StandardSchemaV1>(
  name: string,
  schema: S,
): EventDefinition<InferOutput<S>, InferInput<S>>;
export function defineEvent(name: string, schema?: StandardSchemaV1): AnyEventDefinition {
  if (!name.trim()) throw new Error('event name must not be empty');
  const def: AnyEventDefinition = {
    name,
    ...(schema ? { schema } : {}),
    of: (payload, opts) => ({ ...opts, event: def, payload }),
  };
  return Object.freeze(def);
}

export class InvalidEventError extends Error {
  constructor(
    readonly eventName: string,
    readonly issues: ReadonlyArray<StandardSchemaIssue>,
  ) {
    super(`invalid "${eventName}" event: ${formatIssues(issues)}`);
    this.name = 'InvalidEventError';
  }
}

/** Validate (and transform) a value with the event's schema; passthrough without one. */
export function parseEvent<TPayload>(event: EventDefinition<TPayload, any>, value: unknown): Promise<TPayload> {
  return validateWith(event.schema, value, (issues) => new InvalidEventError(event.name, issues));
}
