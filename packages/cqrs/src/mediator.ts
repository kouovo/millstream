import { formatIssues, validateWith } from './issues.js';
import type { InferInput, InferOutput, StandardSchemaIssue, StandardSchemaV1 } from './standard-schema.js';

/**
 * In-process, synchronous commands and queries: `execute` / `query` call the single
 * registered handler directly and resolve with its result. No transport, no persistence,
 * no retries. For reliable side effects, publish events from the command handler with an
 * outbox `EventBus` inside the same transaction (see the `transaction` middleware example).
 */

export type RequestKind = 'command' | 'query';

export interface RequestDefinition<K extends RequestKind, TPayload, TInput, TResult> {
  readonly kind: K;
  readonly name: string;
  readonly schema?: StandardSchemaV1<TInput, TPayload>;
  /** Declare the result type (types only, same object at runtime). */
  returns<R>(): RequestDefinition<K, TPayload, TInput, R>;
  /** Type carrier only — never set at runtime. */
  readonly _types?: { readonly payload: TPayload; readonly input: TInput; readonly result: TResult };
}

export type CommandDefinition<TPayload, TInput = TPayload, TResult = void> = RequestDefinition<
  'command',
  TPayload,
  TInput,
  TResult
>;
export type QueryDefinition<TPayload, TInput = TPayload, TResult = unknown> = RequestDefinition<
  'query',
  TPayload,
  TInput,
  TResult
>;

export type AnyRequestDefinition = RequestDefinition<RequestKind, any, any, any>;
export type AnyCommandDefinition = RequestDefinition<'command', any, any, any>;
export type AnyQueryDefinition = RequestDefinition<'query', any, any, any>;
/** What handlers receive (schema output). */
export type RequestPayload<D extends AnyRequestDefinition> = NonNullable<D['_types']>['payload'];
/** What callers pass (schema input). */
export type RequestInput<D extends AnyRequestDefinition> = NonNullable<D['_types']>['input'];
export type RequestResult<D extends AnyRequestDefinition> = NonNullable<D['_types']>['result'];

function define(kind: RequestKind, name: string, schema?: StandardSchemaV1): AnyRequestDefinition {
  if (!name.trim()) throw new Error(`${kind} name must not be empty`);
  const def: AnyRequestDefinition = {
    kind,
    name,
    ...(schema ? { schema } : {}),
    returns: () => def,
  };
  return Object.freeze(def);
}

/** Command with compile-time types only: `defineCommand<Input, Result>('todo.complete')`. */
export function defineCommand<TInput, TResult = void>(name: string): CommandDefinition<TInput, TInput, TResult>;
/** Command validated with a Standard Schema; add `.returns<Result>()` for a result type. */
export function defineCommand<S extends StandardSchemaV1>(
  name: string,
  schema: S,
): CommandDefinition<InferOutput<S>, InferInput<S>, void>;
export function defineCommand(name: string, schema?: StandardSchemaV1): AnyCommandDefinition {
  return define('command', name, schema) as AnyCommandDefinition;
}

/** Query with compile-time types only: `defineQuery<Input, Result>('todo.get')`. */
export function defineQuery<TInput, TResult>(name: string): QueryDefinition<TInput, TInput, TResult>;
/** Query validated with a Standard Schema; add `.returns<Result>()` for a result type. */
export function defineQuery<S extends StandardSchemaV1>(
  name: string,
  schema: S,
): QueryDefinition<InferOutput<S>, InferInput<S>, unknown>;
export function defineQuery(name: string, schema?: StandardSchemaV1): AnyQueryDefinition {
  return define('query', name, schema) as AnyQueryDefinition;
}

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface RequestContext {
  readonly kind: RequestKind;
  readonly name: string;
  readonly definition: AnyRequestDefinition;
  /** Validated input (schema output). */
  readonly input: unknown;
  readonly signal?: AbortSignal;
}

export type RequestHandler<D extends AnyRequestDefinition> = (
  input: RequestPayload<D>,
  ctx: RequestContext,
) => RequestResult<D> | Promise<RequestResult<D>>;

/**
 * Wraps every handler call, first middleware outermost. Runs after input validation.
 *
 * ```ts
 * const transaction: MediatorMiddleware = (ctx, next) =>
 *   ctx.kind === 'command' ? withTransaction(db, () => next()) : next();
 * ```
 */
export type MediatorMiddleware = (ctx: RequestContext, next: () => Promise<unknown>) => Promise<unknown>;

export interface MediatorOptions {
  middlewares?: MediatorMiddleware[];
  /** Validate inputs with the definition's schema. Default true. */
  validate?: boolean;
}

export class InvalidRequestError extends Error {
  constructor(
    readonly kind: RequestKind,
    readonly requestName: string,
    readonly issues: ReadonlyArray<StandardSchemaIssue>,
  ) {
    super(`invalid "${requestName}" ${kind}: ${formatIssues(issues)}`);
    this.name = 'InvalidRequestError';
  }
}

export class HandlerNotFoundError extends Error {
  constructor(
    readonly kind: RequestKind,
    readonly requestName: string,
  ) {
    super(`no handler registered for ${kind} "${requestName}"`);
    this.name = 'HandlerNotFoundError';
  }
}

interface Registration {
  definition: AnyRequestDefinition;
  handler: RequestHandler<any>;
}

/** Exactly one handler per command / query name. */
export class Mediator {
  readonly #handlers = new Map<string, Registration>();
  readonly #middlewares: MediatorMiddleware[];
  readonly #validate: boolean;

  constructor(opts: MediatorOptions = {}) {
    this.#middlewares = [...(opts.middlewares ?? [])];
    this.#validate = opts.validate ?? true;
  }

  /** Register the handler; throws if `definition.name` already has one. Returns `this` for chaining. */
  handle<D extends AnyRequestDefinition>(definition: D, handler: RequestHandler<D>): this {
    const existing = this.#handlers.get(definition.name);
    if (existing) {
      throw new Error(
        `${definition.kind} "${definition.name}" already has a handler` +
          (existing.definition.kind === definition.kind ? '' : ` (registered as a ${existing.definition.kind})`),
      );
    }
    this.#handlers.set(definition.name, { definition, handler });
    return this;
  }

  has(definition: AnyRequestDefinition): boolean {
    return this.#handlers.get(definition.name)?.definition.kind === definition.kind;
  }

  execute<D extends AnyCommandDefinition>(
    command: D,
    input: RequestInput<D>,
    opts?: RequestOptions,
  ): Promise<RequestResult<D>> {
    return this.#dispatch('command', command, input, opts);
  }

  query<D extends AnyQueryDefinition>(
    query: D,
    input: RequestInput<D>,
    opts?: RequestOptions,
  ): Promise<RequestResult<D>> {
    return this.#dispatch('query', query, input, opts);
  }

  async #dispatch(
    kind: RequestKind,
    definition: AnyRequestDefinition,
    raw: unknown,
    opts: RequestOptions = {},
  ): Promise<any> {
    const reg = this.#handlers.get(definition.name);
    // `kind` is checked at runtime too: a query passed to `execute` (JS callers, `as any`) is not found.
    if (!reg || definition.kind !== kind || reg.definition.kind !== kind) {
      throw new HandlerNotFoundError(kind, definition.name);
    }
    opts.signal?.throwIfAborted();
    const input = this.#validate
      ? await validateWith(
          definition.schema,
          raw,
          (issues) => new InvalidRequestError(definition.kind, definition.name, issues),
        )
      : raw;
    const ctx: RequestContext = {
      kind: definition.kind,
      name: definition.name,
      definition,
      input,
      ...(opts.signal ? { signal: opts.signal } : {}),
    };
    const call = (i: number): Promise<unknown> => {
      const mw = this.#middlewares[i];
      if (!mw) return Promise.resolve(reg.handler(input, ctx));
      return mw(ctx, () => call(i + 1));
    };
    return call(0);
  }
}
