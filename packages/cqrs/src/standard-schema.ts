/**
 * The Standard Schema v1 interface (https://standardschema.dev), inlined as the spec
 * intends so this package has no dependency. Implemented by zod ≥ 3.24, valibot ≥ 1,
 * arktype ≥ 2, effect/Schema, …
 */
export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly '~standard': StandardSchemaProps<Input, Output>;
}

export interface StandardSchemaProps<Input = unknown, Output = Input> {
  readonly version: 1;
  readonly vendor: string;
  readonly validate: (value: unknown) => StandardSchemaResult<Output> | Promise<StandardSchemaResult<Output>>;
  readonly types?: { readonly input: Input; readonly output: Output } | undefined;
}

export type StandardSchemaResult<Output> =
  | { readonly value: Output; readonly issues?: undefined }
  | { readonly issues: ReadonlyArray<StandardSchemaIssue> };

export interface StandardSchemaIssue {
  readonly message: string;
  readonly path?: ReadonlyArray<PropertyKey | { readonly key: PropertyKey }> | undefined;
}

export type InferInput<S extends StandardSchemaV1> = NonNullable<S['~standard']['types']>['input'];
export type InferOutput<S extends StandardSchemaV1> = NonNullable<S['~standard']['types']>['output'];
