import type { StandardSchemaIssue, StandardSchemaV1 } from './standard-schema.js';

export function formatIssues(issues: ReadonlyArray<StandardSchemaIssue>): string {
  return issues
    .map((issue) => {
      const path = issue.path?.map((p) => String(typeof p === 'object' ? p.key : p)).join('.');
      return path ? `${path}: ${issue.message}` : issue.message;
    })
    .join('; ');
}

/** Validate (and transform) with a Standard Schema; `onIssues` builds the error to throw. */
export async function validateWith<T>(
  schema: StandardSchemaV1<unknown, T> | undefined,
  value: unknown,
  onIssues: (issues: ReadonlyArray<StandardSchemaIssue>) => Error,
): Promise<T> {
  if (!schema) return value as T;
  const result = await schema['~standard'].validate(value);
  if (result.issues) throw onIssues(result.issues);
  return result.value;
}
