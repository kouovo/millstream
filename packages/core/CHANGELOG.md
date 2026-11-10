# @millstream/core

## 0.4.0

No changes in this release.

## 0.3.0

### Patch Changes

- 97d9ca5: All `@millstream/*` packages now share one version and are released together. `@millstream/core`: the README links to `@millstream/otel` for tracing and metrics.
- dd9f92e: Packages are now built with tsdown (Rolldown): each entry point ships as one bundled ESM file with a rolled-up `.d.ts`, instead of one file per source module. Public exports are unchanged.
