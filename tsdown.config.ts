// Shared build config for every package: run as `tsdown -c ../../tsdown.config.ts` from a package dir.
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { defineConfig } from 'tsdown';

type ExportTarget = { default: string };
const pkg = JSON.parse(readFileSync('package.json', 'utf8')) as {
  exports: Record<string, ExportTarget>;
};

// package.json `exports` is the single source of truth: `./dist/foo.js` ← `src/foo.ts`.
const entry = Object.values(pkg.exports).map((t) => t.default.replace(/^\.\/dist\/(.+)\.js$/, 'src/$1.ts'));

export default defineConfig({
  cwd: process.cwd(), // entries/outDir relative to the package, not this file
  entry,
  format: 'esm',
  platform: 'node',
  target: 'node22',
  fixedExtension: false, // keep `.js` / `.d.ts` (packages are `"type": "module"`)
  sourcemap: true,
  tsconfig: resolve('tsconfig.build.json'), // per-package: declaration + declarationMap
  dts: true,
  publint: true,
});
