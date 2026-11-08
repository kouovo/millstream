import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    // Run tests against sources; CI additionally runs the contract suite on built dist with `node --test`.
    alias: [
      {
        find: /^@millstream\/pg\/kysely$/,
        replacement: fileURLToPath(new URL('./packages/pg/src/kysely.ts', import.meta.url)),
      },
      {
        find: /^@millstream\/(.*)$/,
        replacement: fileURLToPath(new URL('./packages/$1/src/index.ts', import.meta.url)),
      },
    ],
  },
  test: {
    include: ['packages/*/test/**/*.test.ts'],
    testTimeout: 20_000,
    hookTimeout: 20_000,
    fileParallelism: false,
  },
});
