---
'@millstream/core': patch
---

Packages are now built with tsdown (Rolldown): each entry point ships as one bundled ESM file with a rolled-up `.d.ts`, instead of one file per source module. Public exports are unchanged.
