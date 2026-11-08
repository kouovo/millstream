# @millstream/test-kit

Watermill-style universal contract tests for `Publisher` / `Subscriber` implementations. They are
framework-agnostic: pass `describe` / `it` / `afterEach` from vitest, `node:test`, jest…

```ts
import { afterEach, describe, it } from 'vitest';
import { pubSubContract } from '@millstream/test-kit';

pubSubContract({ describe, it, afterEach }, 'MyPubSub', MyPubSub.features, async () => ({
  publisher,
  subscriber: (group) => new MySubscriber({ group }),
  topic: async () => `t_${crypto.randomUUID()}`,
  close: async () => {},
}));
```

Cases not covered by the declared `Features` (`ordered`, `persistent`, `consumerGroups`, …) are skipped.
Also exports the helpers `collect`, `msgs`, `payloads`, `eventually`, `nextWithTimeout`.
