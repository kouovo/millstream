import assert from 'node:assert/strict';
import type { Features, Publisher, Subscriber } from '@millstream/core';
import { collect, delay, msgs, nextWithTimeout, payloads } from './helpers.js';

/** The few test-runner functions the suite needs — vitest, `node:test`, jest and `bun:test` all fit. */
export interface TestApi {
  describe: (name: string, fn: () => void) => unknown;
  it: (name: string, fn: () => Promise<void>) => unknown;
  afterEach: (fn: () => Promise<void>) => unknown;
}

export interface ContractFixture {
  publisher: Publisher;
  /** A new subscriber; the same `group` must share progress where consumer groups are supported. */
  subscriber(group?: string): Subscriber;
  /** A fresh topic (tables created, for SQL backends). */
  topic(): Promise<string>;
  close(): Promise<void>;
}

/**
 * Watermill-style universal Pub/Sub tests: every implementation runs the same suite,
 * and cases its declared `Features` don't cover are skipped.
 *
 * ```ts
 * import { describe, it, afterEach } from 'vitest';
 * pubSubContract({ describe, it, afterEach }, 'MyPubSub', MyPubSub.features, async () => ({ ... }));
 * ```
 */
export function pubSubContract(
  t: TestApi,
  name: string,
  features: Features,
  setup: () => Promise<ContractFixture>,
): void {
  t.describe(`${name} — pub/sub contract`, () => {
    const controllers: AbortController[] = [];
    let fx: ContractFixture | undefined;
    const fixture = async () => (fx = await setup());
    const signal = () => {
      const c = new AbortController();
      controllers.push(c);
      return c.signal;
    };
    const itIf = (cond: boolean, title: string, fn: () => Promise<void>) => {
      if (cond) t.it(title, fn);
    };

    t.afterEach(async () => {
      for (const c of controllers.splice(0)) c.abort();
      await fx?.close();
      fx = undefined;
    });

    t.it('delivers published messages', async () => {
      const f = await fixture();
      const topic = await f.topic();
      const sub = f.subscriber().subscribe(topic, signal());
      await f.publisher.publish(topic, ...msgs(10));
      const got = await collect(sub, 10);
      assert.deepEqual(new Set(payloads(got)), new Set(payloads(msgs(10))));
    });

    itIf(features.ordered, 'delivers in order and holds the next message until ack', async () => {
      const f = await fixture();
      const topic = await f.topic();
      const it = f.subscriber().subscribe(topic, signal())[Symbol.asyncIterator]();
      await f.publisher.publish(topic, ...msgs(5));
      const first = await nextWithTimeout(it, 5000);
      assert.ok(first !== 'timeout' && !first.done, 'no first message');
      assert.equal(first.value.payload.toString(), 'm-0');
      // Not acked yet: the subscription must not move on (Watermill TestNoAck).
      const pending = it.next();
      assert.equal(await Promise.race([pending, delay(300).then(() => 'blocked')]), 'blocked');
      first.value.ack();
      const second = await pending;
      assert.ok(!second.done);
      assert.equal(second.value.payload.toString(), 'm-1');
      second.value.ack();
      assert.deepEqual(payloads(await collect(it, 3)), ['m-2', 'm-3', 'm-4']);
    });

    t.it('redelivers a nacked message as a fresh copy', async () => {
      const f = await fixture();
      const topic = await f.topic();
      const sub = f.subscriber().subscribe(topic, signal());
      const [orig] = msgs(1);
      await f.publisher.publish(topic, orig!);
      let nacks = 0;
      const got = await collect(sub, 3, {
        onMessage: (m) => void (nacks++ < 2 ? m.nack() : m.ack()),
      });
      assert.ok(got.every((m) => m.uuid === orig!.uuid && m.payload.equals(orig!.payload)));
      assert.notEqual(got[0], got[1]);
    });

    t.it('does not lose messages under concurrent publishing', async () => {
      const f = await fixture();
      const topic = await f.topic();
      const sub = f.subscriber().subscribe(topic, signal());
      const batches = Array.from({ length: 10 }, (_, b) => msgs(10, `b${b}`));
      await Promise.all(batches.map((b) => f.publisher.publish(topic, ...b)));
      const got = await collect(sub, 100, { timeoutMs: 15_000 });
      assert.equal(new Set(got.map((m) => m.uuid)).size, 100);
    });

    t.it('stops delivering after the subscription is aborted', async () => {
      const f = await fixture();
      const topic = await f.topic();
      const c = new AbortController();
      controllers.push(c);
      const it = f.subscriber().subscribe(topic, c.signal)[Symbol.asyncIterator]();
      await f.publisher.publish(topic, ...msgs(1));
      const r = await nextWithTimeout(it, 5000);
      assert.ok(r !== 'timeout' && !r.done, 'no message');
      r.value.ack();
      c.abort();
      await f.publisher.publish(topic, ...msgs(1, 'late'));
      const after = await nextWithTimeout(it, 3000);
      assert.ok(after !== 'timeout' && after.done, 'iterator should end after abort');
    });

    t.it('a message held while aborting is not acked', async () => {
      const f = await fixture();
      const topic = await f.topic();
      const c = new AbortController();
      controllers.push(c);
      const it = f.subscriber().subscribe(topic, c.signal)[Symbol.asyncIterator]();
      await f.publisher.publish(topic, ...msgs(1));
      const r = await nextWithTimeout(it, 5000);
      assert.ok(r !== 'timeout' && !r.done, 'no message');
      c.abort(); // consumer still holds the message: the subscription must release it promptly
      assert.ok((await nextWithTimeout(it, 3000)) !== 'timeout', 'subscription did not stop');
      if (features.persistent) {
        const again = await collect(f.subscriber().subscribe(topic, signal()), 1);
        assert.equal(again[0]!.uuid, r.value.uuid);
      }
    });

    itIf(features.newSubscriberReceivesOldMessages, 'a later subscriber receives older messages', async () => {
      const f = await fixture();
      const topic = await f.topic();
      await f.publisher.publish(topic, ...msgs(3));
      const got = await collect(f.subscriber().subscribe(topic, signal()), 3);
      assert.equal(got.length, 3);
    });

    itIf(features.persistent, 'resumes where it left off after a restart', async () => {
      const f = await fixture();
      const topic = await f.topic();
      await f.publisher.publish(topic, ...msgs(6));
      const c1 = new AbortController();
      const first = await collect(f.subscriber('g').subscribe(topic, c1.signal), 3);
      c1.abort();
      const second = await collect(f.subscriber('g').subscribe(topic, signal()), 3);
      const all = [...payloads(first), ...payloads(second)];
      assert.equal(new Set(all).size, 6);
      if (features.ordered) assert.deepEqual(all, payloads(msgs(6)));
    });

    itIf(features.consumerGroups, 'each consumer group gets every message', async () => {
      const f = await fixture();
      const topic = await f.topic();
      await f.publisher.publish(topic, ...msgs(5));
      const [a, b] = await Promise.all([
        collect(f.subscriber('a').subscribe(topic, signal()), 5),
        collect(f.subscriber('b').subscribe(topic, signal()), 5),
      ]);
      assert.deepEqual(payloads(a), payloads(msgs(5)));
      assert.deepEqual(payloads(b), payloads(msgs(5)));
    });
  });
}
