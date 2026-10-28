// The client against a real `rateguard-sidecar --limit 2 --burst 2`; see
// sdk/live/run.sh. Skipped unless RATEGUARD_LIVE_URL is set.

import { test } from "node:test";
import assert from "node:assert/strict";
import { Client } from "../index.js";

// The first fetch of the process loads undici: room for that.
const ROOMY = 5000;

const live = process.env.RATEGUARD_LIVE_URL;
const liveTest = live ? test : test.skip;

liveTest("live: the burst is allowed and the next denied", async () => {
  const client = new Client({ url: live, timeoutMs: ROOMY });
  const key = `js-live-${process.pid}`;
  assert.equal((await client.check(key)).allowed, true);
  assert.equal((await client.check(key)).allowed, true);
  const third = await client.check(key);
  assert.equal(third.allowed, false);
  assert.ok(third.retryAfterMs > 0);
  assert.equal(third.error, undefined);
});

liveTest("live: a class with a lone surrogate is checked, not refused", async () => {
  const client = new Client({ url: live, timeoutMs: ROOMY });
  const key = `js-live-surrogate-${process.pid}`;
  const decisions = [];
  for (let i = 0; i < 3; i++) decisions.push(await client.check(key, { class: "pro\uD800" }));
  assert.deepEqual(decisions.map((d) => d.allowed), [true, true, false]);
  assert.ok(decisions.every((d) => d.error === undefined));
});

// What `read` gives once `done` holds of it: the sidecar's counts and a new
// key's state follow its background task, a tick behind.
async function eventually(read, done) {
  const deadline = Date.now() + 2000;
  for (;;) {
    const value = await read();
    if (done(value) || Date.now() > deadline) return value;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

// With --admin; each change is put back as it was.
liveTest("live: a key limit lets its key past the default", async (t) => {
  const client = new Client({ url: live, timeoutMs: ROOMY, adminTimeoutMs: ROOMY });
  const key = `js-live-own-${process.pid}`;
  await client.setKeyLimits([{ key, limit: { per_second: 1, burst: 4 } }]);
  t.after(() => client.setKeyLimits([]));
  const allowed = [];
  for (let i = 0; i < 5; i++) allowed.push((await client.check(key)).allowed);
  assert.deepEqual(allowed, [true, true, true, true, false]);
  const stats = await eventually(() => client.keyStats(key), (s) => s !== null);
  assert.deepEqual(stats.limit, { per_second: 1, burst: 4 });
  assert.equal(stats.shadow, null);
  assert.equal(await client.keyStats(`js-live-never-${process.pid}`), null);
});

liveTest("live: a shadow counts what it would deny", async (t) => {
  const client = new Client({ url: live, timeoutMs: ROOMY, adminTimeoutMs: ROOMY });
  const key = `js-live-shadow-${process.pid}`;
  const before = (await client.stats()).shadow_denied;
  await client.setShadows({ "": { per_second: 1, burst: 1 } });
  t.after(() => client.setShadows({}));
  assert.equal((await client.check(key)).allowed, true);
  assert.equal((await client.check(key)).allowed, true);
  const after = await eventually(() => client.stats(), (s) => s.shadow_denied > before);
  assert.ok(after.shadow_denied > before);
});

liveTest("live: a refused limit rejects and a good one is taken", async () => {
  const client = new Client({ url: live, timeoutMs: ROOMY, adminTimeoutMs: ROOMY });
  await assert.rejects(client.setLimit({ per_second: 0 }), (error) => error.status === 400);
  await client.setLimit({ per_second: 2, burst: 2 });
  await client.setClasses({});
  await client.setDryRun(false);
  assert.equal((await client.stats()).dry_run, false);
});
