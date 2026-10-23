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
