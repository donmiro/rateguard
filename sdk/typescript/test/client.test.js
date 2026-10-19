// The client against a fake sidecar: what it sends, what it makes of the
// answers, and what it does when the sidecar is not there.

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { Client } from "../index.js";

// Answers /v1/check with what `answer` returns for the request body.
async function fake(t, answer, { status = 200, delay = 0 } = {}) {
  const seen = [];
  const peers = new Set();
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      const parsed = JSON.parse(body);
      seen.push({ path: req.url, body: parsed });
      peers.add(req.socket.remotePort);
      setTimeout(() => {
        res.writeHead(status, { "content-type": "application/json" });
        res.end(JSON.stringify(answer(parsed)));
      }, delay);
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  t.after(() => server.closeAllConnections());
  return { url: `http://127.0.0.1:${server.address().port}`, seen, peers };
}

const allow = () => ({ allowed: true });

// For the tests that are not about the timeout. The first fetch of the
// process loads undici, inside the default 50 ms: on a busy CI runner
// that alone can take longer.
const ROOMY = 5000;

test("an allowed check", async (t) => {
  const sidecar = await fake(t, allow);
  const decision = await new Client({ url: sidecar.url, timeoutMs: ROOMY }).check("tenant:42");
  assert.deepEqual(decision, { allowed: true, retryAfterMs: 0 });
  assert.deepEqual(sidecar.seen, [{ path: "/v1/check", body: { key: "tenant:42" } }]);
});

test("a denied check says when to retry", async (t) => {
  const sidecar = await fake(t, () => ({ allowed: false, retry_after_ms: 1500 }));
  const decision = await new Client({ url: sidecar.url, timeoutMs: ROOMY }).check("k");
  assert.deepEqual(decision, { allowed: false, retryAfterMs: 1500 });
});

test("a check in a class sends the class", async (t) => {
  const sidecar = await fake(t, allow);
  await new Client({ url: sidecar.url, timeoutMs: ROOMY }).check("tenant:42", { class: "pro" });
  assert.deepEqual(sidecar.seen[0].body, { key: "tenant:42", class: "pro" });
});

test("one connection serves many checks in turn", async (t) => {
  const sidecar = await fake(t, allow);
  const client = new Client({ url: sidecar.url, timeoutMs: ROOMY });
  for (let i = 0; i < 50; i++) {
    assert.equal((await client.check("k")).allowed, true);
  }
  assert.equal(sidecar.seen.length, 50);
  // Kept alive, not one a check: undici hands a socket back to its pool a
  // moment after the response, so checks in turn alternate over two.
  assert.ok(sidecar.peers.size <= 2, `${sidecar.peers.size} connections for 50 checks`);
});

test("a slow sidecar is given up on and the request allowed", async (t) => {
  const sidecar = await fake(t, () => ({ allowed: false }), { delay: 500 });
  const started = Date.now();
  const decision = await new Client({ url: sidecar.url, timeoutMs: 50 }).check("k");
  assert.equal(decision.allowed, true, "onError allow by default");
  assert.ok(decision.error, "and says why");
  assert.ok(Date.now() - started < 400);
});

test("onError deny denies when the sidecar is gone", async () => {
  const client = new Client({ url: "http://127.0.0.1:1", onError: "deny" });
  const decision = await client.check("k");
  assert.equal(decision.allowed, false);
  assert.ok(decision.error);
});

test("a sidecar that answers badly counts as an error", async (t) => {
  const sidecar = await fake(t, () => ({ error: "no key" }), { status: 400 });
  assert.equal((await new Client({ url: sidecar.url, timeoutMs: ROOMY }).check("k")).allowed, true);
  const strict = new Client({ url: sidecar.url, timeoutMs: ROOMY, onError: "deny" });
  assert.equal((await strict.check("k")).allowed, false);
});

test("an unknown onError is refused", () => {
  assert.throws(() => new Client({ onError: "maybe" }), /onError/);
});

test("the url comes from the environment by default", async (t) => {
  const sidecar = await fake(t, allow);
  process.env.RATEGUARD_SIDECAR_URL = sidecar.url;
  t.after(() => delete process.env.RATEGUARD_SIDECAR_URL);
  assert.equal((await new Client({ timeoutMs: ROOMY }).check("k")).allowed, true);
  assert.equal(sidecar.seen.length, 1);
});

test("a path in the url is kept", async (t) => {
  const sidecar = await fake(t, allow);
  await new Client({ url: `${sidecar.url}/rateguard/`, timeoutMs: ROOMY }).check("k");
  assert.equal(sidecar.seen[0].path, "/rateguard/v1/check");
});
