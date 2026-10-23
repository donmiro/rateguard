// The client against a fake sidecar: what it sends, what it makes of the
// answers, and what it does when the sidecar is not there.

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { Client } from "../index.js";

// Answers /v1/check with what `answer` returns for the request body.
async function fake(t, answer, { status = 200, delay = 0, raw } = {}) {
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
        res.writeHead(status, { "content-type": raw === undefined ? "application/json" : "text/plain" });
        res.end(raw === undefined ? JSON.stringify(answer(parsed)) : raw);
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

test("a key over the sidecar's limit is denied whatever onError says", async (t) => {
  const sidecar = await fake(t, () => ({ error: "the body is too large" }), { status: 413 });
  const decision = await new Client({ url: sidecar.url, timeoutMs: ROOMY, onError: "allow" }).check("k");
  assert.equal(decision.allowed, false);
  assert.equal(decision.retryAfterMs, 0);
  assert.match(decision.error, /64 KiB/);
});

test("a 413 with a body that is not JSON is still a 413", async (t) => {
  const sidecar = await fake(t, allow, { status: 413, raw: "Payload Too Large" });
  const decision = await new Client({ url: sidecar.url, timeoutMs: ROOMY }).check("k");
  assert.equal(decision.allowed, false);
  assert.match(decision.error, /64 KiB/);
  const other = await fake(t, allow, { status: 502, raw: "<html>bad gateway</html>" });
  const bad = await new Client({ url: other.url, timeoutMs: ROOMY }).check("k");
  assert.equal(bad.allowed, true);
  assert.equal(bad.error, "the sidecar answered 502");
});

// The timeout may fire after the headers, while the body is still coming:
// the body stream has errored then, and cancelling it rejects. That is no
// reason for check() to throw instead of deciding.
test("a body that errored is still a decision", async (t) => {
  const errored = (status) => {
    const body = new ReadableStream({
      start(controller) {
        controller.error(new Error("The operation was aborted due to timeout"));
      },
    });
    return new Response(body, { status });
  };
  // Put back by hand: Node 18's t.mock restores a method mocked twice to
  // the first mock, not to fetch.
  const original = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = original;
  });
  let status = 413;
  globalThis.fetch = async () => errored(status);
  const tooLarge = await new Client({ timeoutMs: ROOMY }).check("k");
  assert.equal(tooLarge.allowed, false);
  assert.match(tooLarge.error, /64 KiB/);

  status = 503;
  const failed = await new Client({ timeoutMs: ROOMY, onError: "deny" }).check("k");
  assert.deepEqual(failed, { allowed: false, retryAfterMs: 0, error: "the sidecar answered 503" });
});

test("a server error follows onError and says why", async (t) => {
  const sidecar = await fake(t, () => ({ error: "boom" }), { status: 500 });
  const allowed = await new Client({ url: sidecar.url, timeoutMs: ROOMY }).check("k");
  assert.equal(allowed.allowed, true);
  assert.match(allowed.error, /500/);
  const denied = await new Client({ url: sidecar.url, timeoutMs: ROOMY, onError: "deny" }).check("k");
  assert.equal(denied.allowed, false);
  assert.match(denied.error, /500/);
});

test("an empty url is unset", async (t) => {
  const sidecar = await fake(t, allow);
  // The default is 127.0.0.1:7947: nothing listens there, so the check
  // fails, and says it tried that address rather than throwing on "".
  assert.doesNotThrow(() => new Client({ url: "" }));
  process.env.RATEGUARD_SIDECAR_URL = "";
  t.after(() => delete process.env.RATEGUARD_SIDECAR_URL);
  assert.doesNotThrow(() => new Client({}));
  process.env.RATEGUARD_SIDECAR_URL = sidecar.url;
  assert.equal((await new Client({ url: "", timeoutMs: ROOMY }).check("k")).allowed, true);
  assert.equal(sidecar.seen.length, 1, "an empty url falls back to the environment");
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
