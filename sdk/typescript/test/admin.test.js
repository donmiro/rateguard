// The calls off the request path, stats and the admin routes, against a
// fake sidecar.

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { Client, SidecarError } from "../index.js";

// Answers every request with `status` and `reply`, and keeps what it was
// sent: method, path, and the body as JSON.
async function fake(t, { status = 204, reply } = {}) {
  const seen = [];
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      seen.push({ method: req.method, path: req.url, body: body ? JSON.parse(body) : undefined });
      const data = reply === undefined ? "" : typeof reply === "string" ? reply : JSON.stringify(reply);
      res.writeHead(status, { "content-type": "application/json" });
      res.end(data);
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  t.after(() => server.closeAllConnections());
  return { url: `http://127.0.0.1:${server.address().port}`, seen };
}

// The first fetch of the process loads undici: room for that.
const ROOMY = 5000;

test("each change is a PUT of its JSON", async (t) => {
  const sidecar = await fake(t);
  const client = new Client({ url: sidecar.url, adminTimeoutMs: ROOMY });
  const pro = { per_minute: 60000, burst: 50 };
  await client.setClasses({ pro });
  await client.setLimit({ per_second: 100 });
  await client.setKeyLimits([{ key: "tenant:7", as_class: "pro" }]);
  await client.setShadows({ "": { per_second: 10 }, pro: null });
  await client.setDryRun(true);
  assert.deepEqual(sidecar.seen, [
    { method: "PUT", path: "/v1/classes", body: { pro } },
    { method: "PUT", path: "/v1/limit", body: { per_second: 100 } },
    { method: "PUT", path: "/v1/key_limits", body: [{ key: "tenant:7", as_class: "pro" }] },
    { method: "PUT", path: "/v1/shadows", body: { "": { per_second: 10 }, pro: null } },
    { method: "PUT", path: "/v1/dry_run", body: { on: true } },
  ]);
});

test("stats are the sidecar's JSON", async (t) => {
  const sidecar = await fake(t, { status: 200, reply: { admitted: 5, most_denied: [] } });
  const stats = await new Client({ url: sidecar.url, adminTimeoutMs: ROOMY }).stats();
  assert.deepEqual(stats, { admitted: 5, most_denied: [] });
  assert.deepEqual(sidecar.seen, [{ method: "GET", path: "/v1/stats", body: undefined }]);
});

test("key stats ask for the key and its class, encoded", async (t) => {
  const sidecar = await fake(t, { status: 200, reply: { share: 1.5 } });
  const client = new Client({ url: sidecar.url, adminTimeoutMs: ROOMY });
  assert.deepEqual(await client.keyStats("tenant 42&x", { class: "pro" }), { share: 1.5 });
  await client.keyStats("k");
  assert.equal(sidecar.seen[0].path, "/v1/keys?key=tenant+42%26x&class=pro");
  assert.equal(sidecar.seen[1].path, "/v1/keys?key=k");
});

test("a key the sidecar does not track is null", async (t) => {
  const sidecar = await fake(t, { status: 404, reply: { error: "this node does not track the key" } });
  assert.equal(await new Client({ url: sidecar.url, adminTimeoutMs: ROOMY }).keyStats("k"), null);
});

test("a refused change rejects with the sidecar's reason", async (t) => {
  const sidecar = await fake(t, { status: 400, reply: { error: "a limit needs one of per_second" } });
  const client = new Client({ url: sidecar.url, adminTimeoutMs: ROOMY });
  await assert.rejects(client.setLimit({ burst: 5 }), (error) => {
    assert.ok(error instanceof SidecarError);
    assert.equal(error.status, 400);
    assert.match(error.message, /400: a limit needs one of per_second/);
    return true;
  });
});

test("a sidecar without --admin says so", async (t) => {
  const sidecar = await fake(t, { status: 404 });
  const client = new Client({ url: sidecar.url, adminTimeoutMs: ROOMY });
  await assert.rejects(client.setDryRun(false), /--admin/);
});

test("a sidecar that is gone rejects with no status", async () => {
  const client = new Client({ url: "http://127.0.0.1:1", adminTimeoutMs: ROOMY });
  await assert.rejects(client.stats(), (error) => error instanceof SidecarError && error.status === undefined);
});

test("a path in the URL is kept", async (t) => {
  const sidecar = await fake(t, { status: 200, reply: {} });
  await new Client({ url: `${sidecar.url}/rateguard/`, adminTimeoutMs: ROOMY }).stats();
  assert.equal(sidecar.seen[0].path, "/rateguard/v1/stats");
});
