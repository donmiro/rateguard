# rateguard-client (TypeScript, Node.js)

A client for [`rateguard-sidecar`](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar):
one global rate limit across a fleet, from Node.js 18 or newer. No
dependencies; types included.

```sh
npm install rateguard-client
```

```ts
import { Client } from "rateguard-client";

const limiter = new Client(); // http://127.0.0.1:7947, or $RATEGUARD_SIDECAR_URL

const decision = await limiter.check("tenant:42", { class: "pro" });
if (!decision.allowed) {
  res.setHeader("Retry-After", Math.ceil(decision.retryAfterMs / 1000));
  return res.status(429).end();
}
```

| Option | Default | Meaning |
|---|---|---|
| `url` | `$RATEGUARD_SIDECAR_URL` or `http://127.0.0.1:7947` | The sidecar |
| `timeoutMs` | `50` | How long to wait for an answer |
| `onError` | `"allow"` | `"allow"` or `"deny"` the request when the sidecar cannot answer |
| `adminTimeoutMs` | `1000` | How long to wait for every call but `check` |

A broken limiter should not take the service down with it, hence `allow` by
default; `decision.error` then says why. An empty `url` or
`$RATEGUARD_SIDECAR_URL` counts as unset.

A key over the sidecar's 64 KiB limit is the one exception: the sidecar
refuses it with a 413 every time, so the client denies it (`allowed: false`,
with an `error`) whatever `onError` says, rather than let it past the limit
for good.

Connections are kept alive by
Node's `fetch`.

## Stats and limits

The rest of the sidecar's API, off the request path. Limits and answers are
the JSON of the [sidecar's README](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar),
typed in `index.d.ts`; the calls that change limits need the sidecar
started with `--admin`.

```ts
const stats = await limiter.stats(); // admitted, denied, most_denied by name, ...
const one = await limiter.keyStats("tenant:42", { class: "pro" }); // null while the key is not busy here

await limiter.setClasses({ free: { per_second: 10 }, pro: { per_minute: 60000, burst: 50 } });
await limiter.setLimit({ per_second: 100 }); // the default class
await limiter.setKeyLimits([
  { class: "pro", key: "tenant:42", limit: { per_minute: 90000 } },
  { key: "tenant:7", as_class: "pro" },
]);
await limiter.setShadows({ free: { per_second: 5 } }); // counts what it would deny
await limiter.setDryRun(false);
```

Each `set` call replaces what was there: every class, every key's
exception, every shadow. These calls wait `adminTimeoutMs` and reject with a
`SidecarError` when the sidecar does not carry them out, with its reason
and `status`, the HTTP status, or `undefined` when it did not answer; they
never follow `onError`.

Tests: `npm test`.
