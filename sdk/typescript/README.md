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

A broken limiter should not take the service down with it, hence `allow` by
default; `decision.error` then says why. An empty `url` or
`$RATEGUARD_SIDECAR_URL` counts as unset.

A key over the sidecar's 64 KiB limit is the one exception: the sidecar
refuses it with a 413 every time, so the client denies it (`allowed: false`,
with an `error`) whatever `onError` says, rather than let it past the limit
for good.

Connections are kept alive by
Node's `fetch`.

Tests: `npm test`.
