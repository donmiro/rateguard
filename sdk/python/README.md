# rateguard-client (Python)

A client for [`rateguard-sidecar`](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar):
one global rate limit across a fleet, from Python. Standard library only,
Python 3.9 or newer.

```sh
pip install rateguard-client
```

```python
from rateguard_client import Client

limiter = Client()  # http://127.0.0.1:7947, or $RATEGUARD_SIDECAR_URL

decision = limiter.check("tenant:42", cls="pro")
if not decision.allowed:
    return Response(status=429, headers={"Retry-After": str(math.ceil(decision.retry_after))})
```

| Option | Default | Meaning |
|---|---|---|
| `url` | `$RATEGUARD_SIDECAR_URL` or `http://127.0.0.1:7947` | The sidecar |
| `timeout` | `0.05` | Seconds to wait for an answer |
| `on_error` | `"allow"` | `"allow"` or `"deny"` the request when the sidecar cannot answer |
| `admin_timeout` | `1.0` | Seconds to wait for every call but `check` |

A broken limiter should not take the service down with it, hence `allow` by
default. Whenever the sidecar gives no decision, `decision.error` says why
(it is `None` otherwise, and does not take part in comparing decisions). An
empty `url` or `$RATEGUARD_SIDECAR_URL` counts as unset.

A key over the sidecar's 64 KiB limit is the one exception: the sidecar
refuses it with a 413 every time, so the client denies it
(`allowed=False`, `retry_after=0`, with an `error`) whatever `on_error` says,
rather than let it past the limit for good.

One keep-alive connection per thread, closed when the thread ends;
`client.close()`, or `with Client() as limiter:`, closes every thread's.

## Stats and limits

The rest of the sidecar's API, off the request path. Limits and answers are
the JSON of the [sidecar's README](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar),
as dicts; the calls that change limits need the sidecar started with
`--admin`.

```python
stats = limiter.stats()                  # admitted, denied, most_denied by name, ...
one = limiter.key_stats("tenant:42", cls="pro")  # None while the key is not busy here

limiter.set_classes({"free": {"per_second": 10}, "pro": {"per_minute": 60000, "burst": 50}})
limiter.set_limit({"per_second": 100})  # the default class
limiter.set_key_limits([
    {"class": "pro", "key": "tenant:42", "limit": {"per_minute": 90000}},
    {"key": "tenant:7", "as_class": "pro"},
])
limiter.set_shadows({"free": {"per_second": 5}})  # counts what it would deny
limiter.set_dry_run(False)
```

Each `set_` call replaces what was there: every class, every key's
exception, every shadow. These calls wait `admin_timeout` seconds, one by
default, and raise `SidecarError` when the sidecar does not carry them out,
with its reason and `status`, the HTTP status, or `None` when it did not
answer; they never follow `on_error`.

Tests: `PYTHONPATH=src python3 -m unittest discover -s tests`.
