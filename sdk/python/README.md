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

Tests: `PYTHONPATH=src python3 -m unittest discover -s tests`.
