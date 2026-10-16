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
default. One keep-alive connection per thread; `client.close()`, or `with
Client() as limiter:`, closes the current thread's.

Tests: `PYTHONPATH=src python3 -m unittest discover -s tests`.
