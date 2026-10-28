# rateguard (Go)

A client for [`rateguard-sidecar`](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar):
one global rate limit across a fleet, from Go. Standard library only.

```sh
go get github.com/donmiro/rateguard/sdk/go
```

```go
limiter := rateguard.New(rateguard.Options{}) // http://127.0.0.1:7947, or $RATEGUARD_SIDECAR_URL

d, err := limiter.CheckIn(r.Context(), "pro", "tenant:42")
if err != nil {
	log.Printf("rateguard: %v", err) // the sidecar did not answer; d follows OnError
}
if !d.Allowed {
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(d.RetryAfter.Seconds()))))
	w.WriteHeader(http.StatusTooManyRequests)
	return
}
```

| Option | Default | Meaning |
|---|---|---|
| `URL` | `$RATEGUARD_SIDECAR_URL` or `http://127.0.0.1:7947` | The sidecar |
| `Timeout` | `50 * time.Millisecond` | How long to wait for an answer |
| `OnError` | `Allow` | `Allow` or `Deny` the request when the sidecar cannot answer |
| `HTTPClient` | one of its own | To send through |
| `AdminTimeout` | `time.Second` | How long to wait for every call but a check |

A broken limiter should not take the service down with it, hence `Allow` by
default; the error is returned all the same, to log. A key over the
sidecar's 64 KiB limit is the one exception: the sidecar refuses it with a
413 every time, so the client denies it (and returns an error) whatever
`OnError` says, rather than let it past the limit for good. A `Client` is
safe for
concurrent use and keeps connections alive.

## Stats and limits

The rest of the sidecar's API, off the request path; the calls that change
limits need the sidecar started with `--admin`. What each field means is in
the [sidecar's README](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar).

```go
stats, err := limiter.Stats(ctx)                 // Admitted, Denied, MostDenied by name, ...
one, err := limiter.KeyStats(ctx, "pro", "tenant:42") // nil while the key is not busy here

err = limiter.SetClasses(ctx, map[string]rateguard.Limit{
	"free": rateguard.PerSecond(10),
	"pro":  rateguard.PerMinute(60000).WithBurst(50),
})
err = limiter.SetLimit(ctx, rateguard.PerSecond(100)) // the default class
err = limiter.SetKeyLimits(ctx, []rateguard.KeyLimit{
	rateguard.OwnLimit("pro", "tenant:42", rateguard.PerMinute(90000)),
	rateguard.FollowClass("", "tenant:7", "pro"),
})
err = limiter.SetShadows(ctx, map[string]rateguard.Limit{"free": rateguard.PerSecond(5)})
err = limiter.SetDryRun(ctx, false)
```

Each `Set` call replaces what was there: every class, every key's
exception, every shadow; `nil` clears them. These calls wait
`AdminTimeout` and return a `*rateguard.SidecarError` when the sidecar does
not carry them out, with its reason and `Status`, the HTTP status, or 0
when it did not answer; they never follow `OnError`.

Tests: `go test ./...`.
