# rateguard-sidecar

[`rateguard`](https://crates.io/crates/rateguard) for services in any
language: one global rate limit across a fleet, enforced locally, over a
small HTTP API on loopback. Run one next to every instance of your service;
the sidecars find each other and share the limit, as `rateguard` does in a
Rust process.

```sh
cargo install rateguard-sidecar

rateguard-sidecar --limit 1000 \
  --bind 0.0.0.0:7946 --advertise 10.0.0.3:7946 \
  --seeds 10.0.0.1:7946,10.0.0.2:7946 \
  --class pro=5000/250

curl -s -X POST -d '{"key": "tenant:42", "class": "pro"}' http://127.0.0.1:7947/v1/check
# {"allowed":true}
```

Clients for Python, Go and TypeScript are in
[`sdk/`](https://github.com/donmiro/rateguard/tree/main/sdk); any HTTP
client will do.

## API

| Method | Path | Body | Answer |
|---|---|---|---|
| POST | `/v1/check` | `{"key": "tenant:42", "class": "pro"}`, `class` optional | `200 {"allowed": true}` or `200 {"allowed": false, "retry_after_ms": 120}` |
| GET | `/v1/stats` | | what `Guard::stats` tells, as JSON |
| GET | `/healthz` | | `200` while the node runs, `503` once it has left |
| GET | `/metrics` | | Prometheus metrics |
| PUT | `/v1/classes` | `{"pro": {"per_second": 1000, "burst": 50}}` | `204`; replaces every named class; `--admin` only |
| PUT | `/v1/limit` | `{"per_second": 100, "burst": 5}` | `204`; the default class; `--admin` only |
| PUT | `/v1/dry_run` | `{"on": true}` | `204`; `--admin` only |

A decision is always a `200`: the `429` is your service's answer to its
client. A request the sidecar cannot read is a `400`. `burst` is optional,
a twentieth of the rate by default.

## Configuration

Every flag is also a variable, for Kubernetes: `--limit` is
`RATEGUARD_LIMIT`, and so on; `rateguard-sidecar --help` lists them all.

| Flag | Default | Meaning |
|---|---|---|
| `--limit` | required | Requests per second per key in the default class, across the fleet |
| `--burst` | `limit / 20` | Requests one instance may admit back to back |
| `--class name=rate[/burst]` | none | A class of limits; repeated, or comma-separated in `RATEGUARD_CLASSES` |
| `--bind` | `0.0.0.0:7946` | UDP address for gossip |
| `--advertise` | the bind address | Where the other instances reach this one; required with `0.0.0.0`. In Kubernetes, the pod IP |
| `--seeds` | none | Peers to join through, comma-separated; a headless service name will do |
| `--partition-policy` | `hold-down:10s` | `optimistic`, `quorum` or `hold-down:<duration>` |
| `--listen` | `127.0.0.1:7947` | The API |
| `--metrics-listen` | none | `/healthz` and `/metrics` only, for kubelet and Prometheus, which reach the pod's IP |
| `--admin` | off | Serve the routes that change limits |
| `--dry-run` | off | Admit everything, count what would be denied |
| `--drain` | `10s` | How long to go on answering after leaving on SIGTERM |
| `--tracked-keys`, `--hot-keys`, `--max-classes` | as in `rateguard` | |

Switches take `true`, `1`, `yes` or `on` as variables, and their opposites.

The API has no authentication, like the gossip: keep it on loopback, and
`--admin` only where you mean it.

On SIGTERM the sidecar leaves the cluster, so that its peers take its share
at once, then goes on answering for `--drain`, ten seconds by default:
Kubernetes signals every container of the pod at once, and the service next
to it is still draining its requests. They are checked at the floor
meanwhile, rather than let through by a client that finds nobody to ask;
`/healthz` answers `503`. A second signal stops it at once. Keep `--drain`
under the pod's `terminationGracePeriodSeconds`.

## In Kubernetes

[`examples/kubernetes.yaml`](examples/kubernetes.yaml): the sidecar as a
second container of each pod, a headless service for the seeds, probes and
Prometheus on `--metrics-listen`. The image:

```sh
docker build -f rateguard-sidecar/Dockerfile -t rateguard-sidecar:0.5.0 .
```

## License

MIT OR Apache-2.0, as `rateguard`.
