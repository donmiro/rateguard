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
| GET | `/v1/stats` | | what `Guard::stats` tells, as JSON, with the keys denied most by name; see [Stats](#stats) |
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
| `--gossip-secret-file` | none | Gossip secrets, one per line in base64, at most two: the first signs, either verifies. Reread every 10 s; see [Signed gossip](#signed-gossip) |
| `--gossip-auth` | `required` with a secret file, else `off` | `off`, `transitional` or `required` |
| `--key-salt-file` | none | A salt for the hashes of keys, its bytes as they are, at least 16; read once, at start |

Switches take `true`, `1`, `yes` or `on` as variables, and their opposites.

The API has no authentication: keep it on loopback, and `--admin` only
where you mean it.

On SIGTERM the sidecar leaves the cluster, so that its peers take its share
at once, then goes on answering for `--drain`, ten seconds by default:
Kubernetes signals every container of the pod at once, and the service next
to it is still draining its requests. They are checked at the floor
meanwhile, rather than let through by a client that finds nobody to ask;
`/healthz` answers `503`. A second signal stops it at once. Keep `--drain`
under the pod's `terminationGracePeriodSeconds`.

## Stats

`/v1/stats` is `Guard::stats` as JSON: the fields of the
[`rateguard` README](https://github.com/donmiro/rateguard#observability),
`gossip_auth` as `"off"`, `"transitional"` or `"required"`, and
`rejected_datagrams` as `{"unsigned": 0, "bad_tag": 0, "malformed": 0}`.
Each entry of `most_denied` has its `key_hash`, as a string, and
`denied_per_sec`; and, when the sidecar knows them, the `key` itself and its
`class`, `""` for the default one:

```json
{"key_hash": "8270364711458143018", "denied_per_sec": 42.5, "key": "203.0.113.7", "class": "free"}
```

The sidecar remembers a key's string when it denies it, and only then, in a
table of the last 1024; a key over 256 bytes is kept cut, with
`"key_truncated": true`. An entry has no `key` and `class` when the key was
denied before the sidecar restarted or has been pushed out of the table, and
in a dry run, where nothing is denied.

Keys are what your service sends, and may be personal data: IP addresses,
user names. They are served on the API's address only, on loopback, never
on `--metrics-listen` and never in the metrics.

## Signed gossip

Gossip is unsigned by default, and anyone who reaches the UDP port can join
the cluster. A secret shared by every instance signs it; a key salt keeps a
client who picks the keys from choosing one that shares another's limit.
What each protects, and what it does not, is in the
[`rateguard` README](https://github.com/donmiro/rateguard#security).

Both come from files, so that they show neither in `ps` nor in the
environment: in Kubernetes, a Secret mounted as a volume.

```sh
openssl rand -base64 32 > gossip-secrets
openssl rand 16 > key-salt
kubectl create secret generic api-rateguard \
  --from-file=gossip-secrets --from-file=key-salt
```

`--gossip-secret-file` holds one secret per line in base64, each of 32
bytes or more, at most two; blank lines are skipped. The first signs,
either verifies. The sidecar rereads it every 10 s, and Kubernetes updates
a mounted Secret in place a minute or two after it changes, so a rotation
needs no restart of the pods; mount the Secret as a directory, since a file
mounted with `subPath` is never updated. A file that
cannot be read or is refused leaves the secrets in force: the sidecar says
so on stderr, by line number and never by content, and counts it in
`rateguard_gossip_secret_reload_errors_total`, a counter with no `guard`
label, unlike the rest. At start, the same file stops the sidecar.

`--gossip-auth` is `required` with a secret file and `off` without. Under
`off` a secret file given is still read at start, which a refused one
stops, and reread, though nothing is signed. The mode is not reread: each
step below is a rolling update of the variable.

To turn signing on in a running cluster, each step on every pod before the
next:

1. Every sidecar on 0.6, no secret: on the wire, 0.5.
2. The secret, with `RATEGUARD_GOSSIP_AUTH=transitional`.
3. `RATEGUARD_GOSSIP_AUTH=required`.

To turn it off, the same backwards: `transitional` on every pod first, then
`off`. Never `required` straight to `off`: while that rolls out, the pods
already `off` send unsigned, the ones still `required` refuse it as
`unsigned`, and the cluster splits.

To rotate from secret A to B, change the Secret's `gossip-secrets` to `A`
and `B` on two lines, then `B` and `A`, then `B`, each once every pod has
reread the one before. Nothing restarts, and `unsigned` and `bad_tag` in
`rejected_datagrams` do not grow while all goes well. To undo a rotation,
walk its steps backwards.

`--key-salt-file` is read once, at start, as raw bytes, 16 or more. Set it
when the cluster is created; change it only by restarting every pod at
once, never by a rolling update: instances whose salts differ do not know
each other's keys, and admit up to N times the limit between them.

## In Kubernetes

[`examples/kubernetes.yaml`](examples/kubernetes.yaml): the sidecar as a
second container of each pod, a headless service for the seeds, the gossip
secret and the key salt mounted from a Secret, which you create first with
the commands in its header, probes and Prometheus on `--metrics-listen`.
The image:

```sh
docker build -f rateguard-sidecar/Dockerfile -t rateguard-sidecar:0.6.0 .
```

## License

MIT OR Apache-2.0, as `rateguard`.
