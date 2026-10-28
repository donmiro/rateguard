# rateguard-sidecar

[`rateguard`](https://crates.io/crates/rateguard) for services in any
language: one global rate limit across a fleet, enforced locally, over a
small HTTP API on loopback. Run one next to every instance of your service;
the sidecars find each other and share the limit, as `rateguard` does in a
Rust process.

```sh
cargo install rateguard-sidecar   # or: docker pull ghcr.io/donmiro/rateguard-sidecar:0.7

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
| GET | `/v1/stats` | | what `Guard::stats` tells, as JSON, with the keys denied most, and those a shadow limit would deny most, by name; see [Stats](#stats) |
| GET | `/v1/keys?class=pro&key=tenant:42` | | one key's rates, share, limit and shadow, as JSON; `404` while this node keeps no state for it; loopback only, never on `--metrics-listen`; see [One key](#one-key) |
| GET | `/healthz` | | `200` while the node runs, `503` once it has left |
| GET | `/metrics` | | Prometheus metrics |
| PUT | `/v1/classes` | `{"pro": {"per_second": 1000, "burst": 50}}` | `204`; replaces every named class; `--admin` only |
| PUT | `/v1/limit` | `{"per_second": 100, "burst": 5}` | `204`; the default class; `--admin` only |
| PUT | `/v1/key_limits` | `[{"class": "pro", "key": "tenant:42", "limit": {"per_minute": 90000}}, {"class": "free", "key": "tenant:7", "as_class": "pro"}]` | `204`; replaces every exception; `--admin` only; see [Limits](#limits) |
| PUT | `/v1/shadows` | `{"free": {"per_second": 10}, "": null}` | `204`; replaces every shadow, `""` is the default class; `--admin` only; see [Shadow limits](#shadow-limits) |
| PUT | `/v1/dry_run` | `{"on": true}` | `204`; `--admin` only |

A decision is always a `200`: the `429` is your service's answer to its
client. A request the sidecar cannot read is a `400`.

## Limits

A `/v1/check` body may hold at most 64 KiB (65 536 bytes), the admin
routes' bodies up to axum's 2 MB default; a larger body is
answered `413` with `{"error": "the body is too large"}`.

A limit in a body has exactly one of `per_second`, `per_minute` and
`per_hour`, and an optional `burst`, by default a twentieth of the count:
`{"per_minute": 1000}` and `{"per_hour": 5000, "burst": 10}`. Two periods
at once, or none, is a `400`; `{"per_second": 100}`, the form of 0.6, reads
as before.

`per_minute` is not a fixed window. The limit is smoothed: 1000 a minute is
about 16.7 a second plus the burst, so a client that waits for the minute to
begin and sends 1000 at once gets denials. The burst is per sidecar: a fleet
of N admits up to N times the burst back to back.

An entry of `PUT /v1/key_limits` is the exception of one key: `class` is
the class your service checks it in (`""` or left out for the default), and
it has either its own `limit` or `as_class`, the name of the class whose
limit it follows, tariff changes included; an `as_class` the sidecar does
not have is accepted, and gives the default limit, counted in
`unknown_class`. The key keeps its identity and
its debt: moving it from 500 to 2000 a second gives it no fresh burst, and
while the change has not reached every sidecar, the fleet admits at most the
higher of the two limits. A body replaces every exception, all or nothing;
there is room for 4096 of them by default.

## Shadow limits

`PUT /v1/shadows` gives a class a second limit that denies nothing: every
attempt on a key of the class is also run through it, and what it would have
denied is counted, in `shadow_denied` and in `most_shadow_denied`; a key in
the overflow slot, past the tracked keys, has no shadow, so under heavy key
churn the count is short of them. The class's real limit decides as before.
To roll a new limit out, set it as the shadow, read the report, then put it in
`/v1/classes` and clear the shadow.

The count errs on the strict side: a key whose demand lies between the hot
thresholds of the two limits is held to a smaller share under the shadow than
it would get as the real limit, so the shadow denies it more than enforcing
the limit would. Rolled out, the new limit affects as many clients as the
report says, or fewer. A key with its own limit has no shadow; one that
follows a class follows that class's shadow.

## Configuration

Every flag is also a variable, for Kubernetes: `--limit` is
`RATEGUARD_LIMIT`, and so on; `rateguard-sidecar --help` lists them all.

| Flag | Default | Meaning |
|---|---|---|
| `--limit` | required | Requests per key in the default class, across the fleet, as `100`, `1000/min` or `5000/h`, with a burst after the unit if you like: `5000/h/10` |
| `--burst` | `count / 20` | Requests one instance may admit back to back, when `--limit` gives none |
| `--class name=limit` | none | A class of limits, its limit in the form of `--limit`: `pro=1000/min/50`, `free=10`; repeated, or comma-separated in `RATEGUARD_CLASSES` |
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
| `--most-denied` | `10` | How many keys `most_denied` and `most_shadow_denied` list in `/v1/stats`, at most 1024, `0` for none (`RATEGUARD_MOST_DENIED`) |
| `--max-members` | `8192` | How many members an instance keeps track of, the dead not yet forgotten and itself included, from 1 to 65536; newcomers over it are refused and counted in `members_refused`. Size it for the fleet plus the addresses that churned in the last 10 minutes, about twice the fleet for rolling deploys with new pod IPs: while it refuses newcomers, the instance holds every key at the floor `R × β / max-members` (`members_full`). Over the cap the limit is not guaranteed: that hold softens an overflow but does not close it, so raise the cap when `members_refused` grows (`RATEGUARD_MAX_MEMBERS`) |
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
`gossip_auth` as `"off"`, `"transitional"` or `"required"`,
`rejected_datagrams` as
`{"unsigned": 0, "bad_tag": 0, "malformed": 0, "mismatched_records": 0}`
(the last counts records dropped from datagrams otherwise taken), and
`identity_conflicts`: one after a restart quicker than the peers noticed,
growing steadily when two instances advertise one address; and
`members_refused`: newcomers refused for a full member table, growing
steadily in a fleet bigger than `--max-members` or under a flood of unsigned
gossip, which only a secret file keeps out; `members_full` is true while
every key is held at the floor for it.
Each entry of `most_denied` has its `key_hash`, as a string, its
`denied_per_sec`, its `requests_per_sec` and its `shadow_denied_per_sec`, all
averaged over the last ten seconds or so on this sidecar, a new key's so far;
and, when the sidecar knows them, the `key` itself and its `class`, `""` for
the default one:

```json
{"key_hash": "8270364711458143018", "denied_per_sec": 42.5, "requests_per_sec": 310.0, "shadow_denied_per_sec": 0.0, "key": "203.0.113.7", "class": "free"}
```

`most_shadow_denied` is the same list by shadow denials: the clients a shadow
limit would have limited. Both rank keys by the plain average over the window,
the time before a key was first seen counted as silent, so a key denied once
just now ranks below one denied as often for ten seconds. `shadow_denied` is
the total. Every number is this sidecar's alone; adding them up across the
fleet is for whoever asks.

The sidecar remembers a key's string when it denies it, and only then, in a
table of the last 1024; a key over 256 bytes is kept cut, with
`"key_truncated": true`. An entry has no `key` and `class` when the key was
denied before the sidecar restarted or has been pushed out of the table, and
in a dry run, where nothing is denied. A shadow denial remembers the name
too, so `most_shadow_denied` is named like the other, in a table of its own
of the last 1024 keys only a shadow denied: a shadow tried on many keys
pushes out no name of a real denial.

### One key

`GET /v1/keys?class=pro&key=tenant:42` answers for one key (`class` may be
left out for the default class):

```json
{"requests_per_sec": 310.0, "denied_per_sec": 12.5, "shadow_denied_per_sec": 40.0, "share": 200.0, "limit": {"per_second": 1000, "burst": 50}, "shadow": {"per_second": 500, "burst": 25}}
```

`share` is the rate this sidecar admits the key at now, `limit` is the limit
it is held to, its own or its class's, and `shadow` is null without one.
`404` means this sidecar keeps no state for the key: it is not busy here.

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

`openssl rand -base64 64` wraps its output at 64 characters, so the secret
lands on two lines and the file is refused, naming the second. Use
`openssl rand -base64 32`, or add `-A` for any length: one line.

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

## Try it

[`examples/demo`](examples/demo): three sidecars and an unfair load balancer
in front of them, 1500 requests a second to one, 300 and 200 to the others,
under one limit of 1000 a second.

```sh
cd rateguard-sidecar/examples/demo
docker compose up
```

Every second it prints what each sidecar admitted: the shares follow the
demand, about 730, 160 and 110, and the fleet holds 1000. `docker compose
stop c` in another terminal, and the other two take its share at once.

## In Kubernetes

[`examples/kubernetes.yaml`](examples/kubernetes.yaml): the sidecar as a
second container of each pod, a headless service for the seeds, the gossip
secret and the key salt mounted from a Secret, which you create first with
the commands in its header, probes and Prometheus on `--metrics-listen`.
The image, for amd64 and arm64, is published with every release:

```sh
docker pull ghcr.io/donmiro/rateguard-sidecar:0.7
```

Tags are the version (`0.7.1`), the minor version (`0.7`, the latest
patch) and `latest`. To build it yourself, from the repository root:

```sh
docker build -f rateguard-sidecar/Dockerfile -t rateguard-sidecar .
```

## License

MIT OR Apache-2.0, as `rateguard`.
