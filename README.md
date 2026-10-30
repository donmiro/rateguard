# rateguard

[![CI](https://github.com/donmiro/rateguard/actions/workflows/ci.yml/badge.svg)](https://github.com/donmiro/rateguard/actions/workflows/ci.yml)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**Distributed rate limiting for Rust services — one global limit across your
whole fleet, with no Redis, no central service, and no network call on the
request path.**

```rust
use rateguard::Guard;

let guard = Guard::builder()
    .bind("0.0.0.0:7946")
    .advertise("10.0.0.3:7946")         // where the other instances reach this one
    .seeds(["10.0.0.1:7946", "10.0.0.2:7946"])
    .limit(1_000)                       // 1000 rps per key, across every instance
    .spawn()?;

// ~25 ns. No I/O, no lock shared with the network, no .await.
// Still true if every other node in the cluster is gone.
if guard.check("api:tenant-42").is_allowed() {
    serve(request)
} else {
    too_many_requests()
}
```

## See it work

Three instances under one limit of 1000 a second, behind a load balancer
that sends 1500 requests a second to A, 300 to B and 200 to C:

```console
$ cargo run -p rateguard --release --example cluster
   t       A       B       C   fleet  nodes
  1s     161      66      68     295      3
  2s     670     120      83     873      3
  3s     764     164     111    1039      3
  4s     697     150     107     954      3
  ...
 12s     757     165     115    1037      3
      C leaves
 13s     729     159       -     888      2
 14s     817     184       -    1001      2
```

The first second sits at the floor while the instances learn each other's
demand; then each takes a share in proportion to its demand, and the fleet
holds 1000. When C leaves, A and B take its share at once. The same with the
sidecar, in Docker, is `docker compose up` in
[`rateguard-sidecar/examples/demo`](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar/examples/demo).

And in Kubernetes, while `kubectl scale` takes the deployment from 3
replicas to 6, 2 and 4, each offered 600 requests a second on one key: the
fleet holds 1000 throughout, the shares following the replicas
([`rateguard-sidecar/examples/k3d`](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar/examples/k3d),
`./demo.sh up && ./demo.sh scale`).

![The fleet holding 1000 a second while the deployment scales](https://raw.githubusercontent.com/donmiro/rateguard/main/rateguard-sidecar/examples/k3d/demo.gif)

## Features

- **One limit for the whole fleet.** Ten instances of your service, one limit
  of 1000 rps per key across all of them.
- **Nothing on the request path.** `check()` decides from local memory in about
  25 ns: no Redis to deploy, scale or lose, no `.await`, no lock shared with
  the network.
- **Immune to cluster failure.** If gossip stops, every instance keeps
  enforcing with the last share it knew.
- **Shares follow demand**, so an uneven load balancer does not leave one
  instance rejecting while its neighbours idle.
- **Tariffs, changed live.** Classes of limits, per second, minute or hour,
  and one customer's own limit, all changed while the fleet runs; a shadow
  limit and a dry run show what a new limit would deny before it does.
- **Bounded.** Two UDP datagrams per instance per 200 ms however much traffic
  you serve, and memory set by configuration, not by how many keys there are.
- **Network partitions are a configuration choice**, with an overshoot bound
  for each policy, measured in a deterministic simulator.
- **Any language** through a sidecar, with clients for Python, Go and
  TypeScript. For Rust: a `tower` layer for axum, `stats()` and metrics,
  signed gossip, and no `unsafe`.

## Installation

```toml
[dependencies]
rateguard = "0.7"
```

Requires Rust 1.88 or newer (2024 edition). Optional features:

- `tower`: a `tower` layer, for axum and any tower stack;
- `metrics`: what `Guard::stats` tells, through the
  [`metrics`](https://crates.io/crates/metrics) facade.

## Getting started

Every instance of your service creates one `Guard`, pointed at a few well-known
peers. Membership discovers the rest.

```rust
use rateguard::{Decision, Guard, PartitionPolicy};
use std::time::Duration;

let guard = Guard::builder()
    .bind("0.0.0.0:7946")
    .advertise("10.0.0.3:7946")
    .seeds(["10.0.0.1:7946", "10.0.0.2:7946"])
    .limit(1_000)
    .burst(50)
    .partition_policy(PartitionPolicy::HoldDown(Duration::from_secs(30)))
    .spawn()?;

match guard.check("api:tenant-42") {
    Decision::Allow => serve(request),
    Decision::Deny { retry_after } => reject(retry_after),
}
```

`Guard` is cheap to clone and `Send + Sync`. Clone it into every handler, task,
or piece of state that needs it; all clones share one limiter. `spawn()` needs a
tokio runtime to run the node in; `check()` does not, and works from any thread.

When the instance shuts down, say so: the other instances take its share at
once, instead of first suspecting it and then holding its share for a split
that is not one.

```rust
// On SIGTERM, before the last requests drain.
guard.leave().await;
```

Dropping the last clone leaves too, if the runtime is still running to send
the news.

## From other languages

`rateguard-sidecar` runs next to each instance of a service in any language
and answers it over HTTP on loopback; the sidecars share the limit as Rust
processes do.

```sh
rateguard-sidecar --limit 1000 --advertise 10.0.0.3:7946 --seeds 10.0.0.1:7946
curl -s -X POST -d '{"key": "tenant:42"}' http://127.0.0.1:7947/v1/check
# {"allowed":true}
```

It is on crates.io (`cargo install rateguard-sidecar`) and an image,
`ghcr.io/donmiro/rateguard-sidecar:0.7`, for amd64 and arm64.

```python
from rateguard_client import Client  # pip install rateguard-client

decision = Client().check("tenant:42", cls="pro")
```

Clients for [Python](https://pypi.org/project/rateguard-client/),
[Go](https://pkg.go.dev/github.com/donmiro/rateguard/sdk/go) and
[TypeScript](https://www.npmjs.com/package/rateguard-client) check keys,
read stats, and change limits and shadows while the fleet runs; any HTTP
client will do. The
[sidecar's README](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar)
has its flags, its API, a Kubernetes example, and a `docker compose` demo.

## When to use it, and when not to

**A good fit when** you run several instances of a Rust service and want one
shared limit; when a round trip to a datastore on every request is a real cost;
when your traffic is uneven across instances; and when being briefly and
boundedly over the limit during a network partition is acceptable — as it
usually is for protecting a backend, an upstream API quota, or a noisy tenant.

**Reach for something else when** you need exact counting, where a single extra
admitted request has a real price — billing, prepaid quotas, regulatory caps.
Use a centralized counter. If you run a single instance,
[`governor`](https://crates.io/crates/governor) is simpler and gives you the
same enforcement. And if your gossip traffic would cross an untrusted network,
note that it can be signed but is never encrypted; see [Security](#security)
below.

## Compared to alternatives

| | rateguard | A counter in Redis | [Gubernator](https://github.com/gubernator-io/gubernator) | [Envoy global rate limit](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_features/global_rate_limiting) | [governor](https://crates.io/crates/governor) |
|---|---|---|---|---|---|
| Network call per request | none | to Redis, ~0.2–1 ms | to the key's owner node, except in `GLOBAL` mode | gRPC to the rate limit service | none |
| To deploy | nothing: a library, or a sidecar | Redis | a Gubernator cluster | the rate limit service and Redis | nothing |
| When the coordinator is gone | keeps enforcing on the last shares | allow or deny, your choice | — | allows, unless `failure_mode_deny` | no coordinator |
| Accuracy | approximate, with a bounded, measured overshoot | exact | exact at the owner; approximate in `GLOBAL` | exact, in fixed windows | exact |
| One limit across a fleet | yes | yes | yes | yes | no, one process |
| Languages | Rust; any through the sidecar | any | any over gRPC or HTTP; a Go library | any behind Envoy | Rust |

Pick another when it fits better:

- **A counter in Redis** when every admitted request must be counted
  exactly: billing, prepaid quotas. It costs a round trip per request and a
  datastore to run.
- **Envoy's global rate limit** when every request already goes through
  Envoy, and limits belong to the mesh rather than to the service.
- **Gubernator** for a rate limit service of its own with exact counts per
  key. Its `GLOBAL` mode answers from each node's cache and reconciles in
  the background, the nearest thing to rateguard.
- **governor** for a single instance: the same GCRA, with nothing to
  coordinate.

## Security

By default the gossip protocol trusts its network: anyone who can reach the
UDP port can join the cluster, shift the shares, or declare members dead.
Keep the port on a private network, and sign the gossip with a secret every
instance shares; where clients choose the keys, such as an IP address, add a
key salt too. Signing authenticates the gossip but does not encrypt it.
Turning signing on in a running cluster, rotating secrets, and what is still
open: [Security](https://github.com/donmiro/rateguard/blob/main/guide/security.md).

## Documentation

- [Using rateguard](https://github.com/donmiro/rateguard/blob/main/guide/usage.md):
  keys, classes of limits, limits per minute and hour, a key's own limit,
  shadow limits, a dry run, axum, and every option.
- [Observability](https://github.com/donmiro/rateguard/blob/main/guide/observability.md):
  `stats()`, one key's counters, and the metrics.
- [How it works](https://github.com/donmiro/rateguard/blob/main/guide/how-it-works.md):
  the design, performance, the guarantees, and the accuracy report.
- [Security](https://github.com/donmiro/rateguard/blob/main/guide/security.md),
  [Without the network](https://github.com/donmiro/rateguard/blob/main/guide/without-the-network.md)
  (the protocol with no I/O), and the
  [FAQ](https://github.com/donmiro/rateguard/blob/main/guide/faq.md).
- The API on [docs.rs](https://docs.rs/rateguard).

## Crates

| Crate | Contents |
|---|---|
| `rateguard` | The runtime: UDP, timers, the public API |
| `rateguard-core` | Pure logic — GCRA, demand estimation, hot/cold sets, membership state machine. No I/O, no tokio, time as a parameter |
| `rateguard-proto` | Wire format |
| `rateguard-sidecar` | The runtime as a program, with an HTTP API on loopback, for services in any language |
| `rateguard-sim` | Deterministic simulator on virtual time, used to develop and test the protocol. Not published |

## Contributing

Issues and discussion are welcome, particularly failure scenarios the design does
not survive and review from anyone who has operated a gossip protocol in
production.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/donmiro/rateguard/blob/main/LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](https://github.com/donmiro/rateguard/blob/main/LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
