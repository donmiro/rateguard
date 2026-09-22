# rateguard

[![crates.io](https://img.shields.io/crates/v/rateguard.svg)](https://crates.io/crates/rateguard)
[![docs.rs](https://docs.rs/rateguard/badge.svg)](https://docs.rs/rateguard)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Distributed rate limiting for Rust services — one global limit across your
whole fleet, with no Redis, no central service, and no network call on the
request path.**

```rust
use rateguard::Guard;

let guard = Guard::builder()
    .bind("0.0.0.0:7946")
    .seeds(["10.0.0.1:7946", "10.0.0.2:7946"])
    .limit(1_000)                       // 1000 rps per key, across every instance
    .spawn()?;

// ~50 ns. No I/O, no lock shared with the network, no .await.
// Still true if every other node in the cluster is gone.
if guard.check("api:tenant-42").is_allowed() {
    serve(request)
} else {
    too_many_requests()
}
```

## Features

- **One limit for the whole fleet.** Run ten instances of your service, keep a
  single limit of 1000 rps per key across all of them.
- **No datastore on the request path.** Decisions are made from local memory in
  about 50 nanoseconds. There is no Redis to deploy, scale, or lose.
- **Immune to cluster failure.** If gossip stops, every node keeps enforcing with
  the last share it knew. Nothing on the request path can fail, because nothing
  on the request path talks to anyone.
- **Shares follow demand.** Nodes exchange observed demand and take a
  proportional slice of the limit, so an uneven load balancer does not leave one
  node rejecting traffic while its neighbours idle.
- **Bandwidth independent of traffic.** One UDP datagram per node per 200 ms,
  whether you serve ten requests per second or a hundred thousand.
- **Bounded memory.** Configured ceilings on tracked keys, independent of how
  many distinct keys your traffic actually contains.
- **Network partitions are a configuration choice**, with proven overshoot bounds
  for each policy, rather than undefined behaviour.
- **Synchronous API.** `check()` is a plain function. No `async`, no executor
  required, callable from any thread.
- **No `unsafe`**, and a pure sans-I/O core with zero runtime dependencies.

## Installation

```toml
[dependencies]
rateguard = "0.1"
```

Requires Rust 1.85 or newer (2024 edition).

## Usage

### Getting started

Every instance of your service creates one `Guard`, pointed at a few well-known
peers. Membership discovers the rest.

```rust
use rateguard::{Decision, Guard, PartitionPolicy};
use std::time::Duration;

let guard = Guard::builder()
    .bind("0.0.0.0:7946")
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
or piece of state that needs it; all clones share one limiter.

### Keys

A key is any string you choose, and the limit applies to each key independently.
Use whatever dimension you are protecting:

```rust
guard.check("tenant:42");                   // per customer
guard.check("ip:203.0.113.7");              // per client address
guard.check(&format!("{tenant}:{endpoint}")); // per customer, per endpoint
```

Key cardinality is not a capacity concern: cold keys cost nothing on the network
and the per-key table has a configured ceiling. Ten million keys are fine.

### With `axum`

```rust
use axum::{extract::State, http::{header, StatusCode}, middleware::Next,
           response::{IntoResponse, Response}, extract::Request};
use rateguard::{Decision, Guard};

async fn rate_limit(State(guard): State<Guard>, req: Request, next: Next) -> Response {
    let key = format!("api:{}", tenant_of(&req));

    match guard.check(&key) {
        Decision::Allow => next.run(req).await,
        Decision::Deny { retry_after } => (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, retry_after.as_secs().max(1).to_string())],
        )
            .into_response(),
    }
}
```

### Configuration

| Option | Default | Meaning |
|---|---|---|
| `limit(rps)` | required | Requests per second per key, across the entire fleet |
| `burst(n)` | `limit / 20` | How much of the limit may be spent instantaneously |
| `bind(addr)` | required | UDP address for gossip |
| `seeds([..])` | `[]` | Peers to join through; any live member is enough |
| `partition_policy(p)` | `HoldDown(30s)` | Behaviour when the cluster splits |
| `protocol_period(d)` | `200 ms` | How often nodes exchange membership and demand |
| `hot_keys(n)` | `512` | How many keys may be coordinated at once |
| `tracked_keys(n)` | `4096` | Ceiling on the per-key table |

### Without the network

`rateguard-core` is the enforcement layer on its own: no I/O, no tokio, time
passed in as a parameter. Use it directly if you already have your own
clustering and only want the limiter.

```rust
use rateguard_core::{boundary::Event, gcra::Decision, limiter::Config, node::Node};

let mut node = Node::new(Config {
    limit_per_sec: 1_000,
    burst: 10,
    alpha: 0.5,
    cooldown: 5 * ONE_SEC,
    hot_set_size: 512,
    max_tracked_keys: 4_096,
    demand_time_constant: ONE_SEC,
});
node.set_cluster_size(5);

match node.check(key, now_nanos) {
    Decision::Allow => serve(),
    Decision::Deny { retry_at } => reject(retry_at),
}

node.handle(Event::Tick, now_nanos);   // driven by your own timer
```

## How it works

### Three layers

```
   request ──► [ enforcement ]   local, ~50 ns, exact, no I/O
                     ▲              GCRA over the share this node holds
                     │ "your share: 340 rps"
              [ allocation  ]   over the network, every 200 ms, approximate
                     ▲              demand gossip, shares recomputed
                     │
              [ membership  ]   SWIM: who is alive
```

The idea the whole design rests on: **only allocation is distributed and
eventually consistent.** Enforcement is always local, synchronous and
deterministic.

Three consequences follow, and they are the reason for the split:

- `check()` is an ordinary synchronous function — no executor, no `.await`, no
  lock shared with a network task.
- Clock skew between machines cannot produce a wrong decision. Nodes exchange
  demand estimates, never timestamps that must agree.
- The system is testable in a single-threaded simulator on virtual time.
  Partitions, packet loss and five-second GC pauses are ordinary test cases, and
  there is no `sleep` anywhere in the suite.

### Shares follow demand

Splitting the limit evenly, `R/N` per node, breaks as soon as your load balancer
is uneven: one node starts rejecting while its neighbours sit below their share.

Instead, each node gossips the demand it observes and computes its own share as
`R × (my_demand / total_demand)`. Shares converge on the real traffic
distribution. Absolute values are sent rather than deltas, so a lost datagram
costs 200 milliseconds of accuracy rather than corrupting a counter permanently.

A floor of `R × β / N` is always reserved for every live node, so a node with no
traffic yet can still admit its first requests.

### Only hot keys are coordinated

You may have ten million keys; about seventy fit in a UDP datagram. But
coordination is only needed for keys that are approaching their limit:

- **cold keys** run locally on `R/N × α` (α = 0.5) and generate no network traffic
- **hot keys** join the gossip set and receive a demand-proportional share

Keys are promoted on crossing the threshold and demoted after they cool down.
The bound is provable: traffic that is never coordinated cannot exceed `R × α` —
half the limit at the default α.

### Network partitions

When the network splits into `k` groups, each group believes it is the whole
cluster. No design can avoid that; `rateguard` makes it an explicit choice.

| Policy | Overshoot | Cost |
|---|---|---|
| `Optimistic` | up to `k × R` | none, maximum availability |
| `HoldDown(d)` **(default)** | ≈ `R` for `d`, then `k × R` | the limit is underused for `d` after a real failure |
| `Quorum` | `R + ε` | the minority side stops serving almost entirely |

`HoldDown` is the default because most apparent partitions are a GC pause or a
pod restart lasting seconds, and holding the limit steady through those is nearly
always what you want.

## Guarantees

1. `check()` performs no I/O and takes no lock shared with a network task. Its
   latency is independent of cluster state, including a fully collapsed one.
2. A healthy cluster in steady state admits between `R × (1 − ε)` and `R`.
3. Under partition, the bounds above, according to the selected policy.
4. Memory is `O(hot_keys × N)`, bounded by configuration.
5. Bandwidth is one datagram per node per protocol period.

## Compared to a shared counter

| | Redis / Memcached | rateguard |
|---|---|---|
| Decision latency | network round trip (~0.2–1 ms) | ~50 ns, local |
| Store unavailable | service degrades | nothing to be unavailable |
| Network load | linear in request volume | one datagram per node per 200 ms |
| Memory | linear in key count | bounded by configuration |
| Accuracy under partition | undefined | proven bounds, selected by config |
| Operational cost | a datastore to run and scale | a UDP port |

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
note that the protocol assumes a trusted one: it is neither encrypted nor
authenticated.

## FAQ

**Is there a coordinator, leader, or master node?** No. Every node runs the same
code and reaches its own conclusions. There is nothing to elect and nothing whose
loss stops the cluster.

**What happens when a node dies?** Membership notices within a few protocol
periods and the remaining nodes redistribute its share among themselves. Until
that happens, the fleet is under the limit rather than over it.

**What happens when a node restarts?** It rejoins through its seeds and starts
on the cold-key share until its demand is observed again. A restart does not
hand it a fresh full quota.

**Does clock skew matter?** No. Nodes never compare timestamps with each other;
each uses only its own monotonic clock, and what travels between them are demand
estimates.

**How much traffic does gossip generate?** One datagram per node per protocol
period, 200 ms by default, capped at a 1400-byte MTU. A fifty-node cluster
exchanges roughly 350 KB/s in total, no matter how much traffic it serves.

**Can I use it from services that aren't in Rust?** Not yet. A sidecar speaking
gRPC/HTTP is planned.

## Crates

| Crate | Contents |
|---|---|
| `rateguard` | The runtime: UDP, timers, the public API |
| `rateguard-core` | Pure logic — GCRA, demand estimation, hot/cold sets, membership state machine. No I/O, no tokio, time as a parameter |
| `rateguard-proto` | Wire format |
| `rateguard-sim` | Deterministic simulator on virtual time, used to develop and test the protocol. Not published |

## Contributing

Issues and discussion are welcome, particularly failure scenarios the design does
not survive and review from anyone who has operated a gossip protocol in
production.

## License

MIT
