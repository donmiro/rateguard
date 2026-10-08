# rateguard

[![CI](https://github.com/donmiro/rateguard/actions/workflows/ci.yml/badge.svg)](https://github.com/donmiro/rateguard/actions/workflows/ci.yml)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**Distributed rate limiting for Rust services — one global limit across your
whole fleet, with no Redis, no central service, and no network call on the
request path.**

> **Status: not released yet.** The core, the simulator, the `rateguard`
> runtime and its benchmarks are done and tested; the accuracy report is next.
> Nothing is published on crates.io yet.

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

## Features

- **One limit for the whole fleet.** Run ten instances of your service, keep a
  single limit of 1000 rps per key across all of them.
- **No datastore on the request path.** Decisions are made from local memory in
  about 25 nanoseconds. There is no Redis to deploy, scale, or lose.
- **Immune to cluster failure.** If gossip stops, every node keeps enforcing with
  the last share it knew. Nothing on the request path can fail, because nothing
  on the request path talks to anyone.
- **Shares follow demand.** Nodes exchange observed demand and take a
  proportional slice of the limit, so an uneven load balancer does not leave one
  node rejecting traffic while its neighbours idle.
- **Bandwidth independent of traffic.** Two UDP datagrams per node per 200 ms
  in a healthy cluster, whether you serve ten requests per second or a
  hundred thousand.
- **Bounded memory.** Configured ceilings on tracked keys, independent of how
  many distinct keys your traffic actually contains.
- **Network partitions are a configuration choice**, with proven overshoot bounds
  for each policy, rather than undefined behaviour.
- **Synchronous API.** `check()` is a plain function. No `async`, no executor
  required, callable from any thread.
- **No `unsafe`**, and a pure sans-I/O core: no async runtime, no clock, no
  sockets.

## Installation

```toml
[dependencies]
rateguard = "0.1"
```

Requires Rust 1.88 or newer (2024 edition).

## Usage

### Getting started

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
The node stops when the last clone is dropped.

### Keys

A key is any string you choose, and the limit applies to each key independently.
Use whatever dimension you are protecting:

```rust
let per_customer = guard.check("tenant:42");
let per_client = guard.check("ip:203.0.113.7");
let per_endpoint = guard.check(&format!("{tenant}:{endpoint}"));
```

Cold keys cost nothing on the network, and memory does not grow with the number
of keys: the per-key table is allocated once, with room for twice
`tracked_keys`, and a key that has gone quiet frees its place. Many distinct keys
over time are fine. What the table bounds is how many keys are *active at once*:
up to a quarter again as many as `tracked_keys`, every key gets a place of its
own; at half again as many, now and then one key does not; beyond that, the
extra ones share a single allowance at the cold rate. That errs on the strict
side, never the loose one, but it can deny a well-behaved key; size
`tracked_keys` to at least the number of keys you expect to be busy at the same
moment.

If clients choose the keys (an IP address, an API token), the same applies to
them: a flood of distinct keys beyond that pushes new keys into the shared
allowance. Picking keys that crowd out one particular key does not work: where
a key lives in the table depends on a secret random to each instance.

### With `axum`

Runnable as `cargo run -p rateguard --example axum`.

```rust
use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rateguard::{Decision, Guard};

async fn rate_limit(State(guard): State<Guard>, req: Request, next: Next) -> Response {
    let key = format!("api:{}", tenant_of(&req));

    match guard.check(&key) {
        Decision::Allow => next.run(req).await,
        Decision::Deny { retry_after } => (
            StatusCode::TOO_MANY_REQUESTS,
            // Whole seconds, rounded up: rounding down invites an early retry.
            [(
                header::RETRY_AFTER,
                retry_after.as_millis().div_ceil(1000).max(1).to_string(),
            )],
        )
            .into_response(),
    }
}
```

### Configuration

| Option | Default | Meaning |
|---|---|---|
| `limit(rps)` | required | Requests per second per key, across the entire fleet |
| `burst(n)` | `limit / 20`, at least 1 | Requests one instance may admit back to back, on top of its share. Per instance: the fleet as a whole may admit `R` plus a burst on each. At most 16,777,215; `0` is taken as 1 |
| `bind(addr)` | required | UDP address for gossip, as `ip:port` |
| `advertise(addr)` | the `bind` address | Where the other instances reach this one; required when binding to `0.0.0.0` or `::`. In Kubernetes, the pod IP |
| `seeds([..])` | `[]` | Peers to join through, as `ip:port` (no DNS names yet); any live member is enough |
| `partition_policy(p)` | `HoldDown(10s)` | Behaviour when the cluster splits |
| `protocol_period(d)` | `200 ms` | How often nodes exchange membership and demand |
| `hot_keys(n)` | `64` | How many keys may be coordinated at once, at most 64 |
| `tracked_keys(n)` | `4096` | Keys one instance keeps state for at once, at least `hot_keys` and at most 1,048,576; the table holds twice as many |

### Without the network

`rateguard-core` is the whole protocol without the I/O: enforcement and
membership, no sockets, no tokio, time passed in as a parameter. Bring your
own UDP socket and timer, feed it events, and send the datagrams it returns.

```rust
use rateguard_core::{
    boundary::{Action, Event, PeerId},
    gcra::Decision,
    limiter::Config,
    node::{Node, SwimConfig},
};

const ONE_SEC: u64 = 1_000_000_000;

let mut node = Node::new(
    Config {
        limit_per_sec: 1_000,
        burst: 10,
        alpha: 0.5,
        floor_factor: 0.05,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 64,
        max_tracked_keys: 4_096,
        demand_time_constant: ONE_SEC,
    },
    SwimConfig::default(),
    PeerId::new(1), // this node
    my_address,     // a rateguard_proto::Address, gossiped so others can reach it
    rng_seed,       // drives its random choices: same seed, same run
);
node.add_seed(PeerId::new(0), seed_address); // N comes from membership

// Send each datagram to node.address(peer); a received datagram's sender is
// the member of its first record, see the rateguard crate.

// Keys are 64-bit hashes; the original string never leaves your process.
match node.check(key_hash, now_nanos) {
    Decision::Allow => serve(),
    Decision::Deny { retry_at } => reject(retry_at),
}

// From your own timer (several times per protocol period) and socket.
for Action::SendTo { peer, bytes } in node.handle(Event::Tick, now_nanos) {
    send(*peer, bytes);
}
node.handle(
    Event::MessageReceived {
        from,
        bytes: &datagram,
    },
    now_nanos,
);
```

## How it works

### Three layers

```
   request ──► [ enforcement ]   local, ~25 ns, exact, no I/O
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

Under `Quorum`, a node that cannot tell a split from a death keeps counting
the members it lost, however long ago. So never remove half the cluster or
more at once: the nodes left would take it for a split and stay at the floor.
Scale down by less than half at a time, and let ten minutes pass between
steps, the time a dead member takes to be forgotten.

## Performance

`check()`, measured with `criterion` on an Apple M4 Pro
(`cargo bench -p rateguard --bench check`):

| Case | Per check |
|---|---|
| One hot key, one thread | 26 ns |
| 4096 keys in turn | 33 ns |
| One hot key, the network hung (the cluster as good as gone) | 25 ns |
| 8 threads, a key each | 27 ns |
| 8 threads on one key, back to back | 1.4–2.1 µs |

The last line is the one cost to know. An exact limit means every check on a key
writes the same word, so cores hitting one key at once queue for it: about 6
million checks a second on a single key per instance. Far beyond what one tenant
sends to one instance in practice, but not free.

## Guarantees

1. `check()` performs no I/O and takes no lock shared with a network task. Its
   latency is independent of cluster state, including a fully collapsed one.
2. A healthy cluster in steady state admits between `R × (1 − ε)` and `R`.
3. Under partition, the bounds above, according to the selected policy.
4. Memory is bounded by configuration: the coordination state is
   `O(hot_keys × N)`, and the per-key table is allocated once: two 32-byte
   slots per tracked key, rounded up to a power of two (256 KB at the default
   4096).
5. Bandwidth in a healthy cluster is two datagrams per node per protocol
   period: the node's own probe and, on average, one answer to a probe of it.

## Compared to a shared counter

| | Redis / Memcached | rateguard |
|---|---|---|
| Decision latency | network round trip (~0.2–1 ms) | ~25 ns, local |
| Store unavailable | service degrades | nothing to be unavailable |
| Network load | linear in request volume | two datagrams per node per 200 ms |
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

## Security

The gossip protocol assumes a trusted network. Datagrams are neither encrypted
nor authenticated: anyone who can reach the UDP port can join the cluster,
announce demand that shifts the shares, or declare members dead. Keep the port
on a private network, or behind a firewall that lets in only your own
instances. Authenticated gossip is planned.

## FAQ

**Is there a coordinator, leader, or master node?** No. Every node runs the same
code and reaches its own conclusions. There is nothing to elect and nothing whose
loss stops the cluster.

**What happens when a node dies?** Membership notices within a few protocol
periods and the remaining nodes redistribute its share among themselves. Until
that happens, the fleet is under the limit rather than over it.

**What happens when a node restarts?** It rejoins through its seeds. Until it
has, and until its peers have heard its demand, every key it serves is held at
a floor, `R × β` split over the members it knows of (under `Optimistic`,
before it rejoins, at the cold-key share instead). A restart does not hand it a fresh full quota.

**And when a node stops on purpose, in a rolling deploy?** Its peers cannot
tell a stop from a crash: there is no goodbye message. They notice within the
suspicion timeout (six seconds), and under the default `HoldDown` they keep
the departed node's share out of use for the hold, ten seconds. A deploy that
restarts instances one by one therefore runs a little under the limit while it
lasts, never over it. An instance restarted on the same address and back
within the suspicion timeout is never declared dead at all.

**How do I health-check it?** `guard.is_running()` turns false if the
background task has died, which is a bug; the instance then holds every key at
the floor `R × β / N` rather than at shares its peers no longer count.
`guard.cluster_size()` tells you how many members it sees, itself included.

**Does clock skew matter?** No. Nodes never compare timestamps with each other;
each uses only its own monotonic clock, and what travels between them are demand
estimates.

**How much traffic does gossip generate?** In a healthy cluster, two datagrams
per node per protocol period (200 ms by default), each capped at a 1400-byte
MTU. A fifty-node cluster exchanges at most about 700 KB/s in total, no matter
how much traffic it serves. Failures add indirect probes for as long as they
last.

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

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
