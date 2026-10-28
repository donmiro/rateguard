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

## Features

- **One limit for the whole fleet.** Run ten instances of your service, keep a
  single limit of 1000 rps per key across all of them.
- **Tariffs, changed live.** Classes of limits in one guard, Free, Pro or one
  customer of its own, added and changed while the fleet runs.
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
- **Signed gossip.** A secret the fleet shares keeps strangers out of the
  cluster, turned on and rotated by rolling updates; a key salt keeps clients
  from choosing keys that collide with someone else's.
- **Limits per second, minute or hour**, for the default class, a class or
  one key; one customer's deal is a key limit, changed while the fleet runs.
- **Observable, and safe to roll out.** `stats()` and optional `metrics` say
  what each instance admits and denies and why; a dry run counts what would
  be denied without denying it, and enforcement turns on with no restart. A
  shadow limit counts, beside a class's real one, which keys a new limit would
  deny, and `key_stats` tells how one key fares.
- **Synchronous API.** `check()` is a plain function. No `async`, no executor
  required, callable from any thread.
- **No `unsafe`**, and a pure sans-I/O core: no async runtime, no clock, no
  sockets.

## Installation

```toml
[dependencies]
rateguard = "0.7"
```

Requires Rust 1.88 or newer (2024 edition). Optional features:

- `tower`: a `tower` layer, for axum and any tower stack;
- `metrics`: what `Guard::stats` tells, through the
  [`metrics`](https://crates.io/crates/metrics) facade.

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

When the instance shuts down, say so: the other instances take its share at
once, instead of first suspecting it and then holding its share for a split
that is not one.

```rust
// On SIGTERM, before the last requests drain.
guard.leave().await;
```

Dropping the last clone leaves too, if the runtime is still running to send
the news.

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

### Classes of limits

Different keys can have different limits: tariffs, one customer with a deal
of its own, a costly endpoint. A class is a name with a limit; a key is
checked in a class, and `check(key)` is the default class, the one of
`limit`.

```rust
use rateguard::{Guard, Limit};

let guard = Guard::builder()
    .bind("0.0.0.0:7946")
    .advertise("10.0.0.3:7946")
    .seeds(["10.0.0.1:7946"])
    .limit(100) // the default class
    .class("pro", Limit::per_second(1_000).burst(50))
    .spawn()?;

let by_ip = guard.check("ip:203.0.113.7"); // 100 a second
let pro = guard.check_in("pro", "tenant:42"); // 1000 a second
```

The same key in two classes is two keys. A class this instance does not
know, a typo or one since removed, takes the default limit, and its checks
are counted in `stats().unknown_class`.

Classes change while the fleet runs, from any clone of the guard:

```rust
guard.set_class("tenant:7", Limit::per_second(2_000))?; // added or changed
guard.remove_class("pro");
guard.set_limit(Limit::per_second(200))?; // the default class
guard.set_classes([("free", Limit::per_second(10))])?; // all named, at once
```

New keys take a changed limit at once, known ones within a tick of the
background task. Give every instance the same classes: while some have a
class's new limit and others the old, the fleet admits no more than the
higher of the two. A raised limit is never overshot; a lowered one holds
once every instance has it (see [Accuracy](#accuracy)).

### Limits per minute and per hour

A limit is a count and a period:

```rust
use rateguard::Limit;

Limit::per_second(100);
Limit::per_minute(1_000); // about 16.7 a second, burst 50
Limit::per_hour(5_000).burst(10);
```

`limit.count()`, `limit.period()` and `limit.rate_per_sec()` read it back;
`rate()` is deprecated, as it rounds a slow rate up to a whole number. The
default burst is a twentieth of the count, at least 1. `Builder::default_limit`
takes any `Limit` for the default class, where `Builder::limit(rps)` takes
requests per second. `per_minute(60)` and `per_second(1)` admit the same, but
are not `==`: a `Limit` keeps what it was given.

`per_minute` is not a fixed window. The limit is smoothed, GCRA, not a counter
that resets: "1000 a minute" is about 16.7 a second, plus the burst. A client
that waits for the minute to begin and sends 1000 at once gets denials. And
the burst is per instance: a fleet of N admits up to N times the burst back to
back, which for a burst of thousands is a lot. Use burst for smoothing, not for
quota; a daily or monthly quota that must never be exceeded wants a shared
ledger, not this.

Slow rates are held at their rate, not rounded up: `per_hour(1)` on a fleet of
40 is shared out as one an hour across the fleet, not cut short to a rate each
instance can hold. As always, each instance also admits its burst on top.
(A burst so long at so slow a rate that it spans over 292 years is cut short,
the strict way.)

### Limits for one key

A customer with a deal of its own needs no class of its own and no deploy: an
exception names a class and a key, and gives it a limit, or says "as in that
class".

```rust
use rateguard::{KeyLimit, Limit};

guard.set_key_limit("pro", "tenant:42", Limit::per_second(2_000))?;
guard.set_key_limit("free", "tenant:7", KeyLimit::class("pro"))?; // follows Pro
guard.remove_key_limit("pro", "tenant:42");
// All at once:
guard.set_key_limits([("pro", "tenant:42", Limit::per_minute(90_000))])?;
guard.key_limits(); // Vec<(String, String, KeyLimit)>
```

The key stays the key: its hash and its debt are kept, so moving a customer
from 500 to 2000 a second gives it no fresh burst, and the other way takes
none away. While the change has not reached every instance, the fleet admits
at most the higher of the two limits. `KeyLimit::class` refers to a class by
name, so a new Pro limit follows; a class this instance does not have is
the default limit, counted in `unknown_class`. `set_key_limits` replaces all
the exceptions, or none if one is refused; load a big set with it, as each
`set_key_limit` copies the whole table. An instance keeps up to
`max_key_limits` of them, 4096 by default and at most 1,048,576
(`Error::TooManyKeyLimits` when full, `Error::TooLargeMaxKeyLimits` for a
bound over that).

An exception is not among the classes that set the cold rate for new keys,
so a key held to a limit stricter than every class may pass at a looser rate
in three narrow cases: another thread's check in the nanoseconds between the
key's taking a slot and the slot getting its own quota, at the strictest
class's rate; a key whose first check comes just as the instance's first
exception is set, at its class's rate until the next tick; and a key in the
overflow slot, past `tracked_keys`, while it is there. Once a key has its
slot, nothing is read from the exceptions on its request path; a new key's
first check looks itself up while any exception exists.

### Shadow limits

To see what a new limit would do before it does it, give a class a shadow:

```rust
// "" is the default class:
guard.set_shadow("free", Some(Limit::per_second(10)))?;
// ... a day later, by the report:
guard.set_class("free", Limit::per_second(10))?;
guard.set_shadow("free", None)?;
```

The class's real limit decides, as before. Every attempt of a key with a slot
of its own is also run through the shadow, the ones the real limit denied
included, so it answers "what if clients sent the same under the new limit",
for a lower limit and a higher one. Keys in the overflow slot, past
`tracked_keys`, have no shadow, so under heavy key churn the count is short
of them. What it would have denied is in `stats().shadow_denied`,
`stats().most_shadow_denied` and, with `metrics`,
`rateguard_shadow_denied_total`.
A key with an exception of its own has no shadow; a key that follows a class
follows its shadow. `guard.shadows()` lists them, `set_shadows` replaces them
all, and a class that does not exist is `Error::UnknownClass`.

The shadow is approximate, and errs on the strict side. A key is hot, and takes
a share by its demand, when it passes a threshold that depends on the limit.
A key whose demand lies between the thresholds of the two limits is cold under
the real limit and would be hot under the shadow; the shadow gives it only the
cold share, and so counts more denials than enforcing the limit would. The
report is cautious: rolled out, the new limit affects as many clients as it
says, or fewer. A class without a shadow pays for it with one load of a word
the check already has in its cache line.

`guard.check_traced(key)` and `check_in_traced(class, key)` return a `Checked`
with the `decision` and the shadow's, `Option<Decision>`, for logs and for
anything that needs to know which keys the shadow denied. They decide as
`check` does.

### With `axum`

With the `tower` feature, `RateLimitLayer` takes the key from each request
and answers a denied one with `429 Too Many Requests` and `Retry-After`.
Runnable as `cargo run -p rateguard --features tower --example axum`.

```rust
use axum::extract::Request;
use rateguard::tower::RateLimitLayer;

// One limit per tenant. A request with no tenant is not limited:
// return a key for it too, such as "api:anonymous", to limit it.
let limit = RateLimitLayer::new(guard, |req: &Request| {
    let tenant = req.headers().get("x-tenant")?.to_str().ok()?;
    Some(format!("api:{tenant}"))
});

let app = Router::new()
    .route("/", get(|| async { "served\n" }))
    .layer(limit);
```

To check in a class, return it with the key, as a pair:
`Some((plan, format!("api:{tenant}")))`.

### From other languages

`rateguard-sidecar` runs next to each instance of a service in any language,
and answers it over HTTP on loopback; the sidecars share the limit as Rust
processes do. Clients for Python, Go and TypeScript are in
[`sdk/`](https://github.com/donmiro/rateguard/tree/main/sdk), and any HTTP
client will do:

```sh
rateguard-sidecar --limit 1000 --advertise 10.0.0.3:7946 --seeds 10.0.0.1:7946
curl -s -X POST -d '{"key": "tenant:42"}' http://127.0.0.1:7947/v1/check
# {"allowed":true}
```

```python
from rateguard_client import Client  # pip install rateguard-client

decision = Client().check("tenant:42", cls="pro")
```

The sidecar takes limits per second, minute and hour (`--limit 1000/min`,
`--class free=10`), and over its admin API key limits, shadows, and one key's
counters (`GET /v1/keys`); the clients cover that API too. See the
[sidecar's README](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar)
for its flags, its API, and a Kubernetes example.

### Configuration

| Option | Default | Meaning |
|---|---|---|
| `limit(rps)` | required | Requests per second per key, across the entire fleet; or `default_limit(limit)` for a `Limit` per second, minute or hour |
| `burst(n)` | `limit / 20`, at least 1 | Requests one instance may admit back to back, on top of its share. Per instance: the fleet as a whole may admit `R` plus a burst on each. At most 4,194,303; `0` is taken as 1 |
| `bind(addr)` | required | UDP address for gossip, as `ip:port` |
| `advertise(addr)` | the `bind` address | Where the other instances reach this one; required when binding to `0.0.0.0` or `::`. In Kubernetes, the pod IP |
| `seeds([..])` | `[]` | Peers to join through, as `ip:port` or `host:port`; any live member is enough. Names are looked up again while the node runs, so one name for the whole fleet, such as a Kubernetes headless service, keeps up with instances that come and go |
| `partition_policy(p)` | `HoldDown(10s)` | Behaviour when the cluster splits |
| `protocol_period(d)` | `200 ms` | How often nodes exchange membership and demand |
| `hot_keys(n)` | `64` | How many keys may be coordinated at once, at most 64 |
| `tracked_keys(n)` | `4096` | Keys one instance keeps state for at once, at least `hot_keys` and at most 1,048,576; the table holds twice as many |
| `dry_run(on)` | `false` | Admit every request and count those that would be denied; see [Dry run](#dry-run) |
| `class(name, limit)` | none | A class of limits, see [Classes of limits](#classes-of-limits) |
| `max_classes(n)` | `64` | How many named classes an instance keeps, at most 4096 |
| `max_key_limits(n)` | `4096` | How many exceptions for single keys an instance keeps, at most 1,048,576; see [Limits for one key](#limits-for-one-key) |
| `most_denied(k)` | `10` | How long `most_denied` and `most_shadow_denied` are, at most 1024, `0` for none |
| `max_members(n)` | `8192` | How many members an instance keeps track of, the dead not yet forgotten and itself included, from 1 to 65,536. Over it newcomers are refused and counted in `members_refused`. Size it for the fleet plus every address that churned in the last 10 minutes, the time the dead are remembered: about twice the fleet for rolling deploys that give pods new IPs. While the table refuses newcomers, every instance shares the limit as if `n` nodes were up and holds every key at the floor `R × β / n`. Over the cap the limit is not guaranteed, as that hold softens an overflow but does not close it: raise the cap when `members_refused` grows. The default covers fleets up to about 4000 instances under rolling deploys; see [Security](#security) |
| `gossip_auth(mode, secrets)` | `Off`, no secret | Whether gossip is signed, with at most two secrets of 32 bytes or more; see [Security](#security) |
| `key_salt(bytes)` | none | A salt of 16 bytes or more for the hashes of keys, the same on every node; see [Security](#security) |

### Observability

`guard.stats()` tells what one instance did and how it sees the cluster, as
of the last tick of its background task, 50 ms ago at most. It takes a lock
the request path never takes.

```rust
let stats = guard.stats();
// Since the instance started.
println!("{} admitted, {} denied", stats.admitted, stats.denied);
```

| Field | What a dashboard learns from it |
|---|---|
| `admitted`, `denied` | What this instance decided; in a dry run, `denied` counts what it would have denied |
| `overflowed` | Checks on keys with no place of their own in the key table: if it grows, raise `tracked_keys` |
| `cluster_size`, `counted_size` | Members seen, and the N the limit is shared over. `counted_size` stays higher under `HoldDown` while a lost member is held, and is at least `max_members` while `members_full`; that is a floor on this node's shares, not a guarantee for the fleet, see `max_members(n)` |
| `suspects` | Members not heard from lately; many of them, a network in trouble |
| `joining`, `learning`, `without_quorum`, `members_full` | Why every key is held at the floor `R × β / N`, if one is true: no peer reached yet, peers not yet told this node's demand, no majority under `Quorum`, or a newcomer refused for a full member table in the last 10 minutes (N is then at least `max_members`) |
| `tracked_keys`, `hot_keys` | Keys with state here, and keys coordinated with the peers |
| `since_last_round` | Far over the protocol period, the background task is stuck |
| `running`, `dry_run` | Whether the node still runs, and whether this is a dry run |
| `most_denied` | The keys denied most lately (ten, or `most_denied(k)`), with their `denied_per_sec`, `requests_per_sec` and `shadow_denied_per_sec`, all over the same ten seconds or so: who is being limited, and what share of its requests. By hash: match them with `guard.key_hash_in(class, key)` |
| `shadow_denied`, `most_shadow_denied` | What shadow limits would have denied, in all, and by key, sorted by it: who a new limit would limit |
| `unknown_class` | Checks in a class this instance does not know, held to the default limit |
| `identity_conflicts` | Times this instance had to outbid an `Alive` record of itself it did not make. A restart quicker than its peers noticed adds one; steady growth means two instances advertise one address |
| `members_refused` | News of members not seen before, refused because the member table was full (`max_members(n)`). Steady growth means a fleet bigger than the cap, or a stranger flooding unsigned gossip with made-up members |
| `gossip_auth`, `rejected_datagrams` | How this instance signs its gossip, and the datagrams it refused, by reason: `unsigned`, `bad_tag`, `malformed` (no message, or a sender under an ID its address does not give), and `mismatched_records`: not datagrams but records after the sender's, under an ID their address does not give, dropped from a datagram otherwise taken. A growing `bad_tag`, secrets that do not match the peers'; a growing `unsigned`, a peer not signing yet, or a stranger |

`guard.share(key)` is the rate one key is admitted at on this instance now:
its share of the limit, before the burst; `share_in(class, key)` the same in a
class.

For one key, ask the background task:

```rust
if let Some(k) = guard.key_stats("pro", "tenant:42").await {
    let (requests, denied) = (k.requests_per_sec, k.denied_per_sec);
    println!("{requests:.0} requests a second, {denied:.0} denied");
}
```

A `KeyStats` has `requests_per_sec`, `denied_per_sec`, `shadow_denied_per_sec`,
`share`, the `limit` the key is held to, its own or its class's, and its
`shadow`. `None` means this instance keeps no state for the key, and asking
does not make it. Every number is this instance's alone, averaged over the
last ten seconds or so; to get the fleet's, add them up.

With the `metrics` feature the background task publishes the same, labelled
`guard` with the address the instance advertises:
`rateguard_checks_total{decision="allow"|"deny"}`,
`rateguard_overflowed_total`, `rateguard_unknown_class_total`, and the gauges
`rateguard_cluster_size`, `rateguard_counted_size`, `rateguard_suspects`,
`rateguard_tracked_keys`, `rateguard_hot_keys`,
`rateguard_held{reason="joining"|"learning"|"without_quorum"|"members_full"}`,
`rateguard_dry_run`, the counters `rateguard_shadow_denied_total`,
`rateguard_identity_conflicts_total`, `rateguard_members_refused_total` and
`rateguard_rejected_datagrams_total{reason="unsigned"|"bad_tag"|"malformed"|"mismatched_records"}`
and `rateguard_gossip_auth{mode="off"|"transitional"|"required"}`, 1 for the
instance's mode: everything but `most_denied` and `most_shadow_denied`, whose
key hashes as labels would make a series per key. Install any `metrics`
recorder, Prometheus or another; nothing is published on the request path.

### Dry run

To try a limit on real traffic before enforcing it, start with
`.dry_run(true)`: `check()` then admits every request, and counts those it
would have denied in `stats().denied`. When the numbers look right, turn
enforcement on with no restart, from any clone of the guard:

```rust
guard.set_dry_run(false);
```

A denied request in a dry run uses up nothing, as when enforcing: the count
of would-be denials is the count enforcement would give.

### Without the network

`rateguard-core` is the whole protocol without the I/O: enforcement and
membership, no sockets, no tokio, time passed in as a parameter. Bring your
own UDP socket and timer, feed it events, and send the datagrams it returns.

```rust
use rateguard_core::{
    boundary::{Action, Event, PeerId},
    gcra::Decision,
    limiter::{Config, Limit},
    node::{Node, SwimConfig},
};

const ONE_SEC: u64 = 1_000_000_000;

let mut node = Node::new(
    Config {
        limit: Limit {
            rate: 1_000.0,
            burst: 10,
        },
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
for action in node.handle(Event::Tick, now_nanos) {
    if let Action::SendTo { peer, bytes } = action {
        send(*peer, bytes);
    }
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
2. A healthy cluster in steady state admits between `R × (1 − ε)` and `R`,
   as long as it fits in `max_members(n)`; over it `R` is not guaranteed,
   see [Security](#security).
3. Under partition, the bounds above, according to the selected policy.
4. Memory is bounded by configuration: the coordination state is
   `O(hot_keys × N)`, and the per-key table is allocated once: two 64-byte
   slots per tracked key, one cache line each, rounded up to a power of two
   (512 KB at the default 4096).
5. Bandwidth in a healthy cluster is two datagrams per node per protocol
   period: the node's own probe and, on average, one answer to a probe of it.

## Accuracy

Every scenario below runs in the simulator on the settings `rateguard` ships
with: five instances, a limit of 1000 per second on one key, `β = 0.05`,
64 hot keys, a burst of 50 on each instance. The table is generated, not
written: `cargo run -p rateguard-sim --release --bin accuracy -- --write`,
and CI fails if it falls out of date.

<!-- accuracy: begin -->
| Scenario | Design bound | Admitted, steady | Peak second | Seconds over 1.01 R | Settles in |
|---|---|---|---|---|---|
| Even demand, 5 × 400 | R | 1.00 R | 1.00 R | 0 | 3 s |
| Skewed demand, 1200 + 4 × 150 | R | 1.00 R | 1.00 R | 0 | 3 s |
| Demand on 1 of 5 nodes | ≥ R × (1 − 4β/5) = 0.96 R | 0.96 R | 0.96 R | 0 | 2 s |
| Skewed, 30% packet loss | R | 1.00 R | 1.00 R | 0 | 2 s |
| Skewed, 20% duplicated, half delayed up to 6 periods | R | 1.00 R | 1.00 R | 0 | 2 s |
| 3/2 split, Optimistic | up to k × R = 2 R | 1.80 R | 1.80 R | 37 | 4 s |
| 3/2 split, HoldDown(10 s) | ≈ R for 10 s, then up to 2 R | 1.80 R | 1.80 R | 24 | 18 s |
| 3/2 split, HoldDown(60 s), longer than the split | ≈ R | 1.00 R | 1.00 R | 0 | 0 s |
| 3/2 split, Quorum | R + the minority's floors = 1.02 R | 1.02 R | 1.02 R | 33 | 4 s |
| One-way link, 0 → 1 lost | R | 1.00 R | 1.00 R | 0 | 0 s |
| GC pause, 5 s on node 4 (the other four) | their shares unchanged | 0.80 R | 0.80 R | 0 | 9 s |
| Rolling restart, one node every 8 s | ≤ R, no second without service | 1.00 R | 1.00 R | 0 | 2 s |
| Node 4 crashes for good (the other four) | R once the suspicion and the hold are over | 1.00 R | 1.00 R | 0 | 17 s |
| Node 4 leaves for good (the other four) | R at once | 1.00 R | 1.00 R | 0 | 1 s |
| Traffic jumps 100×, cold to hot | R | 1.00 R | 1.00 R | 0 | 3 s |
| A node joins 10 s late | R | 1.00 R | 1.01 R | 0 | 1 s |
| Limit lowered to R/2, one node a second | ≤ R in passing, R/2 after | 0.50 R | 1.00 R | 0 | 3 s |
| Limit raised from R/2 to R, one node a second | ≤ R, then R | 1.00 R | 1.00 R | 0 | 3 s |

Shadow at R/5, skewed demand on R: shadow denials vs. enforced, 1600.0 vs. 1600.0 a second (+0.0%).
<!-- accuracy: end -->

*Admitted, steady* is the rate once the scenario has settled, over a window
after its event; *peak second* the busiest second of the whole run;
*settles in* the time from the event until every second stays within 5% of
the steady rate. Bursts let an instance admit a little over its share now and
then, which is why only seconds above 1.01 R are counted as over.

Two rows deserve a word. Under `Quorum` the minority keeps its floors, which
is the `ε` of `R + ε`: 2% here. And after a GC pause the paused instance
works through the requests that queued up meanwhile, its demand estimate
jumps, and the others give way for a few seconds: the cluster dips *under*
the limit, never over it.

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
note that it can be signed but is never encrypted; see [Security](#security).

## Security

By default the gossip protocol trusts its network. Datagrams are neither
encrypted nor authenticated: anyone who can reach the UDP port can join the
cluster, announce demand that shifts the shares, or declare members dead or
gone. Keep the port on a private network, or behind a firewall that lets in
only your own instances, and sign the gossip.

Two values only the cluster's nodes know close what is open:

- **A gossip secret**, the same on every node. Each datagram carries an
  HMAC-SHA256 tag made with it, cut to 16 bytes: 18 bytes more per
  datagram. Under `GossipAuth::Required` a node drops every datagram
  without a valid tag, so a stranger can neither join nor forge news of
  members or demand.
- **A key salt**, against chosen keys. Where clients pick the keys, an IP
  address or a user name, one of them could search for a string that
  hashes like someone else's key and spend that key's limit. With a salt,
  keys are hashed with SipHash-1-3 keyed by it rather than xxh3, and
  nobody without the salt can.

```rust
use rateguard::{GossipAuth, Secret};

let secret = Secret::new(std::fs::read("/etc/rateguard/gossip-secret")?)?;
let guard = Guard::builder()
    .bind("0.0.0.0:7946")
    .advertise("10.0.0.3:7946")
    .seeds(["10.0.0.1:7946", "10.0.0.2:7946"])
    .limit(1_000)
    .gossip_auth(GossipAuth::Required, vec![secret])
    .key_salt(std::fs::read("/etc/rateguard/key-salt")?)
    .spawn()?;
```

A secret is at least 32 bytes and a salt at least 16, random ones, such as
`openssl rand 32 > gossip-secret` and `openssl rand 16 > key-salt`. Neither
shows in `Debug`, errors, stats or metrics. The sidecar takes both from
files, a Kubernetes Secret: see its
[README](https://github.com/donmiro/rateguard/tree/main/rateguard-sidecar#signed-gossip).
Its secret file holds each secret as a line of base64, not raw bytes: in a
fleet of Rust nodes and sidecars, give `Secret::new` the decoded line, or
the secrets differ and the cluster splits.

What this does not protect against:

- **Eavesdropping.** Nothing is encrypted: whoever sees the traffic reads
  the nodes' addresses, the hashes of keys and their rates.
- **Replay.** A datagram recorded on the way can be sent again, and is
  taken. News of a member loses to newer news, by incarnation, but a
  recorded leave can remove a node again after it restarts, until it
  refutes: that is open (spec §9.1, item 37). A replayed demand report
  makes a node only stricter, never looser, with one exception: a recording
  0.75 to 1 of a round circle old (the round is a `u16`, so about 2.7 to
  3.6 hours at the default 200 ms period) can lower a peer's demand for one
  gap between its reports, about N/2 rounds. Closing that takes a wider
  round on the wire. A reordered datagram may make a node stricter for one
  stale window. A ping's reply must match the probe in flight, and a
  probe's sequence number starts at random, so a recorded reply cannot
  match a probe of a later run. Guarding against replay in full would take
  synchronised clocks, or counters per sender that survive restarts, for no
  gain yet.
- **A node gone rogue.** There is one secret per cluster, and whoever holds
  it can speak for any node.
- **Salts that differ between nodes.** Nothing detects them; see below.

**Without signing**, two checks still bound what a stranger can do. Every
member in a datagram must go by the ID its address gives, as every node
derives its own: a datagram whose sender does not is refused as
`malformed`, and any later record that does not is dropped alone, counted
in `mismatched_records`, so that a peer passing on a forger's news is still
heard. That stops IDs detached from their addresses. It does not stop a stranger from giving a
real member's address, whose ID anyone can compute; keeping out that
impersonation takes signing. And an instance keeps at most
`max_members(n)` members, 8192 by default: news of a newcomer is refused
while its table is full, and counted in `members_refused`. Size it for the
fleet plus every address that churned in the last 10 minutes, as the dead
keep their slots that long: about twice the fleet for rolling deploys that
give pods new IPs; the default covers fleets up to about 4000 instances
under rolling deploys. **Over the cap the limit is not guaranteed.** The
instances that refused a newcomer count the fleet short, and some can
admit more than their share. While its table refuses newcomers, an
instance cannot count them, so it shares the limit as if `max_members`
nodes were up and holds every key at the floor `R × β / max_members`
(`members_full`, with `members_refused` and
`rateguard_held{reason="members_full"}` to show it). That softens an
overflow but does not close it, for two reasons. The refused instance
itself never holds: its own table is not full, so it shares the limit over
the small view it has. And an instance that heard of the newcomer only
second-hand lifts its hold after `tombstone_ttl` while the overflow lasts,
because the gossip about the refused one has run out of retransmits. In
the simulator, ten instances with a cap of 4 admitted 1501 a second
against a bound of 1300, and a fleet over its cap went up to 1.3 times
the limit. Treat any growth
of `members_refused` in a legitimate fleet as a reason to raise the cap
now. That cap bounds memory, not availability. A stranger can still fill the table with
made-up members at addresses of its choosing, and every key falls to that
floor; real newcomers wait for a slot until the made-up ones are found dead
and forgotten. Only signing keeps strangers out. Steady growth of
`members_refused` means either a fleet bigger than the cap or such a
flood.

**Turning signing on** in a running cluster takes three rolling updates,
each on every node before the next starts:

1. Every node on 0.6 with no secret, `GossipAuth::Off`. On the wire that is
   0.5, so old and new nodes run side by side. Go on only once no 0.5 node
   is left: 0.5 cannot read a signed datagram.
2. The secret, in `GossipAuth::Transitional`: such a node signs, and takes
   signed datagrams with a valid tag and unsigned ones; a node still `Off`
   takes signed ones without checking them. Old and new hear each other.
   For the minutes of a rollout only: a forgery passes by going unsigned.
3. `GossipAuth::Required`.

**Turning signing off** walks the same steps backwards: `Transitional` on
every node first, then `Off`. Never go from `Required` straight to `Off`:
for the length of that rollout the `Off` nodes send unsigned datagrams,
which the `Required` ones refuse as `unsigned`, and the cluster splits.

**Rotating** from secret A to secret B, in the same way:

1. `[A, B]`: A signs, either verifies.
2. `[B, A]`: B signs, either verifies.
3. `[B]`.

To undo a rotation, walk its steps backwards in the same way.

A node at step 1 and one at step 3 at once do not hear each other, and the
cluster splits: start a step only when the one before has reached every
node. A node whose secrets match nobody's hears nobody, and shows it in
`stats().rejected_datagrams` and in `rateguard_rejected_datagrams_total`:
`bad_tag` grows on secrets that do not match, `unsigned` under `Required`
on a peer not signing yet, still `Off`, or on a stranger. `guard.set_gossip_auth(mode,
secrets)` takes a step with no restart; the sidecar rereads its secret
file.

**The salt** is set when the cluster is created, and is not rotated with
the secret. Nodes exchange demand by the hash of each key: while some hash
with one salt and others with another, or none, they do not know each
other's keys, and each takes its share as if the others' demand were for
other keys, up to N times the limit for N nodes. Nothing tells: the gossip
carries no trace of the salt. So set or change a salt by restarting every
node at once, never by a rolling update, which would admit up to that much
for as long as it lasts. With a salt, match the hashes of `most_denied` with
`guard.key_hash_in(class, key)`, which knows it, not the free function
`key_hash_in`.

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

**And when a node stops on purpose, in a rolling deploy?** If it calls
`guard.leave()`, or drops its last `Guard` while the runtime still runs, it
tells its peers it is going, and they take its share within a round: a deploy
runs at the limit throughout. Requests it still serves afterwards get the
floor `R × β / N`, which its peers no longer count, so draining ones admit a
little over the limit. An instance that just stops looks like a crash: its
peers notice within the suspicion timeout (six seconds), and under the
default `HoldDown` keep its share out of use for the hold, ten seconds, so
the fleet runs a little under the limit meanwhile, never over it. Instances
of a version from before `leave` do not understand the news and treat the
departure as a crash.

**How do I health-check it?** `guard.is_running()` turns false if the
background task has died, which is a bug; the instance then holds every key at
the floor `R × β / N` rather than at shares its peers no longer count.
`guard.cluster_size()` tells you how many members it sees, itself included, and
`guard.stats()` the rest, see [Observability](#observability).

**Does clock skew matter?** No. Nodes never compare timestamps with each other;
each uses only its own monotonic clock, and what travels between them are demand
estimates.

**How much traffic does gossip generate?** In a healthy cluster, two datagrams
per node per protocol period (200 ms by default), each capped at a 1400-byte
MTU. A fifty-node cluster exchanges at most about 700 KB/s in total, no matter
how much traffic it serves. Failures add indirect probes for as long as they
last.

**Can I use it from services that aren't in Rust?** Yes, through
`rateguard-sidecar` and the clients in `sdk/`: see
[From other languages](#from-other-languages). A check then costs a round
trip on loopback, a fraction of a millisecond, rather than 25 ns.

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
