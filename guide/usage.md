# Using rateguard

Everything past [getting started](https://github.com/donmiro/rateguard#getting-started): keys, classes of limits, limits per minute and hour, a key's own limit, shadow limits, a dry run, axum, and every option.

## Keys

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

## Classes of limits

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
once every instance has it (see [Accuracy](how-it-works.md#accuracy)).

## Limits per minute and per hour

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

## Limits for one key

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

## Shadow limits

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

## Dry run

To try a limit on real traffic before enforcing it, start with
`.dry_run(true)`: `check()` then admits every request, and counts those it
would have denied in `stats().denied`. When the numbers look right, turn
enforcement on with no restart, from any clone of the guard:

```rust
guard.set_dry_run(false);
```

A denied request in a dry run uses up nothing, as when enforcing: the count
of would-be denials is the count enforcement would give.

## With `axum`

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

## Configuration

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
| `max_members(n)` | `8192` | How many members an instance keeps track of, the dead not yet forgotten and itself included, from 1 to 65,536. Over it newcomers are refused and counted in `members_refused`. Size it for the fleet plus every address that churned in the last 10 minutes, the time the dead are remembered: about twice the fleet for rolling deploys that give pods new IPs. While the table refuses newcomers, every instance shares the limit as if `n` nodes were up and holds every key at the floor `R × β / n`. Over the cap the limit is not guaranteed, as that hold softens an overflow but does not close it: raise the cap when `members_refused` grows. The default covers fleets up to about 4000 instances under rolling deploys; see [Security](security.md) |
| `gossip_auth(mode, secrets)` | `Off`, no secret | Whether gossip is signed, with at most two secrets of 32 bytes or more; see [Security](security.md) |
| `key_salt(bytes)` | none | A salt of 16 bytes or more for the hashes of keys, the same on every node; see [Security](security.md) |
