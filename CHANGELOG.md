# Changelog

All notable changes to the published crates (`rateguard`, `rateguard-core`,
`rateguard-proto`) are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crates
follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `examples/cluster.rs`: three instances in one process under one limit,
  behind an unfair load balancer, printing every second what each admits;
  its output opens the README.
- The `rateguard-sidecar` image on `ghcr.io/donmiro/rateguard-sidecar`,
  for amd64 and arm64, published by CI with every release tag; the
  Kubernetes example uses it.
- `rateguard-sidecar/examples/demo`: the same with three sidecars, in
  `docker compose up`.

### Changed

- The README is a quarter of its length, with a comparison to Redis,
  Gubernator, Envoy and governor; the rest, unchanged, is in `guide/`:
  usage, observability, how it works with the accuracy report, security,
  the core without the network, and the FAQ.

## [0.7.1] - 2026-10-22

Fixes from an audit of 0.7.0, and a cap on the members a node keeps. The
wire format is unchanged: nodes of 0.7.0 and 0.7.1 run side by side, and
every fix works when the peer is on 0.7.0.

### Added

- `Builder::max_members(n)`: a bound on the members a node keeps, 8192 by
  default, from 1 to 65,536 (`Error::TooLargeMaxMembers` outside that).
  News of a member the node already knows is always taken; a stranger past
  the cap is refused and not gossiped on, and nobody is evicted. Size it
  for the fleet plus every address that churned in the last 10 minutes
  (`tombstone_ttl`): about twice the fleet for rolling deploys that give
  pods new IPs (the default covers fleets up to about 4000 instances under
  rolling deploys). Over the cap the limit is not guaranteed: a node the
  table refused counts the fleet short and can admit more than its share.
  For 10 minutes after it refused a live newcomer, a node shares the limit
  as if `n` nodes were up and holds every key at the floor `R × β / n`, as
  it cannot count the newcomer nor see its demand. That softens an
  overflow but does not close it: the refused node itself does not hold,
  as its own table is not full, and a node that heard of the newcomer only
  second-hand lifts its hold after `tombstone_ttl` while the overflow
  lasts, once the gossip about it has run out. Treat any growth of
  `members_refused` in a legitimate fleet as a reason to raise the cap now.
  It bounds memory, not availability: a flood of made-up
  members brings every key down to that floor, and real newcomers wait for
  a slot. Only signing keeps strangers out. `rateguard-core`:
  `Node::set_max_members`, `max_members`, `members_refused` and
  `Held::members_full`.
- `Stats::members_full` and, with `metrics`,
  `rateguard_held{reason="members_full"}`, also in the sidecar's
  `/v1/stats`: every key held at the floor over `max_members` because the
  full member table refused a live newcomer in the last 10 minutes.
- `Stats::members_refused` and, with `metrics`,
  `rateguard_members_refused_total`: members refused at the cap. Steady
  growth means a cluster bigger than the cap or a flood.
- `Stats::identity_conflicts` and `rateguard_identity_conflicts_total`
  (`rateguard-core`: `Node::identity_conflicts`): news that outbid this
  node's own incarnation. A quick restart adds one; steady growth means two
  instances advertise one address.
- The Python client's `Decision.error`: why a decision was made without the
  sidecar's answer, whatever `on_error` says.
- `rateguard-sidecar`: `--max-members` (`RATEGUARD_MAX_MEMBERS`),
  `members_refused` and `identity_conflicts` in `/v1/stats`, a warning at
  start when gossip is not signed, and a body limit of 64 KiB on
  `/v1/check`.

### Fixed

- A key that was tracked and idle kept its old limit after a change to its
  key limit, class or the default: the driver only handed the new limit to
  keys that had traffic in the round. It now takes it at the next tick.
- `rateguard-sidecar`: a key with a lone surrogate sent in `class` was
  answered `400`, as 0.5.1 fixed for `key`; the class is read as bytes now.
  A `/v1/check` body over 64 KiB is refused with `413` and a JSON error;
  the admin routes keep 2 MB. A gossip secret file wrapped at 64 characters
  by `openssl base64` is refused with a hint, not read as short secrets.
- The clients took a `413` from the sidecar, a key over its limit, as an
  outage and let the request through, as `on_error` says. A `413` now
  denies, whatever `on_error` is. The TypeScript client checks the status
  before it reads the body, and in Python and TypeScript an empty URL is
  unset.
- A key new to a node had its demand measured from the moment it was first
  seen, not from the last tick, so its first tick read low, and under
  `max_tracked_keys` a heavy new key was evicted in favour of idle ones. It
  is now measured from the previous tick.
- A node that fell behind its own incarnation after a quick restart was
  never told, so its leave went unheard. It is told now, and
  `identity_conflicts` counts the conflict.
- A demand report that was not taken, as a reordered or replayed datagram
  is, did not count for the sender at all. It now counts at the maximum for
  the stale window, so such a datagram can make a node stricter and no more.
  One case stays: a recording 0.75 to 1 of a round circle old (about 2.7 to
  3.6 hours at the default period) can lower a peer's demand for one gap
  between its reports; closing it takes a wider round on the wire. The
  recorded leave replayed after a restart is open (spec §9.1, item 37).
- A probe's sequence number started at 1 on every run, so a reply recorded
  in an earlier run could keep a dead peer alive in a later one. It starts
  at random.
- A key at the saturation point of its TAT, after a dip to the slowest rate
  and back, was handed its whole burst again. It now carries at least the
  larger burst's debt, and `Gcra::set_quota` with an identical quota no
  longer moves a large TAT by rounding.
- A period over 600 s panicked in `Builder::spawn`, the tombstone TTL being
  shorter than the suspicion timeout; the TTL is stretched to the period.
  Changing a class copied the whole table while it held readers out; the
  copy is made outside their lock now.
- A datagram whose sender goes by an ID other than its address gives is
  `malformed`, signed or not; a later record naming a member so is dropped
  alone, counted in the new `RejectedDatagrams::mismatched_records` (also
  `rateguard_rejected_datagrams_total{reason="mismatched_records"}` and in
  the sidecar's `/v1/stats`), so that a 0.7.0 peer passing on a forger's
  news is still heard. The demand of a refused stranger is not
  kept; the members a node keeps are bounded by `max_members`, so a flood of
  made-up members no longer grows its memory without limit.

## [0.7.0] - 2026-10-21

Limits per minute and per hour, limits for single keys, shadow limits for
classes, and counters per key. The wire format is unchanged: nodes of 0.6
and 0.7 run side by side, but a node of 0.6 knows no key limits or shadows,
so in a mixed cluster an exception reaches the share of the nodes of 0.7
only. That is a transition, not a mode to run in.

### Added

- `Limit::per_minute` and `Limit::per_hour`, and `count()`, `period()` and
  `rate_per_sec()` to read a limit back. A `Limit` keeps what it was given:
  `per_minute(60)` and `per_second(1)` admit the same but are not `==`.
  A limit per minute is not a fixed window: GCRA smooths it, so 1000 a
  minute is about 16.7 a second plus the burst, and the burst is per
  instance, so a fleet of N admits up to N times it back to back.
- `Builder::default_limit(Limit)`: the default class at any period.
  `Builder::limit(rps)` stays.
- Limits for single keys: `KeyLimit`, either a `Limit` of its own or
  `KeyLimit::class(name)`, a class followed by name;
  `Guard::set_key_limit`, `set_key_limits` (all or nothing),
  `remove_key_limit` and `key_limits`. The key keeps its hash and its debt,
  so a change gives no fresh burst; while it has not reached every node the
  fleet admits at most the higher of the two limits. `Builder::max_key_limits`
  (4096 by default, at most 2^20), `Error::TooManyKeyLimits` for a table
  that is full and `Error::TooLargeMaxKeyLimits` for a bound over 2^20.
  A big set goes in with `set_key_limits`: each `set_key_limit` copies the
  table.
- Shadow limits for classes: `Guard::set_shadow`, `set_shadows` and
  `shadows`. Every attempt of a key with a slot of its own is also run
  through its class's shadow, the ones the real limit denied too (keys in
  the overflow slot have none); the shadow decides nothing and counts
  what it would have denied. It errs on the strict side: it counts at least
  the denials that enforcing the limit would give. `Error::UnknownClass`
  for a class that does not exist. `Stats::shadow_denied` and, with
  `metrics`, `rateguard_shadow_denied_total`.
- `Guard::check_traced` and `check_in_traced`: a `Checked` of the
  `decision` and the shadow's. They decide as `check` does.
- Counters per key: `DeniedKey` has `requests_per_sec` and
  `shadow_denied_per_sec`, all its rates over the same ten seconds or so,
  a new key's rates so far; the lists rank keys by their plain average,
  so a key denied once just now ranks below one denied as often for ten
  seconds; `Stats::most_shadow_denied`; `Builder::most_denied(k)` sets the
  length of both lists (10 by default, at most 1024,
  `Error::TooManyMostDenied`);
  `Guard::key_stats(class, key)`, an `async fn`, gives one key's
  `KeyStats`, or `None` if the node keeps no state for it.
- `rateguard-core`: `Tally`, `KeyRates`, `ShadowGcra` and
  `Node::check_in_traced`.
- `rateguard-sidecar`: a limit in JSON has exactly one of `per_second`,
  `per_minute` and `per_hour`, with an optional `burst`; `--limit 1000/min`
  and `--class name=5000/h/10`. `PUT /v1/key_limits`, `PUT /v1/shadows`,
  `GET /v1/keys?class=&key=` for one key, on loopback only. `/v1/stats` has
  `shadow_denied` and `most_shadow_denied`, named like `most_denied`, whose
  entries have the new rates. `--most-denied` (`RATEGUARD_MOST_DENIED`).

### Changed

- **Breaking**, `rateguard-core`: `limiter::Limit::per_second: u32` is
  `rate: f64`, requests per second; `limiter::Config` has `limit: Limit` in
  place of `limit_per_sec` and `burst`; `Node::record` takes a `Tally`,
  with the shadow denials, and `Limit`, in place of the counts, and so does
  `Limiter::record`; `Limiter::most_denied` and `Node::most_denied` return
  `Vec<(u64, KeyRates)>`, where they returned the denials a second;
  `limiter::Limit` is no longer `Eq`, its rate being an `f64`; the method
  `limiter::Config::limit()` is gone, `limit` being a field.
- **Breaking**, `rateguard-core`: a quota keeps its interval with a unit
  (40 bits and two of unit), so the slowest it holds is about a request in
  34 years, where it held 18 minutes; for that, `gcra::MAX_BURST` is
  4,194,303, where it was 16,777,215.
- **Breaking**, `rateguard`: a burst over 4,194,303, which 0.6 took, is
  `Error::TooLargeBurst`, from `Builder::spawn`, `set_class` and the
  other setters of a limit.
- `Limit::rate()` is deprecated: it rounds a slow rate up to a whole number.
  Use `rate_per_sec()`.
- A key's tally in a slot has room for a shadow: its slot is still 64 bytes.
  What the 0.4 slot kept in reserve for `check_n` went to the shadow.
- `rateguard-sidecar`: `/v1/stats` and `Args` take the new fields, and
  `Args::limit` is a `LimitSpec { limit, has_burst }`, where it was a
  `u32`; the 0.6 forms of flags and bodies are read as before.

### Fixed

- A rate so slow that its interval did not fit the quota, such as
  `per_hour(1)` on 40 nodes at the floor, was held *looser* than asked: the
  interval was cut, and the docs said the opposite. Slow rates are now held
  exactly, and the docs of `Quota` say what is cut, past 34 years.
- The theoretical arrival time of a key at a very slow rate could overflow
  `max(tat, now) + t`; it saturates now, in `Gcra::check` and
  `AtomicGcra::check`. A burst so long at so slow a rate that
  `interval × (burst − 1)` passes 292 years is cut short there, stricter,
  where it would have let every request through. `rateguard-core` refuses
  a limit whose rate is not finite or not above zero, with a panic as it
  does a zero burst.

## [0.6.0] - 2026-10-20

Signed gossip and salted keys. Without a secret the wire format is
unchanged: a node of 0.6 sends the bytes of 0.5, and 0.5 and 0.6 nodes run
side by side. A cluster turns signing on in steps once every node is on
0.6; see Security in the README.

### Added

- Signed gossip: `Builder::gossip_auth(mode, secrets)` signs every datagram
  with HMAC-SHA256, its tag cut to 16 bytes, in a frame of 18 bytes more.
  `GossipAuth::Off`, the default, sends unsigned and takes signed
  datagrams without checking them; `Transitional` signs and takes signed
  ones with a valid tag and unsigned ones; `Required` takes only signed
  ones with a valid tag. A secret is a `Secret`, 32 bytes or more, never
  shown in `Debug`; errors are `Error::GossipAuth(AuthError)`. The
  framing is the `auth` module of `rateguard-proto`, behind its feature
  `auth`, off by default.
- Rotation without a restart: at most two secrets, the first signs and
  either verifies, so secret A goes to B by `[A, B]`, `[B, A]`, `[B]`.
  `Guard::set_gossip_auth` changes the mode and secrets on the fly.
- `Builder::key_salt`: keys hashed with SipHash-1-3 keyed by a salt of 16
  bytes or more, rather than xxh3, so that a client who picks the keys
  cannot find one that shares another's limit. Set when the cluster is
  created, by restarting every node at once; `Error::ShortSalt` for one
  too short. Without a salt, the hashes are those of 0.5.
- `Guard::key_hash_in(class, key)`: the hash this node gives a key, its
  salt included, for matching `Stats::most_denied`.
- `Stats::gossip_auth` and `Stats::rejected_datagrams`, a
  `RejectedDatagrams` of `unsigned`, `bad_tag` and `malformed`, the last
  counted in every mode: such datagrams were dropped silently before. With
  the `metrics` feature, `rateguard_rejected_datagrams_total{reason}` and
  `rateguard_gossip_auth{mode}`.
- `rateguard-sidecar`: `--gossip-secret-file` (`RATEGUARD_GOSSIP_SECRET_FILE`),
  secrets as base64 lines, reread every 10 s, so that a rotated Kubernetes
  Secret takes effect without a restart; a file refused on a reread leaves
  the secrets in force, said on stderr and counted in
  `rateguard_gossip_secret_reload_errors_total`. `--gossip-auth`
  (`RATEGUARD_GOSSIP_AUTH`), `required` with a secret file and `off`
  without. `--key-salt-file` (`RATEGUARD_KEY_SALT_FILE`), read once.
  `/v1/stats` gains `gossip_auth` and `rejected_datagrams`. The Kubernetes
  example mounts both from a Secret.
- `rateguard-sidecar`: the entries of `most_denied` in `/v1/stats` carry
  the denied `key` and its `class`, remembered when it is denied, for the
  last 1024 keys; a key over 256 bytes is cut, with `key_truncated`. Keys
  may be personal data, and are served on the loopback API only, never on
  `--metrics-listen`.

### Changed

- `rateguard-sidecar` as a library: `Args::builder()` returns
  `Result<Builder, ArgsError>`, since it reads the secret and salt files;
  `App` has a new field, `denied`, so a struct literal no longer builds:
  make one with `App::new(guard, admin, metrics)`. New: `reload_secrets`
  and the modules `secrets` and `denied`; `denied::Named` is
  `#[non_exhaustive]`.

### Fixed

- A demand report from a peer's previous epoch, sent before it restarted
  and arriving late or replayed, replaced the peer's current snapshot for a
  round. Reports of a peer's last four epochs before the current one are
  now ignored.

## [0.5.1] - 2026-10-19

### Added

- `Builder::quorum_memory` and `Node::set_quorum_memory`: how long a node
  under `PartitionPolicy::Quorum` without a majority keeps counting it
  against the cluster it last saw. An hour by default; see Fixed.

### Fixed

- Under `PartitionPolicy::Quorum`, nodes that crashed without leaving and
  came back under new addresses held the whole cluster at the floor for
  good: the survivors counted them as dead members that might be serving
  on the other side of a split, and the size a majority is counted
  against never shrank without one. It now does after the quorum memory.
- At `max_tracked_keys`, a key seen only lately was dropped first, round
  after round, however busy: its demand was ranked while its average still
  showed a fraction of it. Keys are now ranked on the average without the
  zero it started from (`Demand::unbiased_rate`).
- A slot held for a key the node no longer tracks, while it was in debt,
  kept the quota it had: changes to its class's limit, to the cluster's
  size or to the floor did not reach it. It now gets what a new key of its
  class gets.
- `rateguard-sidecar` answered `400` to a key with a lone surrogate, which
  Python's `json.dumps` and JavaScript's `JSON.stringify` send as readily as
  any other key. The clients take a `400` for an outage and fail open, so
  whoever chose the key went unlimited. Such a key is now checked, its
  invalid characters replaced as the Go client does.
- `rateguard-sidecar` never closed a connection on `--metrics-listen`, the
  port open to the pod's network: one is now closed once it has waited
  10 s for a request's head, idle or sending it slowly.
- The Python client kept the connection of every thread it ever served
  open for good; one is now closed when its thread ends.

## [0.5.0] - 2026-10-16

The library is unchanged; this release adds the sidecar and its clients.

### Added

- `rateguard-sidecar`, a new crate and program: rateguard for services in
  any language, over HTTP on loopback. `POST /v1/check`, `GET /v1/stats`,
  `GET /healthz`, Prometheus `GET /metrics`; with `--admin`, `PUT
  /v1/classes`, `/v1/limit` and `/v1/dry_run`. Configured by flags or
  `RATEGUARD_*` variables; `--metrics-listen` serves health and metrics
  alone to the pod's network. On SIGTERM it leaves the cluster and goes on
  answering, at the floor, for `--drain` (10 s), while the service next to
  it drains. A Dockerfile and a Kubernetes example come with it.
- Clients for the sidecar in `sdk/`: Python (`rateguard-client` on PyPI,
  module `rateguard_client`), Go (`github.com/donmiro/rateguard/sdk/go`) and
  TypeScript (`rateguard-client` on npm), standard
  library only, a 50 ms timeout, and the request allowed when the sidecar
  cannot answer unless told to deny. A path in the sidecar's URL is kept.

## [0.4.0] - 2026-10-16

The wire format is the same: 0.4 and 0.3 instances run side by side, but
classes are a 0.4 feature, and an instance of 0.3 holds every key to its
one limit.

### Added

- Classes of limits: `Builder::class(name, Limit)`, `Guard::check_in(class,
  key)`. One key in two classes is two keys; a class an instance does not
  know takes the default limit and is counted in `Stats::unknown_class`.
- Classes changed while the fleet runs, from any clone: `Guard::set_class`,
  `remove_class`, `set_limit` (the default class), `set_classes` (all named
  classes at once, all or nothing), `classes`. While instances disagree on
  a limit the fleet admits no more than the higher; a raised limit is never
  overshot, a lowered one holds once every instance has it. Two rows in the
  accuracy report measure it.
- `Limit`, `key_hash_in`, `Guard::share_in`, `Builder::max_classes`
  (default 64, at most 4096), and the errors `EmptyClassName` and
  `TooManyClasses`.
- `Stats::most_denied`: up to ten keys denied most lately, by hash, with
  their denials a second.
- `tower`: a key function may return `(class, key)` to check in a class.
- `rateguard-core`: `Limit`, `Config::limit`, `Limiter::check_in`,
  `Limiter::limit`, `Limiter::most_denied`, `Limiter::new_key_factor`,
  `Node::check_in`, `Node::new_key_quota_in`, `Node::new_key_factor`,
  `Node::most_denied`.

### Changed

- Guarantee 4: a key table slot is 64 bytes, one cache line, up from 32:
  512 KB at the default 4096 tracked keys instead of 256 KB. It holds the
  key's class.
- `check()` costs about 0.3 ns more on one thread, a few ns more with eight
  threads on keys of their own: every check reads the slot's class, which
  also mends a slot whose class a lost race for another key overwrote.
- **Breaking**, `rateguard-core`: every key has a limit of its own.
  `Node::record(key, attempts, denials, limit, now)` replaces
  `record_attempts`; `Limiter::record` likewise. `Limiter::set_cap` takes a
  part of each key's R, not a rate; `Limiter::new_key_quota` takes the key's
  limit; the share function of `Limiter::tick_with_shares` is given the key's
  R as a third argument.
- **Breaking**, `rateguard`, feature `tower`: a key function returns any
  `rateguard::tower::Key`: `String`, `&str`, `Box<str>`, `Arc<str>`,
  `Cow<str>`, or a pair `(class, key)`, rather than any `AsRef<str>`.

### Fixed

- The background task could free a key's slot between a check finding or
  taking it and the check itself: the key then started over in a new slot
  with a fresh burst. A check now counts its attempt first, and a slot with
  attempts not yet taken is never freed; what is left is a window of a few
  instructions, at most one request admitted on the freed slot.
- A key taking a slot as the background task refreshed the free ones could
  lose its quota to theirs. `AtomicGcra::set_quota` no longer writes a quota
  that has not changed.

## [0.3.1] - 2026-10-16

### Fixed

- A key whose first requests reached the node in the very tick of a
  protocol round was dropped with them: its demand went uncounted, about
  one new key in four lost up to one tick of attempts, and a key checked
  once right after start never showed in `Stats::tracked_keys`. Never over
  the limit, only under-counted demand. The attempts now wait for the next
  tick.

## [0.3.0] - 2026-10-16

Everything here is additive: no public item changed or went away, and the
wire format is the same, so 0.3 and 0.2 instances run side by side.

### Added

- `Guard::stats`: what an instance admitted and denied since it started,
  checks crowded into the key table's overflow slot, how it sees the
  cluster (members, the N the limit is shared over, suspects), its keys,
  why it holds every key at the floor if it does (`joining`, `learning`,
  `without_quorum`), and how long ago its last round ran. As of the last
  tick of the background task; the request path takes no lock for it, and
  an admission costs nothing more.
- `Guard::share`: the rate one key is admitted at on this instance now.
- Dry run: `Builder::dry_run` and `Guard::set_dry_run`. `check()` admits
  every request and counts those it would have denied in `Stats::denied`;
  enforcement turns on with no restart.
- Feature `metrics`: the background task publishes what `stats()` tells
  through the `metrics` facade, labelled with the instance's address.
- Feature `tower`: `rateguard::tower::RateLimitLayer`, which takes the key
  from each request and answers a denied one with `429` and `Retry-After`.
  The axum example uses it.
- `rateguard-core`: `Node::held` and `Held`, `Node::counted_size`,
  `Quota::rate_per_sec`, `Quota::burst`.

## [0.2.0] - 2026-10-16

This release breaks the API of `rateguard-proto` and `rateguard-core`; that
of `rateguard` only gains `Guard::leave`. On the wire, 0.2 and 0.1 instances
run side by side: a 0.1 instance drops the new `Leave` message and sees the
departure as a crash, as before.

### Changed

- **Breaking**, `rateguard-proto`: `Message` has a new variant, `Leave`, and
  `Message::seq` returns `Option<u32>`, `None` for a `Leave`.
- **Breaking**: `#[non_exhaustive]` on the public enums that may grow:
  `Message` and `Error` in `rateguard-proto`; `Event`, `Action`, `Change`,
  `Applied` and `PartitionPolicy` in `rateguard-core`. A `match` on them
  outside their crate needs a wildcard arm, and later variants will not
  break it again.

### Added

- `Guard::leave`: an instance shutting down tells its peers, and they take
  its share within a round, instead of after the suspicion timeout and,
  under `HoldDown`, the hold. Dropping the last `Guard` leaves too. A deploy
  or a scale-down no longer runs under the limit for about 16 s per
  instance. The news is a new message, `Message::Leave`; versions from
  before it drop the datagram and see the departure as a crash, as before.
  Contributed by [@dspv](https://github.com/dspv) in
  [#3](https://github.com/donmiro/rateguard/pull/3).
- `rateguard-core`: `Node::leave`, `Node::has_left`, `MemberTable::depart`,
  `MemberTable::has_departed`, `MemberTable::joined_at`,
  `SizeHistory::forget_one`.

### Fixed

- Seeds given as names are looked up concurrently, each given up on after
  20 s, so a stalled lookup no longer holds up the other names or the
  rounds after it. A name whose lookup fails or times out keeps its last
  addresses. Contributed by [@dspv](https://github.com/dspv) in
  [#2](https://github.com/donmiro/rateguard/pull/2).

## [0.1.1] - 2026-10-09

### Added

- Seeds given as `host:port`, such as a Kubernetes headless service. A name
  is looked up when the node starts and again while it runs: every reconnect
  interval while the node is alone, every 30 s once it has peers. Until
  it joins, the node holds every key at the floor, even while the name
  resolves to nobody; a name that resolves to the node alone three lookups
  in a row makes it a fleet of one, with the whole limit. Contributed by
  [@dspv](https://github.com/dspv) in
  [#1](https://github.com/donmiro/rateguard/pull/1).
- `rateguard-core`: `Node::remove_seed` and `Node::expect_peers`.

## [0.1.0] - 2026-10-08

The first release.

### Added

- `Guard`: one rate limit per key across a fleet of instances, enforced
  locally in about 25 ns with no I/O and no lock shared with the network;
  configured through `Guard::builder()`.
- Shares of the limit that follow observed demand, exchanged over SWIM
  membership gossip on UDP; only hot keys are coordinated.
- Partition policies `Optimistic`, `HoldDown` (the default, 10 s) and
  `Quorum`, each with its overshoot bound.
- `rateguard-core`: the whole protocol as sans-I/O state machines, for
  bringing your own sockets and timers.
- `rateguard-proto`: the wire format.
- A runnable `axum` middleware example.
- An accuracy report in the README, generated by the simulator.

[Unreleased]: https://github.com/donmiro/rateguard/compare/v0.7.1...HEAD
[0.7.1]: https://github.com/donmiro/rateguard/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/donmiro/rateguard/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/donmiro/rateguard/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/donmiro/rateguard/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/donmiro/rateguard/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/donmiro/rateguard/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/donmiro/rateguard/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/donmiro/rateguard/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/donmiro/rateguard/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/donmiro/rateguard/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/donmiro/rateguard/releases/tag/v0.1.0
