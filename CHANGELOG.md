# Changelog

All notable changes to the published crates (`rateguard`, `rateguard-core`,
`rateguard-proto`) are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crates
follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/donmiro/rateguard/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/donmiro/rateguard/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/donmiro/rateguard/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/donmiro/rateguard/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/donmiro/rateguard/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/donmiro/rateguard/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/donmiro/rateguard/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/donmiro/rateguard/releases/tag/v0.1.0
