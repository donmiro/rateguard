# Contributing to rateguard

Thank you for taking the time. Bug reports, failure scenarios the design does
not survive, and review from anyone who has run a gossip protocol in
production are the most valuable contributions right now.

## Reporting a bug

Open an issue with the bug report template. The most useful report says what
the cluster looked like (how many instances, which `partition_policy`, the
limit), what was admitted, and what you expected. If you can reproduce it in
the simulator, the seed is all we need: a failing simulator test prints a line
like `RATEGUARD_SEED=8371`, and running the tests with it replays the failure
exactly.

Security issues go through private reporting instead; see
[SECURITY.md](SECURITY.md).

## The layout

| Crate | What lives there |
|---|---|
| `rateguard-core` | All the logic, sans I/O: enforcement, SWIM membership, allocation, partition policies. Time is a parameter; no tokio, no sockets |
| `rateguard-proto` | The wire format |
| `rateguard-sim` | A deterministic simulator on virtual time, the scenario tests, and the accuracy report. Not published |
| `rateguard` | The runtime: UDP, timers, the lock-free key table, the public `Guard` API |

A change to the protocol belongs in `rateguard-core` and is tested in the
simulator first; `rateguard` only wires it to the network.

## Before you open a pull request

CI runs all of these; running them locally saves a round trip.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo test --workspace --locked

# The key table's races, explored exhaustively with loom.
RUSTFLAGS="--cfg rateguard_loom" LOOM_MAX_PREEMPTIONS=3 \
    cargo test -p rateguard --lib --release --locked table

# The runtime over turmoil's simulated network.
cargo test -p rateguard --features turmoil --locked --test turmoil

# The README's accuracy table must match what the simulator measures.
cargo run -p rateguard-sim --release --locked --bin accuracy -- --check
```

If a change moves the numbers of the accuracy report, regenerate the table and
commit it with the change:

```sh
cargo run -p rateguard-sim --release --bin accuracy -- --write
```

The minimum supported Rust version is 1.88; CI checks the build on it.

## What a good change looks like

- **A test that fails first.** A bug fix comes with a test that fails without
  it; a new behaviour, with the test that pins it down. For the protocol that
  usually means a simulator scenario next to the ones in
  `rateguard-sim/tests/`.
- **The guarantees hold.** The README lists five; the simulator checks them
  after every event. A change that needs one weakened should say so up front,
  with the reason.
- **No `unsafe`, and nothing on the request path that can block.** `check()`
  takes no lock shared with the network and does no I/O; that is the first
  promise of the crate.
- **Public items are documented.** The crates build with `missing_docs`.

Larger changes, a new policy or a change to the wire format, are best
discussed in an issue first.

## License

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as MIT OR Apache-2.0, without any additional
terms or conditions.
