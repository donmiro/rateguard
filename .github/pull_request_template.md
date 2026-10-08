## What and why

<!-- What the change does, and the problem it solves. Link the issue if there is one. -->

## How it is tested

<!-- The test that fails without the change: a unit test, a simulator scenario, a loom or turmoil test. -->

## Checklist

- [ ] The checks of CONTRIBUTING.md pass locally (fmt, clippy, docs, tests).
- [ ] If the accuracy numbers moved, the README table is regenerated (`cargo run -p rateguard-sim --release --bin accuracy -- --write`).
- [ ] Public items are documented; README and CHANGELOG updated if users see the change.
- [ ] No `unsafe`, and nothing on the `check()` path that can block or do I/O.
