//! The atomics of the key table, swapped for loom's under `--cfg rateguard_loom` (not `loom`: tokio has its own `cfg(loom)` paths) so
//! that its races can be explored exhaustively.

#[cfg(rateguard_loom)]
pub(crate) use loom::sync::atomic::{AtomicU64, Ordering, fence};
#[cfg(not(rateguard_loom))]
pub(crate) use std::sync::atomic::{AtomicU64, Ordering, fence};
