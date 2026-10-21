//! The atomics of [`AtomicGcra`](crate::gcra::AtomicGcra) and
//! [`ShadowGcra`](crate::gcra::ShadowGcra), swapped for loom's under
//! `--cfg rateguard_loom`, as the `rateguard` runtime swaps those of its key
//! table: a slot holds both, and the loom tests of the table explore their
//! races too. A cfg, not a feature: no feature unification can turn it on.

#[cfg(rateguard_loom)]
pub(crate) use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(rateguard_loom))]
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};
