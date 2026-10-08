//! The request path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rateguard_core::gcra;
use tokio::time::Instant;

use crate::key::key_hash;
use crate::table::KeyTable;

/// The answer to a request. Two answers and no more, ever: matching both is
/// safe across versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a request is only limited if the decision is acted on"]
pub enum Decision {
    /// Serve the request.
    Allow,
    /// Denied; a retry is admitted no sooner than `retry_after`.
    Deny {
        /// How long until a retry may be admitted, unless other requests
        /// take that slot first.
        retry_after: Duration,
    },
}
impl Decision {
    /// Whether the request is to be served.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow)
    }
}

/// What the request path and the background task share.
#[derive(Debug)]
pub(crate) struct Shared {
    pub table: KeyTable,
    pub cluster_size: AtomicUsize,
    pub epoch: Instant,
}
impl Shared {
    /// Nanoseconds since the node started, on tokio's clock: the OS's
    /// monotonic clock in production, simulated time under a simulator.
    pub fn now(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }
}

/// The handle every request goes through: cheap to clone, `Send + Sync`.
/// The node runs as long as one clone is alive.
#[derive(Clone)]
pub struct Guard {
    pub(crate) shared: Arc<Shared>,
}
impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("cluster_size", &self.cluster_size())
            .field("key_slots", &self.shared.table.capacity())
            .finish()
    }
}
impl Guard {
    /// Starts configuring a node; see [`Builder`](crate::Builder).
    pub fn builder() -> crate::Builder {
        crate::Builder::new()
    }

    /// Admits or denies one request for `key`. No I/O, no lock shared with
    /// the network, the same latency whatever the state of the cluster.
    pub fn check(&self, key: &str) -> Decision {
        let shared = &*self.shared;
        let now = shared.now();
        let slot = shared.table.slot(key_hash(key));
        if !shared.table.is_overflow(slot) {
            slot.attempts.fetch_add(1, crate::sync::Ordering::Relaxed);
        }
        match slot.gcra.check(now) {
            gcra::Decision::Allow => Decision::Allow,
            gcra::Decision::Deny { retry_at } => Decision::Deny {
                retry_after: Duration::from_nanos(retry_at - now),
            },
        }
    }

    /// The nodes this one counts in the cluster, itself included.
    pub fn cluster_size(&self) -> usize {
        self.shared.cluster_size.load(Ordering::Relaxed)
    }
}
