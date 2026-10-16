//! The request path.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rateguard_core::gcra;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::key::key_hash;
use crate::stats::{Report, Stats};
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
    /// Cleared if the background task dies, or once the node has left.
    pub running: AtomicBool,
    /// Asks the background task to leave; it answers once it has.
    pub leave: mpsc::Sender<oneshot::Sender<()>>,
    /// Admit what would be denied; read on the way to a denial only.
    pub dry_run: AtomicBool,
    /// For `Guard::stats`; never taken on the request path.
    pub report: Mutex<Report>,
    /// The `guard` label of the metrics: the address the node advertises.
    #[cfg(feature = "metrics")]
    pub metrics_label: String,
}
impl Shared {
    /// Nanoseconds since the node started, on tokio's clock: the OS's
    /// monotonic clock in production, simulated time under a simulator.
    pub fn now(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    /// The report, even if the background task panicked holding it: what
    /// it left is still worth showing.
    pub fn report(&self) -> MutexGuard<'_, Report> {
        self.report.lock().unwrap_or_else(PoisonError::into_inner)
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
    /// The key is hashed here, at a cost that grows with its length: keep
    /// keys short, or hash long ones yourself.
    pub fn check(&self, key: &str) -> Decision {
        let shared = &*self.shared;
        let now = shared.now();
        let slot = shared.table.slot(key_hash(key));
        slot.count_attempt();
        match slot.gcra.check(now) {
            gcra::Decision::Allow => Decision::Allow,
            gcra::Decision::Deny { retry_at } => {
                slot.count_denial();
                if shared.dry_run.load(Ordering::Relaxed) {
                    return Decision::Allow;
                }
                Decision::Deny {
                    retry_after: Duration::from_nanos(retry_at - now),
                }
            }
        }
    }

    /// The node's state as of the last tick of its background task, for a
    /// dashboard or a health check: what it admitted and denied, how it
    /// sees the cluster, and why, if it does, it holds every key at the
    /// floor. Takes a lock the request path never takes.
    pub fn stats(&self) -> Stats {
        let shared = &*self.shared;
        let report = shared.report();
        let mut stats = report.stats.clone();
        stats.admitted = report.attempts.saturating_sub(stats.denied);
        stats.since_last_round = report
            .last_round
            .map(|at| Duration::from_nanos(shared.now().saturating_sub(at)));
        stats.running = self.is_running();
        stats.dry_run = shared.dry_run.load(Ordering::Relaxed);
        stats
    }

    /// The rate, in requests per second, this instance admits for `key`
    /// now, on top of its burst: its share of the limit. `None` if it keeps
    /// no state for the key; the key's next request then starts at the
    /// rate of a new key. Never takes a place in the key table.
    pub fn share(&self, key: &str) -> Option<f64> {
        let slot = self.shared.table.find(key_hash(key))?;
        Some(slot.gcra.quota().rate_per_sec())
    }

    /// Starts or ends a dry run, for every clone of this `Guard`: when on,
    /// [`check`](Guard::check) admits every request and counts those it
    /// would have denied in [`Stats::denied`]. For trying a limit on real
    /// traffic first, then enforcing it with no restart.
    pub fn set_dry_run(&self, on: bool) {
        self.shared.dry_run.store(on, Ordering::Relaxed);
    }

    /// The nodes this one counts in the cluster, itself included.
    pub fn cluster_size(&self) -> usize {
        self.shared.cluster_size.load(Ordering::Relaxed)
    }

    /// Whether the background task still runs. If it died, a bug, the node
    /// is gone from its peers' view and holds every key at a small floor
    /// rather than at shares nobody counts any more: worth a health check.
    /// False too once the node has [left](Guard::leave).
    pub fn is_running(&self) -> bool {
        self.shared.running.load(Ordering::Relaxed)
    }

    /// Leaves the cluster for good, telling the other instances so. They
    /// take this one's share at once, where a node that just stops costs
    /// them its share for the suspicion timeout and, under the default
    /// `HoldDown`, the hold after it: a deploy or a scale-down that leaves
    /// runs at the limit throughout, not under it.
    ///
    /// Call it when the instance starts shutting down. Its peers no longer
    /// count it afterwards, so it holds every key at the floor `R × β / N`
    /// from then on, a little over the limit for the requests still
    /// draining, and [`is_running`](Guard::is_running) turns false.
    /// Dropping the last `Guard` leaves too, if the runtime is still there
    /// to send the news.
    ///
    /// Returns once the news has been handed to the transport, or at once
    /// if the node has already left or stopped.
    pub async fn leave(&self) {
        let (done, left) = oneshot::channel();
        if self.shared.leave.send(done).await.is_ok() {
            let _ = left.await;
        }
    }
}
