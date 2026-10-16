//! The request path.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rateguard_core::gcra;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::classes::Classes;
use crate::config::Error;
use crate::key::{class_hash, hash_in};
use crate::limit::Limit;
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
    /// The classes of limits, the default one included.
    pub classes: Classes,
    /// The rate a key not seen before starts with, as a part of its R, as
    /// an `f64`'s bits; kept current by the background task.
    pub fresh_factor: AtomicU64,
    /// The quota free slots carry, packed: the strictest of any class.
    pub free_quota: AtomicU64,
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

    /// The quota of a key taking its first slot in the class with this
    /// hash.
    pub fn fresh_quota(&self, class: u64) -> gcra::Quota {
        let limit = self.classes.limit_of(class);
        let factor = f64::from_bits(self.fresh_factor.load(Ordering::Relaxed));
        gcra::Quota::per_second(limit.per_second as f64 * factor, limit.burst)
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
        self.check_hashed(0, hash_in(0, key))
    }

    /// Like [`check`](Guard::check), for a key of the class `class`: at
    /// that class's limit. One key in two classes is two keys. A class this
    /// node does not know, a typo or one since removed, takes the default
    /// limit, and its checks are counted in
    /// [`Stats::unknown_class`](crate::Stats::unknown_class).
    ///
    /// ```no_run
    /// # fn run(guard: rateguard::Guard) {
    /// let decision = guard.check_in("pro", "tenant:42");
    /// # }
    /// ```
    pub fn check_in(&self, class: &str, key: &str) -> Decision {
        let class = class_hash(class);
        self.check_hashed(class, hash_in(class, key))
    }

    fn check_hashed(&self, class: u64, key: u64) -> Decision {
        let shared = &*self.shared;
        let now = shared.now();
        let (slot, taken) = shared.table.claim(key);
        // A key is always of one class, the class being part of its hash,
        // so the slot is told it on every check: that also puts it back if
        // a thread that lost a race for another key's slot wrote its own
        // there late. The overflow slot is every class's at once.
        let mend = slot.class.load(Ordering::Relaxed) != class && !shared.table.is_overflow(slot);
        if mend {
            slot.class.store(class, Ordering::Relaxed);
        }
        // Then the attempt, released after the class: the background task
        // that takes the attempt sees the class it belongs to. Counted
        // before the GCRA step, so that a slot with an attempt to count is
        // not freed under the check, see the background task's tick.
        slot.count_attempt(if mend {
            Ordering::Release
        } else {
            Ordering::Relaxed
        });
        // A slot taken just now carries the strictest quota of any class:
        // it gets its own class's. If the slot went to another key
        // meanwhile, a race for this one lost after all, the quota goes
        // back to the free slots' strict one, for the background task to
        // set right at its next tick.
        if taken {
            slot.gcra.set_quota(shared.fresh_quota(class), now);
            if !shared.table.holds(slot, key) {
                let free = gcra::Quota::unpack(shared.free_quota.load(Ordering::Relaxed));
                slot.gcra.set_quota(free, now);
            }
        }
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
        self.share_in("", key)
    }

    /// Like [`share`](Guard::share), for a key of the class `class`.
    pub fn share_in(&self, class: &str, key: &str) -> Option<f64> {
        let slot = self.shared.table.find(hash_in(class_hash(class), key))?;
        Some(slot.gcra.quota().rate_per_sec())
    }

    /// Adds the class `name` or changes its limit, for every clone of this
    /// `Guard`. New keys of the class take it at once, known ones from the
    /// next tick of the background task.
    ///
    /// Every instance of the service must be given the same classes:
    /// while one has a class's new limit and another the old, the fleet
    /// admits no more than the higher of the two. A raised limit is never
    /// overshot; a lowered one takes effect once every instance has it.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyClassName`]: `""` is the default class, see
    /// [`set_limit`](Guard::set_limit). [`Error::TooManyClasses`] for a new
    /// class beyond [`Builder::max_classes`](crate::Builder::max_classes).
    /// [`Error::ZeroLimit`] and [`Error::TooLargeBurst`], as for
    /// [`Builder::limit`](crate::Builder::limit).
    pub fn set_class(&self, name: &str, limit: Limit) -> Result<(), Error> {
        self.shared.classes.set(name, limit)
    }

    /// Removes the class `name`: its keys take the default limit from then
    /// on. Whether there was such a class.
    pub fn remove_class(&self, name: &str) -> bool {
        self.shared.classes.remove(name)
    }

    /// Changes the limit of the default class, the one of
    /// [`check`](Guard::check); as [`set_class`](Guard::set_class) does.
    ///
    /// # Errors
    ///
    /// [`Error::ZeroLimit`] and [`Error::TooLargeBurst`].
    pub fn set_limit(&self, limit: Limit) -> Result<(), Error> {
        self.shared.classes.set_default(limit)
    }

    /// Replaces every named class at once: those not listed are removed.
    /// All or nothing: on an error no class changes. For applying a whole
    /// set of tariffs from a config file or a control plane.
    ///
    /// # Errors
    ///
    /// As [`set_class`](Guard::set_class).
    pub fn set_classes<S: Into<String>>(
        &self,
        classes: impl IntoIterator<Item = (S, Limit)>,
    ) -> Result<(), Error> {
        self.shared.classes.replace(classes)
    }

    /// The named classes and their limits, in no particular order.
    pub fn classes(&self) -> Vec<(String, Limit)> {
        self.shared.classes.list()
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

#[cfg(all(test, not(rateguard_loom)))]
mod tests {
    use super::*;

    // Two threads taking slots for one new key at once may both think they
    // took one; the one whose copy is given back may still write its class
    // into the slot after another key has taken it. That key's next check
    // puts its own class back.
    #[tokio::test]
    async fn a_slot_with_another_key_s_class_gets_its_own_back_at_the_next_check() {
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let guard = Guard::builder()
            .bind(format!("127.0.0.1:{port}"))
            .limit(1000)
            .class("pro", Limit::per_second(100))
            .spawn()
            .unwrap();
        let _ = guard.check_in("pro", "k");
        let slot = guard
            .shared
            .table
            .find(hash_in(class_hash("pro"), "k"))
            .unwrap();
        assert_eq!(slot.class.load(Ordering::Relaxed), class_hash("pro"));
        slot.class.store(class_hash("free"), Ordering::Relaxed);
        let _ = guard.check_in("pro", "k");
        assert_eq!(slot.class.load(Ordering::Relaxed), class_hash("pro"));
    }
}
