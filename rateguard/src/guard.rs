//! The request path.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Duration;

use rateguard_core::gcra;
use rateguard_proto::auth::{Auth, GossipAuth, Secret};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::classes::{Classes, shadow_quota};
use crate::config::Error;
use crate::key::{KeySalt, class_hash, salted_hash_in};
use crate::key_limits::{KeyLimit, KeyLimits};
use crate::limit::Limit;
use crate::stats::{KeyStats, Report, Stats};
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

    fn at(decision: gcra::Decision, now: u64) -> Self {
        match decision {
            gcra::Decision::Allow => Decision::Allow,
            gcra::Decision::Deny { retry_at } => Decision::Deny {
                retry_after: Duration::from_nanos(retry_at.saturating_sub(now)),
            },
        }
    }
}

/// A decision and what the key's shadow limit would have decided: see
/// [`Guard::check_in_traced`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use = "a request is only limited if the decision is acted on"]
pub struct Checked {
    /// The decision, the same as [`Guard::check_in`]'s.
    pub decision: Decision,
    /// What the shadow would have decided, had it been the limit; `None`
    /// if the key has no shadow. Never acted on by the guard.
    pub shadow: Option<Decision>,
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
    /// Questions for the background task, which alone knows the core's
    /// rates: see `Guard::key_stats`.
    pub queries: mpsc::Sender<crate::driver::Query>,
    /// How long the lists of the keys denied most are.
    pub most_denied: usize,
    /// Admit what would be denied; read on the way to a denial only.
    pub dry_run: AtomicBool,
    /// For `Guard::stats`; never taken on the request path.
    pub report: Mutex<Report>,
    /// The classes of limits, the default one included.
    pub classes: Classes,
    /// The limits of single keys, over their classes'.
    pub key_limits: KeyLimits,
    /// The rate a key not seen before starts with, as a part of its R, as
    /// an `f64`'s bits; kept current by the background task.
    pub fresh_factor: AtomicU64,
    /// The quota free slots carry, packed: the strictest of any class.
    pub free_quota: AtomicU64,
    /// How the gossip is signed and checked: read by the background task
    /// for every datagram, replaced by `Guard::set_gossip_auth` only. Never
    /// taken on the request path.
    pub auth: RwLock<Arc<Auth>>,
    /// The salt of the key hashes, set at the start for good; its `Debug`
    /// shows none of it.
    pub salt: Option<KeySalt>,
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

    /// The quota of the key with this hash taking its first slot in the
    /// class with this hash, and its shadow's, if it has one. The key
    /// limits are read only if there are any, the classes' table only if
    /// some class has a shadow: with neither, a new key of the default
    /// class takes no lock.
    pub fn fresh_quota(&self, class: u64, key: u64) -> (gcra::Quota, Option<gcra::Quota>) {
        let (limit, shadow) = if self.key_limits.is_empty() && !self.classes.has_shadows() {
            (self.classes.limit_of(class), None)
        } else {
            self.key_limits
                .with(|overrides| self.classes.limit_for(overrides, class, key))
        };
        let factor = f64::from_bits(self.fresh_factor.load(Ordering::Relaxed));
        let main = gcra::Quota::per_second(limit.rate * factor, limit.burst);
        (main, shadow_quota(main, limit, shadow))
    }

    /// The report, even if the background task panicked holding it: what
    /// it left is still worth showing.
    pub fn report(&self) -> MutexGuard<'_, Report> {
        self.report.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The current signing and checking, for the datagrams at hand: a
    /// change waits for the next ones.
    pub fn auth(&self) -> Arc<Auth> {
        self.auth
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
        self.check_hashed(0, self.hash(0, key)).decision
    }

    /// Like [`check`](Guard::check), telling what the default class's
    /// shadow would have decided too; see
    /// [`check_in_traced`](Guard::check_in_traced).
    pub fn check_traced(&self, key: &str) -> Checked {
        self.check_hashed(0, self.hash(0, key))
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
        self.check_in_traced(class, key).decision
    }

    /// Like [`check_in`](Guard::check_in), telling what the key's shadow
    /// limit, if it has one, would have decided too: for naming in a log the
    /// keys a shadow would deny, which [`Stats`] only counts. The decision
    /// is `check_in`'s, whatever the shadow says. See
    /// [`set_shadow`](Guard::set_shadow).
    ///
    /// ```no_run
    /// # fn run(guard: rateguard::Guard) {
    /// use rateguard::Decision;
    /// let checked = guard.check_in_traced("free", "tenant:42");
    /// if let Some(Decision::Deny { .. }) = checked.shadow {
    ///     // tenant:42 would be limited under the new limit of "free"
    /// }
    /// # }
    /// ```
    pub fn check_in_traced(&self, class: &str, key: &str) -> Checked {
        let class = class_hash(class);
        self.check_hashed(class, self.hash(class, key))
    }

    /// The hash this node stores and gossips `key` of the class `class`
    /// under, its [`Builder::key_salt`](crate::Builder::key_salt) included:
    /// for matching the hashes of
    /// [`Stats::most_denied`](crate::Stats::most_denied) to keys. Without a
    /// salt, the same as [`key_hash_in`](crate::key_hash_in).
    pub fn key_hash_in(&self, class: &str, key: &str) -> u64 {
        self.hash(class_hash(class), key)
    }

    fn hash(&self, class: u64, key: &str) -> u64 {
        salted_hash_in(self.shared.salt.as_ref(), class, key)
    }

    fn check_hashed(&self, class: u64, key: u64) -> Checked {
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
        // A slot taken just now carries the strictest quota of any class
        // and no shadow: it gets its own class's of both. If the slot went
        // to another key meanwhile, a race for this one lost after all, the
        // quota goes back to the free slots' strict one and the shadow off,
        // for the background task to set right at its next tick.
        if taken {
            let (quota, shadow) = shared.fresh_quota(class, key);
            slot.gcra.set_quota(quota, now);
            slot.shadow.set(shadow, now);
            if !shared.table.holds(slot, key) {
                let free = gcra::Quota::unpack(shared.free_quota.load(Ordering::Relaxed));
                slot.gcra.set_quota(free, now);
                slot.shadow.set(None, now);
            }
        }
        let decision = slot.gcra.check(now);
        // The shadow sees every attempt, those just denied included, and
        // decides nothing: off, it costs one load from this slot's line.
        let shadow = slot.shadow.check(now);
        if let Some(gcra::Decision::Deny { .. }) = shadow {
            slot.count_shadow_denial();
        }
        let shadow = shadow.map(|shadow| Decision::at(shadow, now));
        let decision = match decision {
            gcra::Decision::Allow => Decision::Allow,
            denied => {
                slot.count_denial();
                if shared.dry_run.load(Ordering::Relaxed) {
                    Decision::Allow
                } else {
                    Decision::at(denied, now)
                }
            }
        };
        Checked { decision, shadow }
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
        drop(report);
        stats.gossip_auth = shared.auth().mode();
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
        let slot = self.shared.table.find(self.key_hash_in(class, key))?;
        Some(slot.gcra.quota().rate_per_sec())
    }

    /// What this node knows of one key of the class `class`: its requests,
    /// denials and shadow denials a second, averaged over the last ten
    /// seconds or so, its share, and the limit and shadow it is held to. For
    /// one key, where [`Stats::most_denied`](crate::Stats::most_denied)
    /// lists the worst.
    ///
    /// The rates are the background task's, as of its last tick, asked of it
    /// here: the answer waits for the task to get to it. `None` if the node
    /// keeps no state for the key, which asking does not change, or if the
    /// background task is gone; `None` too while the background task still
    /// counts a key whose slot went to another, until it forgets the key.
    pub async fn key_stats(&self, class: &str, key: &str) -> Option<KeyStats> {
        let class_hash = class_hash(class);
        let hash = self.hash(class_hash, key);
        let (reply, answer) = oneshot::channel();
        // Bounded, and an error once the task has dropped its end: never a
        // wait for a task that is not there.
        self.shared
            .queries
            .send(crate::driver::Query::Key { hash, reply })
            .await
            .ok()?;
        let rates = answer.await.ok()??;
        let share = self.share_in(class, key)?;
        let (limit, shadow) = self.shared.classes.snapshot().resolve_given(
            &self.shared.key_limits.snapshot(),
            class_hash,
            hash,
        );
        Some(KeyStats {
            requests_per_sec: rates.requests_per_sec,
            denied_per_sec: rates.denied_per_sec,
            shadow_denied_per_sec: rates.shadow_denied_per_sec,
            share,
            limit,
            shadow,
        })
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

    /// Removes the class `name`, and its shadow: its keys take the default
    /// limit from then on. Whether there was such a class.
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

    /// Replaces every named class at once: those not listed are removed,
    /// with their shadows. All or nothing: on an error no class changes.
    /// For applying a whole set of tariffs from a config file or a control
    /// plane.
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

    /// Gives the class `name`, `""` for the default, a shadow limit, or
    /// takes its shadow away with `None`, for every clone of this `Guard`:
    /// a limit tried out on real traffic before it is enforced. The class's
    /// limit goes on deciding; the shadow sees every request of every key
    /// of the class that has a slot of its own, those the limit denies
    /// included, and counts the ones it would deny, in
    /// [`Stats::shadow_denied`](crate::Stats::shadow_denied). Keys in the
    /// overflow slot, past [`Builder::tracked_keys`](crate::Builder::tracked_keys),
    /// have no shadow: under heavy key churn the count is short of them.
    /// So it answers "what if the clients sent the same under the new
    /// limit", for a lower limit as for a higher one; for which keys, see
    /// [`check_in_traced`](Guard::check_in_traced). A key given the class's
    /// limit with [`KeyLimit::Class`] follows the class's shadow; one given
    /// a limit of its own has none, which keeps it out of a rollout. New
    /// keys take the shadow at once, known ones from the next tick.
    ///
    /// ```no_run
    /// # fn run(guard: rateguard::Guard) -> Result<(), rateguard::Error> {
    /// use rateguard::Limit;
    /// guard.set_shadow("free", Some(Limit::per_second(10)))?;
    /// // A day later, by Stats::shadow_denied:
    /// guard.set_class("free", Limit::per_second(10))?;
    /// guard.set_shadow("free", None)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The shadow is an estimate, and it errs on the strict side. A key's
    /// share of a shadow is its share of the real limit, scaled by the
    /// ratio of the two, so the node's holds (`joining`, `learning`,
    /// `without_quorum`) act on it alike. But whether a key is hot, and so
    /// shared by demand rather than evenly, depends on the limit: a key
    /// whose demand on a node is between `α × R_shadow / N` and `α × R / N`
    /// is cold under the real limit and would be hot under the shadow. It
    /// gets the cold share of the shadow, where under that limit it would
    /// get a share by its demand, no smaller. The shadow thus denies such a
    /// key more than enforcing it would: rolled out, the shadow's limit
    /// affects as many clients as it counts, or fewer.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownClass`] for a class this node does not have.
    /// [`Error::ZeroLimit`] and [`Error::TooLargeBurst`], as for
    /// [`set_class`](Guard::set_class). On an error the shadows stay as they
    /// were.
    pub fn set_shadow(&self, name: &str, limit: Option<Limit>) -> Result<(), Error> {
        self.shared.classes.set_shadow(name, limit)
    }

    /// Replaces every shadow at once: those not listed are taken away. All
    /// or nothing: on an error no shadow changes. See
    /// [`set_shadow`](Guard::set_shadow).
    ///
    /// # Errors
    ///
    /// As [`set_shadow`](Guard::set_shadow).
    pub fn set_shadows<S: Into<String>>(
        &self,
        shadows: impl IntoIterator<Item = (S, Limit)>,
    ) -> Result<(), Error> {
        self.shared.classes.replace_shadows(shadows)
    }

    /// The shadows by class, `""` for the default, in no particular order.
    pub fn shadows(&self) -> Vec<(String, Limit)> {
        self.shared.classes.shadows()
    }

    /// Gives one key a limit of its own, or another class's, for every
    /// clone of this `Guard`: for a customer who bought more, or an abuser
    /// to hold to 1 a second. `class` and `key` are as for
    /// [`check_in`](Guard::check_in), and the key stays the same key: the
    /// debt it has is kept, and the other nodes need nothing to know it by.
    /// A new key takes the limit at once, a known one from the next tick of
    /// the background task. A [`KeyLimit::Class`] follows its class as it
    /// is changed; a class that does not exist gives the default limit, and
    /// is counted in [`Stats::unknown_class`](crate::Stats::unknown_class).
    ///
    /// Every instance must be given the same: while one has the new limit
    /// and another the old, the fleet admits no more than the higher of the
    /// two, as with [`set_class`](Guard::set_class). A key with a limit
    /// stricter than every class may pass at a looser quota in three narrow
    /// cases: a check on another thread in the nanoseconds between the
    /// key's taking a slot and the slot getting the key's own quota, at the
    /// strictest class's; a key taking its first slot just as the first key
    /// limit of the node is set, at its class's until the next tick; and a
    /// key in the overflow slot, for as long as it is there. The limits of
    /// keys are not among what sizes the quota of a slot nobody holds.
    ///
    /// ```no_run
    /// # fn run(guard: rateguard::Guard) -> Result<(), rateguard::Error> {
    /// use rateguard::{KeyLimit, Limit};
    /// guard.set_key_limit("pro", "tenant:42", Limit::per_second(2_000))?;
    /// guard.set_key_limit("free", "tenant:7", KeyLimit::class("pro"))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::TooManyKeyLimits`] for a new key beyond
    /// [`Builder::max_key_limits`](crate::Builder::max_key_limits).
    /// [`Error::ZeroLimit`] and [`Error::TooLargeBurst`] for a
    /// [`KeyLimit::Limit`], as for [`set_class`](Guard::set_class).
    pub fn set_key_limit(
        &self,
        class: &str,
        key: &str,
        limit: impl Into<KeyLimit>,
    ) -> Result<(), Error> {
        self.shared.key_limits.set(
            self.key_hash_in(class, key),
            (class.to_owned(), key.to_owned(), limit.into()),
        )
    }

    /// Takes the limit of one key away: it goes back to its class's. Whether
    /// it had one.
    pub fn remove_key_limit(&self, class: &str, key: &str) -> bool {
        self.shared.key_limits.remove(self.key_hash_in(class, key))
    }

    /// Replaces every key limit at once: those not listed are removed.
    /// All or nothing: on an error none changes. For applying a whole state
    /// from a config file or a control plane.
    ///
    /// # Errors
    ///
    /// As [`set_key_limit`](Guard::set_key_limit).
    pub fn set_key_limits<C: Into<String>, K: Into<String>, L: Into<KeyLimit>>(
        &self,
        limits: impl IntoIterator<Item = (C, K, L)>,
    ) -> Result<(), Error> {
        self.shared
            .key_limits
            .replace(limits.into_iter().map(|(class, key, limit)| {
                let (class, key) = (class.into(), key.into());
                (self.key_hash_in(&class, &key), (class, key, limit.into()))
            }))
    }

    /// The key limits as `(class, key, limit)`, in no particular order.
    pub fn key_limits(&self) -> Vec<(String, String, KeyLimit)> {
        self.shared.key_limits.list()
    }

    /// Starts or ends a dry run, for every clone of this `Guard`: when on,
    /// [`check`](Guard::check) admits every request and counts those it
    /// would have denied in [`Stats::denied`]. For trying a limit on real
    /// traffic first, then enforcing it with no restart.
    pub fn set_dry_run(&self, on: bool) {
        self.shared.dry_run.store(on, Ordering::Relaxed);
    }

    /// Changes how the gossip is signed and checked, as
    /// [`Builder::gossip_auth`](crate::Builder::gossip_auth) sets it, for
    /// every clone of this `Guard`: from the next datagram sent or
    /// received on. Checks go on unaffected meanwhile. For rotating a
    /// secret, or turning signing on, with no restart.
    ///
    /// # Errors
    ///
    /// [`Error::GossipAuth`] for a mode other than `Off` with no secret,
    /// or more than two secrets; the setting stays as it was.
    pub fn set_gossip_auth(&self, mode: GossipAuth, secrets: Vec<Secret>) -> Result<(), Error> {
        let auth = Auth::new(mode, secrets).map_err(Error::GossipAuth)?;
        *self
            .shared
            .auth
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(auth);
        Ok(())
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
            .find(guard.key_hash_in("pro", "k"))
            .unwrap();
        assert_eq!(slot.class.load(Ordering::Relaxed), class_hash("pro"));
        slot.class.store(class_hash("free"), Ordering::Relaxed);
        let _ = guard.check_in("pro", "k");
        assert_eq!(slot.class.load(Ordering::Relaxed), class_hash("pro"));
    }
}
