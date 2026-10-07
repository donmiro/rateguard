//! GCRA, the Generic Cell Rate Algorithm: a token bucket kept as a single
//! timestamp.
//!
//! The state is the theoretical arrival time (TAT) of the next request. A
//! request is admitted if it comes no earlier than `TAT - τ`, where τ is the
//! burst tolerance, and admitting it moves TAT one emission interval `T`
//! forward. Unlike `governor`, the quota can change in flight
//! ([`Gcra::set_quota`]): a node's share changes every protocol period.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Time in nanoseconds since an origin the caller picks. Only differences
/// matter, so any monotonic clock will do, and so will a simulated one.
pub type Nanos = u64;

const ONE_SEC: Nanos = 1_000_000_000;
const INTERVAL_BITS: u32 = 40;
const BURST_BITS: u32 = 64 - INTERVAL_BITS;
/// The longest emission interval a quota holds: about 18 minutes. A slower
/// rate is held at this one, stricter than asked, never looser.
pub const MAX_INTERVAL: Nanos = (1 << INTERVAL_BITS) - 1;
/// The largest burst a quota holds: 16,777,215.
pub const MAX_BURST: u32 = (1 << BURST_BITS) - 1;

/// A rate, kept as the emission interval between requests, and how many
/// requests may arrive back to back. An interval rather than a whole number
/// of requests per second: a node's share of a small limit over a big fleet
/// is well under one a second, and rounding it up multiplies the limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quota {
    interval: Nanos,
    burst: u32,
}
impl Quota {
    /// A whole number of requests per second.
    ///
    /// # Panics
    ///
    /// If either number is zero, or the burst is over [`MAX_BURST`].
    pub fn new(rate_per_sec: u32, burst: u32) -> Self {
        assert!(rate_per_sec > 0, "rate_per_sec must be > 0");
        Self::with_interval(ONE_SEC.div_ceil(rate_per_sec as u64), burst)
    }

    /// Any rate, fractions of a request per second included. A rate too
    /// slow to hold, zero or not a number, is held at [`MAX_INTERVAL`].
    ///
    /// # Panics
    ///
    /// If the burst is zero or over [`MAX_BURST`].
    pub fn per_second(rate: f64, burst: u32) -> Self {
        let interval = (ONE_SEC as f64 / rate).ceil();
        let interval = if interval.is_finite() && interval >= 1.0 {
            (interval as Nanos).min(MAX_INTERVAL)
        } else if interval.is_finite() && interval > 0.0 {
            1
        } else {
            MAX_INTERVAL
        };
        Self::with_interval(interval, burst)
    }

    fn with_interval(interval: Nanos, burst: u32) -> Self {
        assert!(burst > 0, "burst must be > 0");
        assert!(burst <= MAX_BURST, "burst must be <= {MAX_BURST}");
        Self {
            interval: interval.clamp(1, MAX_INTERVAL),
            burst,
        }
    }

    /// The quota as one word, for an `AtomicU64`.
    pub fn pack(self) -> u64 {
        (self.interval << BURST_BITS) | self.burst as u64
    }

    /// # Panics
    ///
    /// If the word does not hold a quota made by [`pack`](Quota::pack).
    pub fn unpack(packed: u64) -> Self {
        Self::with_interval(packed >> BURST_BITS, (packed & MAX_BURST as u64) as u32)
    }

    fn t_nanos(&self) -> Nanos {
        self.interval
    }

    fn tau_nanos(&self) -> Nanos {
        self.interval.saturating_mul(self.burst as u64 - 1)
    }
}

/// One key's limiter.
#[derive(Debug, Clone, Copy)]
pub struct Gcra {
    quota: Quota,
    tat: Option<Nanos>,
}
/// The answer to a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// Denied. `retry_at` is the earliest moment a request could be
    /// admitted, unless other requests take that slot first.
    Deny {
        retry_at: Nanos,
    },
}
impl Gcra {
    pub fn new(quota: Quota) -> Self {
        Self { quota, tat: None }
    }

    /// Admits or denies one request at `now`.
    pub fn check(&mut self, now: Nanos) -> Decision {
        let t = self.quota.t_nanos();
        let tau = self.quota.tau_nanos();

        let tat = self.tat.unwrap_or(now);
        let earliest = tat.saturating_sub(tau);

        if now < earliest {
            return Decision::Deny { retry_at: earliest };
        }

        self.tat = Some(std::cmp::max(tat, now) + t);
        Decision::Allow
    }

    /// Switches to a new quota in place. The debt is carried over in
    /// requests, not in time: a key that was two requests ahead stays two
    /// requests ahead at the new rate, so changing the share neither wipes
    /// out its history nor punishes it.
    pub fn set_quota(&mut self, quota: Quota, now: Nanos) {
        if let Some(old_tat) = self.tat {
            let old_t = self.quota.t_nanos();
            let debt_nanos = old_tat.saturating_sub(now);
            let debt_slots = debt_nanos as f64 / old_t as f64;

            self.quota = quota;
            let new_t = self.quota.t_nanos();
            let new_debt_nanos = (debt_slots * new_t as f64).round() as u64;
            self.tat = Some(now.saturating_add(new_debt_nanos));
        } else {
            self.quota = quota;
        }
    }

    /// Whether requests were admitted ahead of their time and not yet paid
    /// for. A GCRA without debt behaves exactly like a fresh one, which is
    /// what makes forgetting an idle key lossless.
    pub fn has_debt(&self, now: Nanos) -> bool {
        self.tat.is_some_and(|tat| tat > now)
    }

    /// How long until a request would be admitted; `None` if it would be
    /// now.
    pub fn retry_after(&self, now: Nanos) -> Option<Duration> {
        let tat = self.tat?;
        let tau = self.quota.tau_nanos();
        let earliest = tat.saturating_sub(tau);
        (now < earliest).then(|| Duration::from_nanos(earliest - now))
    }
}

/// [`Gcra`] for many threads: the TAT in one atomic word, a decision in
/// one compare-and-swap. TAT 0 stands for "never admitted" and behaves like
/// a fresh limiter, as `None` does in `Gcra`.
#[derive(Debug)]
pub struct AtomicGcra {
    quota: AtomicU64,
    tat: AtomicU64,
}
impl AtomicGcra {
    pub fn new(quota: Quota) -> Self {
        Self {
            quota: AtomicU64::new(quota.pack()),
            tat: AtomicU64::new(0),
        }
    }

    pub fn quota(&self) -> Quota {
        Quota::unpack(self.quota.load(Ordering::Relaxed))
    }

    /// Admits or denies one request at `now`.
    pub fn check(&self, now: Nanos) -> Decision {
        let quota = self.quota();
        let (t, tau) = (quota.t_nanos(), quota.tau_nanos());
        let mut tat = self.tat.load(Ordering::Acquire);
        loop {
            let earliest = tat.saturating_sub(tau);
            if now < earliest {
                return Decision::Deny { retry_at: earliest };
            }
            let next = tat.max(now) + t;
            match self
                .tat
                .compare_exchange_weak(tat, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Decision::Allow,
                Err(seen) => tat = seen,
            }
        }
    }

    /// Like [`Gcra::set_quota`]: the debt carries over in requests.
    ///
    /// The quota and the TAT are two words, changed one after the other. A
    /// check landing between the two is charged at the new interval and then
    /// rescaled as if it had been charged at the old one. When the share
    /// drops, that check costs up to `new_t² / old_t` more than it should
    /// (1000 → 25 a second: 1.6 s of extra debt, so denials for that long);
    /// when it rises, it costs less, by under one new interval. The window
    /// is a few nanoseconds once per tick, and closing it would take
    /// versioning the pair, which no longer fits one atomic word. Accepted:
    /// the costly side is the strict one.
    pub fn set_quota(&self, quota: Quota, now: Nanos) {
        let old = Quota::unpack(self.quota.swap(quota.pack(), Ordering::AcqRel));
        if old == quota {
            return;
        }
        let (old_t, new_t) = (old.t_nanos() as f64, quota.t_nanos() as f64);
        // A plain compare-and-swap loop: `fetch_update` is deprecated on
        // newer toolchains and its replacement missing on older ones.
        let mut tat = self.tat.load(Ordering::Acquire);
        loop {
            let debt_slots = tat.saturating_sub(now) as f64 / old_t;
            let rescaled = now.saturating_add((debt_slots * new_t).round() as u64);
            match self
                .tat
                .compare_exchange_weak(tat, rescaled, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(seen) => tat = seen,
            }
        }
    }

    /// See [`Gcra::has_debt`].
    pub fn has_debt(&self, now: Nanos) -> bool {
        self.tat.load(Ordering::Acquire) > now
    }

    /// Back to a fresh limiter on `quota`, for a slot handed to another key.
    pub fn reset(&self, quota: Quota) {
        self.quota.store(quota.pack(), Ordering::Release);
        self.tat.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota(rate: u32, burst: u32) -> Quota {
        Quota::new(rate, burst)
    }

    #[test]
    #[should_panic]
    fn negative_rps_or_burst_schould_panic() {
        Quota::new(0_u32, 0_u32);
    }

    #[test]
    fn burst_of_n_then_deny() {
        let mut g = Gcra::new(quota(1000, 3));
        let t0: Nanos = 0;

        assert_eq!(g.check(t0), Decision::Allow);
        assert_eq!(g.check(t0), Decision::Allow);
        assert_eq!(g.check(t0), Decision::Allow);
        assert!(matches!(g.check(t0), Decision::Deny { .. }));
    }

    #[test]
    fn deny_then_allow_after_retry_at() {
        let mut g = Gcra::new(quota(1000, 1));
        let t0: Nanos = 0;

        assert_eq!(g.check(t0), Decision::Allow);
        let Decision::Deny { retry_at } = g.check(t0) else {
            panic!("second immidiate request must be denied");
        };
        assert_eq!(g.check(retry_at), Decision::Allow);
    }

    #[test]
    fn idle_key_does_not_accumulate_unlimited_credit() {
        let mut g = Gcra::new(quota(1000, 3));
        let t0: Nanos = 0;
        let later = t0 + 10_000_000_000;

        assert_eq!(g.check(later), Decision::Allow);
        assert_eq!(g.check(later), Decision::Allow);
        assert_eq!(g.check(later), Decision::Allow);
        assert!(matches!(g.check(later), Decision::Deny { .. }));
    }

    #[test]
    fn fresh_and_drained_limiters_carry_no_debt() {
        let mut g = Gcra::new(quota(1000, 3));
        assert!(!g.has_debt(0), "a fresh limiter carries no debt");

        g.check(0);
        assert!(g.has_debt(0));
        assert!(!g.has_debt(1_000_000), "one slot drains in exactly t");
    }

    #[test]
    fn has_debt_is_stricter_than_retry_after() {
        let mut g = Gcra::new(quota(1000, 3));
        g.check(0);

        assert!(
            g.retry_after(0).is_none(),
            "requests still pass, one slot of three is spent"
        );
        assert!(
            g.has_debt(0),
            "but the entry is not empty: evicting it would hand back the whole burst"
        );
    }

    #[test]
    fn set_quota_preserves_debt_proportionally() {
        let mut g = Gcra::new(quota(1000, 10));
        let t0: Nanos = 0;
        for _ in 0..10 {
            assert_eq!(g.check(t0), Decision::Allow);
        }
        assert!(matches!(g.check(t0), Decision::Deny { .. }));

        g.set_quota(quota(100, 10), t0);
        assert!(matches!(g.check(t0), Decision::Deny { .. }));
    }

    #[test]
    fn a_quota_packs_into_one_word() {
        for q in [
            Quota::new(1234, 56),
            Quota::per_second(0.25, 1),
            Quota::per_second(0.0, MAX_BURST),
        ] {
            assert_eq!(Quota::unpack(q.pack()), q);
        }
    }

    #[test]
    fn a_rate_under_one_per_second_spaces_requests_seconds_apart() {
        let mut g = Gcra::new(Quota::per_second(0.25, 1));
        assert_eq!(g.check(0), Decision::Allow);
        assert_eq!(
            g.check(1),
            Decision::Deny {
                retry_at: 4 * ONE_SEC
            }
        );
        assert_eq!(g.check(4 * ONE_SEC), Decision::Allow);
    }

    #[test]
    fn a_rate_too_slow_to_express_is_held_at_the_slowest_not_wrapped() {
        let slowest = Quota::per_second(0.0, 1);
        assert_eq!(Quota::per_second(1e-12, 1), slowest);
        assert_eq!(Quota::per_second(f64::NAN, 1), slowest);
        let mut g = Gcra::new(slowest);
        assert_eq!(g.check(0), Decision::Allow);
        assert!(matches!(g.check(1_000 * ONE_SEC), Decision::Deny { .. }));
    }

    #[test]
    fn a_whole_rate_is_the_same_quota_either_way() {
        assert_eq!(Quota::per_second(100.0, 10), Quota::new(100, 10));
        assert_eq!(Quota::per_second(1000.0, 1), Quota::new(1000, 1));
    }

    mod atomic_equivalence {
        use super::super::*;
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum Op {
            Check(Nanos),
            SetQuota(u32, u32, Nanos),
        }

        fn ops() -> impl Strategy<Value = Vec<Op>> {
            prop::collection::vec(
                prop_oneof![
                    4 => (0u64..50_000_000).prop_map(Op::Check),
                    1 => (1u32..5_000, 1u32..50, 0u64..50_000_000)
                        .prop_map(|(r, b, dt)| Op::SetQuota(r, b, dt)),
                ],
                1..200,
            )
        }

        proptest! {
            // Single-threaded, the atomic GCRA is the reference one.
            #[test]
            fn decides_exactly_like_the_reference(
                (rate, burst) in (1u32..5_000, 1u32..50),
                ops in ops(),
            ) {
                let mut reference = Gcra::new(Quota::new(rate, burst));
                let atomic = AtomicGcra::new(Quota::new(rate, burst));
                let mut now = 0;
                for op in ops {
                    match op {
                        Op::Check(dt) => {
                            now += dt;
                            prop_assert_eq!(atomic.check(now), reference.check(now));
                        }
                        Op::SetQuota(r, b, dt) => {
                            now += dt;
                            reference.set_quota(Quota::new(r, b), now);
                            atomic.set_quota(Quota::new(r, b), now);
                        }
                    }
                    prop_assert_eq!(atomic.has_debt(now), reference.has_debt(now));
                }
            }
        }
    }
}
