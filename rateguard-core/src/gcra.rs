//! GCRA, the Generic Cell Rate Algorithm: a token bucket kept as a single
//! timestamp.
//!
//! The state is the theoretical arrival time (TAT) of the next request. A
//! request is admitted if it comes no earlier than `TAT - τ`, where τ is the
//! burst tolerance, and admitting it moves TAT one emission interval `T`
//! forward. Unlike `governor`, the quota can change in flight
//! ([`Gcra::set_quota`]): a node's share changes every protocol period.

use std::time::Duration;

use crate::sync::{AtomicU64, Ordering};

/// Time in nanoseconds since an origin the caller picks. Only differences
/// matter, so any monotonic clock will do, and so will a simulated one.
pub type Nanos = u64;

const ONE_SEC: Nanos = 1_000_000_000;
const INTERVAL_BITS: u32 = 40;
const UNIT_BITS: u32 = 2;
const BURST_BITS: u32 = 64 - INTERVAL_BITS - UNIT_BITS;
const UNIT_SHIFT: u32 = BURST_BITS;
const INTERVAL_SHIFT: u32 = BURST_BITS + UNIT_BITS;
/// Nanoseconds in each unit an interval is counted in, by the unit's code in
/// the packed word. Code 3 is unused.
const UNIT_NANOS: [u64; 3] = [1, 1_000, 1_000_000];
/// The longest emission interval a quota holds: 2^40 - 1 milliseconds, about
/// 34 years. A rate too slow for it is held at this interval, so it admits
/// **more** often than asked, looser, not stricter.
pub const MAX_INTERVAL: Nanos = ((1 << INTERVAL_BITS) - 1) * 1_000_000;
/// The largest burst a quota holds: 4,194,303.
pub const MAX_BURST: u32 = (1 << BURST_BITS) - 1;

/// A rate, kept as the emission interval between requests, and how many
/// requests may arrive back to back. An interval rather than a whole number
/// of requests per second: a node's share of a small limit over a big fleet
/// is well under one a second, and rounding it up multiplies the limit.
///
/// The interval is counted in the finest unit (nanoseconds, microseconds or
/// milliseconds) that holds it in 40 bits, so a short interval is exact and a
/// long one is off by under a part in 10^9. A coarser unit rounds up, to the
/// strict side. The interval kept here is already rounded, so a quota equals
/// the one it is unpacked from.
///
/// The burst tolerance, `interval × (burst − 1)`, is held under 292 years: a
/// slow rate with a big burst past that gets a shorter burst, stricter, not
/// looser (see `MAX_TAU` in the source).
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
        let (value, unit) = Self::encode_interval(interval.clamp(1, MAX_INTERVAL));
        Self {
            interval: value * UNIT_NANOS[unit],
            burst,
        }
    }

    /// The interval as a value and a unit code, in the finest unit that
    /// holds it, rounded up when the unit is coarser than a nanosecond.
    fn encode_interval(interval: Nanos) -> (u64, usize) {
        for (unit, nanos) in UNIT_NANOS.iter().enumerate() {
            let value = interval.div_ceil(*nanos);
            if value < 1 << INTERVAL_BITS {
                return (value, unit);
            }
        }
        unreachable!("the interval is at most MAX_INTERVAL, which fits in milliseconds")
    }

    /// The rate, in requests per second: the one it was made with, up to
    /// the rounding of the interval to its unit.
    pub fn rate_per_sec(&self) -> f64 {
        ONE_SEC as f64 / self.interval as f64
    }

    /// How many requests may arrive back to back.
    pub fn burst(&self) -> u32 {
        self.burst
    }

    /// The quota as one word, for an `AtomicU64`.
    pub fn pack(self) -> u64 {
        let (value, unit) = Self::encode_interval(self.interval);
        (value << INTERVAL_SHIFT) | ((unit as u64) << UNIT_SHIFT) | self.burst as u64
    }

    /// # Panics
    ///
    /// If the word does not hold a quota made by [`pack`](Quota::pack).
    pub fn unpack(packed: u64) -> Self {
        let unit = ((packed >> UNIT_SHIFT) & ((1 << UNIT_BITS) - 1)) as usize;
        let nanos = *UNIT_NANOS
            .get(unit)
            .expect("the interval's unit is unknown");
        Self::with_interval(
            (packed >> INTERVAL_SHIFT).saturating_mul(nanos),
            (packed & MAX_BURST as u64) as u32,
        )
    }

    fn t_nanos(&self) -> Nanos {
        self.interval
    }

    fn tau_nanos(&self) -> Nanos {
        tau(self.interval, self.burst as u64)
    }

    /// The interval and τ of the quota packed in `packed`, straight from
    /// the word: what [`step`] needs, for every check. `unpack` validates
    /// and re-encodes the interval through [`with_interval`], which is
    /// needed nowhere here: a word made by `pack` is already rounded to its
    /// unit, so both give the same `t` and `tau`.
    ///
    /// [`with_interval`]: Quota::with_interval
    #[inline]
    fn decode(packed: u64) -> (Nanos, Nanos) {
        let unit = (packed >> UNIT_SHIFT) & ((1 << UNIT_BITS) - 1);
        // Code 3 is never packed; read as milliseconds, like code 2,
        // instead of a bounds check on the request path.
        let nanos = DECODE_UNIT_NANOS[unit as usize];
        // At most 2^40 × 10^6 < 2^60: no overflow.
        let t = (packed >> INTERVAL_SHIFT) * nanos;
        (t, tau(t, packed & MAX_BURST as u64))
    }
}

/// [`UNIT_NANOS`] for every two-bit code, the unused 3 as milliseconds.
const DECODE_UNIT_NANOS: [u64; 4] = [UNIT_NANOS[0], UNIT_NANOS[1], UNIT_NANOS[2], UNIT_NANOS[2]];

/// The longest burst tolerance τ: 2^63 − 1 ns, some 292 years.
///
/// A slow rate with a big burst asks for more: at the slowest interval,
/// [`MAX_INTERVAL`], `interval × (burst − 1)` passes 2^64 from a burst of
/// 18. Saturated at `u64::MAX`, τ would put the earliest admission,
/// `TAT − τ`, at 0 for good, and every request would be admitted. Capped,
/// such a quota admits `⌊τ_MAX / interval⌋ + 1` back to back, fewer than its
/// burst: it errs strict, never admitting more than the quota.
///
/// Why half of `u64::MAX` and not just under it: an admission needs
/// `TAT ≤ now + τ`, and the TAT saturates at `u64::MAX`. Were `now + τ` to
/// reach `u64::MAX`, any TAT would pass and the limit would be gone again.
/// With τ at most 2^63 − 1, that takes a clock past 2^63 ns from its
/// origin, 292 years: a node counts from its own start, a simulation from
/// 0. A quota whose `interval × (burst − 1)` fits under the cap is
/// unchanged: any burst at an interval up to 36 minutes, a burst up to 9 at
/// any interval.
const MAX_TAU: Nanos = u64::MAX / 2;

/// τ of a quota: `interval × (burst − 1)`, capped at [`MAX_TAU`].
#[inline]
fn tau(interval: Nanos, burst: u64) -> Nanos {
    interval
        .saturating_mul(burst.saturating_sub(1))
        .min(MAX_TAU)
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
    /// Serve the request.
    Allow,
    /// Denied. `retry_at` is the earliest moment a request could be
    /// admitted, unless other requests take that slot first.
    Deny {
        /// When a request could next be admitted, in the caller's nanoseconds.
        retry_at: Nanos,
    },
}
impl Gcra {
    /// A limiter that has admitted nothing yet.
    pub fn new(quota: Quota) -> Self {
        Self { quota, tat: None }
    }

    /// The quota it enforces now.
    pub fn quota(&self) -> Quota {
        self.quota
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

        // Saturating: a TAT near the end of time must not wrap or panic.
        self.tat = Some(std::cmp::max(tat, now).saturating_add(t));
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
    /// A limiter that has admitted nothing yet.
    pub fn new(quota: Quota) -> Self {
        Self {
            quota: AtomicU64::new(quota.pack()),
            tat: AtomicU64::new(0),
        }
    }

    /// The quota it enforces now.
    pub fn quota(&self) -> Quota {
        Quota::unpack(self.quota.load(Ordering::Relaxed))
    }

    /// Admits or denies one request at `now`.
    pub fn check(&self, now: Nanos) -> Decision {
        step(&self.tat, self.quota.load(Ordering::Relaxed), now)
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
        // Most calls change nothing, a runtime setting every slot's quota
        // every tick: then they write nothing either, and a quota another
        // thread sets meanwhile is not overwritten with a stale one.
        if self.quota.load(Ordering::Acquire) == quota.pack() {
            return;
        }
        let old = Quota::unpack(self.quota.swap(quota.pack(), Ordering::AcqRel));
        rescale(&self.tat, old, quota, now);
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

/// One GCRA step on the TAT in `tat`, at the quota packed in `quota`: the
/// decision of [`AtomicGcra::check`] and [`ShadowGcra::check`].
#[inline]
fn step(tat: &AtomicU64, quota: u64, now: Nanos) -> Decision {
    let (t, tau) = Quota::decode(quota);
    let mut seen = tat.load(Ordering::Acquire);
    loop {
        let earliest = seen.saturating_sub(tau);
        if now < earliest {
            return Decision::Deny { retry_at: earliest };
        }
        let next = seen.max(now).saturating_add(t);
        match tat.compare_exchange_weak(seen, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Decision::Allow,
            Err(current) => seen = current,
        }
    }
}

/// The debt in `tat`, run up at `old`, carried over to `new` in requests;
/// see [`Gcra::set_quota`].
fn rescale(tat: &AtomicU64, old: Quota, new: Quota, now: Nanos) {
    if old == new {
        return;
    }
    let (old_t, new_t) = (old.t_nanos() as f64, new.t_nanos() as f64);
    // A plain compare-and-swap loop: `fetch_update` is deprecated on newer
    // toolchains and its replacement missing on older ones.
    let mut seen = tat.load(Ordering::Acquire);
    loop {
        let debt_slots = seen.saturating_sub(now) as f64 / old_t;
        let rescaled = now.saturating_add((debt_slots * new_t).round() as u64);
        match tat.compare_exchange_weak(seen, rescaled, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(current) => seen = current,
        }
    }
}

/// A second [`AtomicGcra`] that only counts, for a limit being tried out
/// next to the one that decides: a shadow. It may be off, and is when new.
///
/// Off is 0 in the quota word, which no packed quota is. It is a type of
/// its own, not an `AtomicGcra` that may be off, because `AtomicGcra`
/// unpacks the quota it replaces on every change, and `Quota::unpack(0)`
/// panics: here switching on and off never goes through the debt's
/// rescaling. Switched on, it starts fresh, its old debt forgotten.
#[derive(Debug)]
pub struct ShadowGcra {
    quota: AtomicU64,
    tat: AtomicU64,
}
impl Default for ShadowGcra {
    fn default() -> Self {
        Self::new()
    }
}
impl ShadowGcra {
    /// A shadow switched off.
    pub fn new() -> Self {
        Self {
            quota: AtomicU64::new(0),
            tat: AtomicU64::new(0),
        }
    }

    /// Switches the shadow to `quota`, or off with `None`. Off to on starts
    /// fresh; on to on carries the debt over in requests, as
    /// [`AtomicGcra::set_quota`] does, with the same window between the two
    /// words; to off forgets everything.
    pub fn set(&self, quota: Option<Quota>, now: Nanos) {
        let packed = quota.map_or(0, Quota::pack);
        // Most calls change nothing, once a tick for every slot.
        if self.quota.load(Ordering::Acquire) == packed {
            return;
        }
        let old = self.quota.swap(packed, Ordering::AcqRel);
        match quota {
            // Only a word that held a quota is unpacked: 0 is "off".
            Some(new) if old != 0 => rescale(&self.tat, Quota::unpack(old), new, now),
            // Off, or on from off: fresh. A check racing the change may
            // still leave its TAT after this store: one interval's debt at
            // most, on a shadow, which only counts.
            _ => self.tat.store(0, Ordering::Release),
        }
    }

    /// What the shadow says of one request at `now`, counting it; `None`
    /// when off, at the cost of one `Relaxed` load. Inline, so that a
    /// runtime checking a slot with no shadow pays that load and no call.
    #[inline]
    pub fn check(&self, now: Nanos) -> Option<Decision> {
        // The word is read once, and decoded only when it is not 0: a
        // concurrent switch off cannot make this decode the 0, an interval
        // of 0 that would deny nothing.
        match self.quota.load(Ordering::Relaxed) {
            0 => None,
            packed => Some(step(&self.tat, packed, now)),
        }
    }

    /// Off, for a slot handed to another key.
    pub fn reset(&self) {
        self.quota.store(0, Ordering::Release);
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
    fn a_quota_tells_its_rate_and_burst() {
        let q = Quota::per_second(2.5, 7);
        assert_eq!((q.rate_per_sec(), q.burst()), (2.5, 7));
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

    const TWO_40: u64 = 1 << 40;

    #[test]
    fn a_slow_rate_is_held_exactly_not_clamped() {
        let rate = 1.0 / (33.0 * 86_400.0);
        let q = Quota::per_second(rate, 1);
        assert!((q.rate_per_sec() / rate - 1.0).abs() < 1e-6);

        let mut g = Gcra::new(q);
        assert_eq!(g.check(0), Decision::Allow);
        assert!(matches!(g.check(ONE_SEC), Decision::Deny { .. }));
        assert_eq!(g.check(33 * 86_400 * ONE_SEC), Decision::Allow);
    }

    #[test]
    fn a_slow_quota_packs_into_one_word() {
        for interval in [1, TWO_40 - 1, TWO_40, TWO_40 * 1_000, MAX_INTERVAL] {
            for burst in [1, MAX_BURST] {
                let q = Quota::with_interval(interval, burst);
                assert_eq!(Quota::unpack(q.pack()), q, "{interval} ns, burst {burst}");
                assert!(
                    q.interval >= interval.min(MAX_INTERVAL),
                    "never rounded down"
                );
            }
        }
    }

    #[test]
    fn an_interval_is_rounded_up_to_its_unit_not_down() {
        // Just past 2^40 ns does not fit in nanoseconds: it goes to whole
        // microseconds, rounded up.
        let q = Quota::with_interval(TWO_40 + 1, 1);
        assert_eq!(q.interval, (TWO_40 / 1_000 + 1) * 1_000);
        // Just past 2^40 us goes to whole milliseconds.
        let q = Quota::with_interval(TWO_40 * 1_000 + 1, 1);
        assert_eq!(q.interval % 1_000_000, 0);
        assert!(q.interval > TWO_40 * 1_000);
    }

    // The check's fast path reads the same quota as `unpack` does.
    #[test]
    fn a_word_decodes_to_the_quota_it_packs() {
        for interval in [
            1,
            999,
            TWO_40 - 1,
            TWO_40 + 1,
            TWO_40 * 1_000 + 1,
            MAX_INTERVAL,
        ] {
            for burst in [1, 2, 8, 18, MAX_BURST] {
                let q = Quota::with_interval(interval, burst);
                let unpacked = Quota::unpack(q.pack());
                assert_eq!(
                    Quota::decode(q.pack()),
                    (unpacked.t_nanos(), unpacked.tau_nanos()),
                    "{interval} ns, burst {burst}"
                );
            }
        }
    }

    #[test]
    fn a_packed_quota_is_never_zero() {
        assert_ne!(Quota::per_second(1e9, 1).pack(), 0);
    }

    #[test]
    fn the_tat_saturates_at_the_longest_interval() {
        let now = u64::MAX - 10;

        // A burst of one: the TAT sits at the end of time and the next
        // request is denied, where an unchecked sum would wrap or panic.
        let slowest = Quota::per_second(0.0, 1);
        let mut g = Gcra::new(slowest);
        assert_eq!(g.check(now), Decision::Allow);
        assert!(matches!(g.check(now), Decision::Deny { .. }));
        let a = AtomicGcra::new(slowest);
        assert_eq!(a.check(now), Decision::Allow);
        assert!(matches!(a.check(now), Decision::Deny { .. }));

        // The biggest burst: `interval × (burst − 1)` is far past the end of
        // time, so tau is capped, and the burst is cut short, not let
        // through whole: denied after the burst, wherever the clock is
        // short of the cap.
        let wide = Quota::per_second(0.0, MAX_BURST);
        for start in [0, ONE_SEC, 1 << 62] {
            assert_eq!(admitted(wide, start, 100), 9, "from {start}");
        }
        // At the very end of time nothing overflows either.
        let mut g = Gcra::new(wide);
        assert_eq!(g.check(now), Decision::Allow);
        assert!(g.has_debt(now));
        let a = AtomicGcra::new(wide);
        assert_eq!(a.check(now), Decision::Allow);
        assert!(a.has_debt(now));
    }

    // How many of `n` requests at `now` a fresh limiter admits, the same
    // for both kinds: the reference and the atomic one.
    fn admitted(quota: Quota, now: Nanos, n: usize) -> usize {
        let mut g = Gcra::new(quota);
        let a = AtomicGcra::new(quota);
        let mut allowed = 0;
        for _ in 0..n {
            let decision = g.check(now);
            assert_eq!(a.check(now), decision);
            allowed += usize::from(decision == Decision::Allow);
        }
        allowed
    }

    // A burst of 18 at the slowest rate: `interval × 17` no longer fits in
    // 64 bits. A saturated tau would put the earliest admission at 0 and
    // admit every request; capped, the burst is cut short instead.
    #[test]
    fn a_burst_too_long_to_hold_is_cut_short_not_let_through() {
        let slowest = Quota::per_second(0.0, 18);
        assert!(admitted(slowest, 0, 1000) <= 18);
        assert_eq!(admitted(Quota::per_second(0.0, 17), 0, 1000), 9);
        // Under the cap a burst is whole: 8 intervals of 34 years fit in
        // 292.
        assert_eq!(admitted(Quota::per_second(0.0, 9), 0, 1000), 9);
        // The floor of one an hour on 40 nodes, `R × β / k` at the default
        // β of 0.05, with a burst of 10,000: an interval of 33 days.
        let floor = Quota::per_second(0.05 / 40.0 / 3600.0, 10_000);
        let allowed = admitted(floor, 0, 20_000);
        assert!(allowed > 0 && allowed <= 10_000, "{allowed}");
    }

    // 0 in the quota word means "no shadow": switching it off and on must
    // never go through `Quota::unpack(0)`, which panics, and on again it
    // starts fresh, its old debt forgotten.
    #[test]
    fn a_shadow_switches_on_and_off_without_unpacking_zero() {
        let shadow = ShadowGcra::new();
        assert_eq!(shadow.check(0), None, "off when new");
        shadow.set(None, 0);
        assert_eq!(shadow.check(0), None, "off to off");

        shadow.set(Some(quota(1000, 2)), 0);
        assert_eq!(shadow.check(0), Some(Decision::Allow));
        assert_eq!(shadow.check(0), Some(Decision::Allow));
        assert!(matches!(shadow.check(0), Some(Decision::Deny { .. })));

        shadow.set(None, 0);
        assert_eq!(shadow.check(0), None, "on to off");

        shadow.set(Some(quota(1000, 2)), 0);
        assert_eq!(shadow.check(0), Some(Decision::Allow), "fresh: the burst");
        assert_eq!(shadow.check(0), Some(Decision::Allow));
        assert!(matches!(shadow.check(0), Some(Decision::Deny { .. })));

        // On to on carries the debt over, as `AtomicGcra::set_quota` does.
        shadow.set(Some(quota(100, 2)), 0);
        assert!(matches!(shadow.check(0), Some(Decision::Deny { .. })));

        shadow.reset();
        assert_eq!(shadow.check(0), None, "a reset slot has no shadow");
    }

    #[test]
    fn a_rate_too_slow_for_34_years_is_held_looser_as_documented() {
        assert_eq!(Quota::per_second(1e-12, 1).interval, MAX_INTERVAL);
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
            // The same with fractional rates, as `Quota::per_second` makes
            // them: slow ones count their interval in coarser units.
            #[test]
            fn decides_like_the_reference_at_any_rate(
                (rate, burst) in (0.000_000_1f64..5_000.0, 1u32..50),
                ops in ops(),
            ) {
                let quota = Quota::per_second(rate, burst);
                let mut reference = Gcra::new(quota);
                let atomic = AtomicGcra::new(quota);
                let mut now = 0;
                for op in ops {
                    if let Op::Check(dt) = op {
                        now += dt;
                        prop_assert_eq!(atomic.check(now), reference.check(now));
                        prop_assert_eq!(atomic.has_debt(now), reference.has_debt(now));
                    }
                }
            }

            // Single-threaded, the atomic GCRA is the reference one.
            #[test]
            fn decides_exactly_like_the_reference(
                (rate, burst) in (1u32..5_000, 1u32..50),
                ops in ops(),
            ) {
                let mut reference = Gcra::new(Quota::new(rate, burst));
                let atomic = AtomicGcra::new(Quota::new(rate, burst));
                // A shadow switched on decides as the others do.
                let shadow = ShadowGcra::new();
                shadow.set(Some(Quota::new(rate, burst)), 0);
                let mut now = 0;
                for op in ops {
                    match op {
                        Op::Check(dt) => {
                            now += dt;
                            let decision = reference.check(now);
                            prop_assert_eq!(shadow.check(now), Some(decision.clone()));
                            prop_assert_eq!(atomic.check(now), decision);
                        }
                        Op::SetQuota(r, b, dt) => {
                            now += dt;
                            reference.set_quota(Quota::new(r, b), now);
                            atomic.set_quota(Quota::new(r, b), now);
                            shadow.set(Some(Quota::new(r, b)), now);
                        }
                    }
                    prop_assert_eq!(atomic.has_debt(now), reference.has_debt(now));
                }
            }
        }
    }
}
