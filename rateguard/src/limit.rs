//! A limit: what a class of keys, or the default class, is held to.

use std::time::Duration;

use rateguard_core::gcra::MAX_BURST;

use crate::config::Error;

/// A limit for each key of a class: a count of requests per second, minute
/// or hour across the whole fleet, and the burst one instance may admit
/// back to back on top of its share. See
/// [`Builder::class`](crate::Builder::class) and
/// [`Guard::set_class`](crate::Guard::set_class).
///
/// ```
/// use rateguard::Limit;
///
/// let pro = Limit::per_second(1_000).burst(50);
/// assert_eq!((pro.rate_per_sec(), pro.burst_size()), (1_000.0, 50));
/// let slow = Limit::per_hour(100);
/// assert_eq!((slow.count(), slow.burst_size()), (100, 5));
/// ```
///
/// A limit reads back as it was given: `per_minute(60)` is not
/// `per_second(1)`, though both mean the same rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    count: u32,
    period: Period,
    /// Set, or the default resolved at once: a limit given the default
    /// burst equals the same limit given it explicitly.
    burst: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Period {
    Second,
    Minute,
    Hour,
}
impl Period {
    fn seconds(self) -> u64 {
        match self {
            Period::Second => 1,
            Period::Minute => 60,
            Period::Hour => 3600,
        }
    }
}

impl Limit {
    fn new(count: u32, period: Period) -> Self {
        Self {
            count,
            period,
            burst: (count / 20).clamp(1, MAX_BURST),
        }
    }

    /// `n` requests per second per key, across every instance; the burst a
    /// twentieth of `n`, at least 1, unless set with [`burst`](Limit::burst).
    pub fn per_second(n: u32) -> Self {
        Self::new(n, Period::Second)
    }

    /// `n` requests per minute per key, across every instance; the burst a
    /// twentieth of `n`, at least 1, unless set with [`burst`](Limit::burst).
    pub fn per_minute(n: u32) -> Self {
        Self::new(n, Period::Minute)
    }

    /// `n` requests per hour per key, across every instance; the burst a
    /// twentieth of `n`, at least 1, unless set with [`burst`](Limit::burst).
    pub fn per_hour(n: u32) -> Self {
        Self::new(n, Period::Hour)
    }

    /// The burst: requests one instance may admit back to back. `0` is
    /// taken as 1.
    pub fn burst(self, burst: u32) -> Self {
        Self {
            burst: burst.max(1),
            ..self
        }
    }

    /// How many requests the limit allows in each [`period`](Limit::period).
    pub fn count(&self) -> u32 {
        self.count
    }

    /// The span the [`count`](Limit::count) is allowed in.
    pub fn period(&self) -> Duration {
        Duration::from_secs(self.period.seconds())
    }

    /// The rate, in requests per second: the count over the period.
    pub fn rate_per_sec(&self) -> f64 {
        self.count as f64 / self.period.seconds() as f64
    }

    /// The rate in whole requests per second, rounded up.
    #[deprecated(since = "0.7.0", note = "use rate_per_sec")]
    pub fn rate(&self) -> u32 {
        self.rate_per_sec().ceil() as u32
    }

    /// The burst, set or by default.
    pub fn burst_size(&self) -> u32 {
        self.burst
    }

    /// The limit as the core takes it, checked.
    pub(crate) fn to_core(self) -> Result<rateguard_core::limiter::Limit, Error> {
        if self.count == 0 {
            return Err(Error::ZeroLimit);
        }
        let burst = self.burst_size();
        if burst > MAX_BURST {
            return Err(Error::TooLargeBurst(burst));
        }
        Ok(rateguard_core::limiter::Limit {
            rate: self.rate_per_sec(),
            burst,
        })
    }
}
