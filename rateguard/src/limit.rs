//! A limit: what a class of keys, or the default class, is held to.

use rateguard_core::gcra::MAX_BURST;

use crate::config::Error;

/// A limit for each key of a class: requests per second across the whole
/// fleet, and the burst one instance may admit back to back on top of its
/// share. See [`Builder::class`](crate::Builder::class) and
/// [`Guard::set_class`](crate::Guard::set_class).
///
/// ```
/// use rateguard::Limit;
///
/// let pro = Limit::per_second(1_000).burst(50);
/// assert_eq!((pro.rate(), pro.burst_size()), (1_000, 50));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    per_second: u32,
    /// Set, or the default resolved at once: a limit given the default
    /// burst equals the same limit given it explicitly.
    burst: u32,
}
impl Limit {
    /// `n` requests per second per key, across every instance; the burst a
    /// twentieth of it, at least 1, unless set with [`burst`](Limit::burst).
    pub fn per_second(n: u32) -> Self {
        Self {
            per_second: n,
            burst: (n / 20).clamp(1, MAX_BURST),
        }
    }

    /// The burst: requests one instance may admit back to back. `0` is
    /// taken as 1.
    pub fn burst(self, burst: u32) -> Self {
        Self {
            burst: burst.max(1),
            ..self
        }
    }

    /// The rate, in requests per second.
    pub fn rate(&self) -> u32 {
        self.per_second
    }

    /// The burst, set or by default.
    pub fn burst_size(&self) -> u32 {
        self.burst
    }

    /// The limit as the core takes it, checked.
    pub(crate) fn to_core(self) -> Result<rateguard_core::limiter::Limit, Error> {
        if self.per_second == 0 {
            return Err(Error::ZeroLimit);
        }
        let burst = self.burst_size();
        if burst > MAX_BURST {
            return Err(Error::TooLargeBurst(burst));
        }
        Ok(rateguard_core::limiter::Limit {
            per_second: self.per_second,
            burst,
        })
    }

    pub(crate) fn from_core(limit: rateguard_core::limiter::Limit) -> Self {
        Self::per_second(limit.per_second).burst(limit.burst)
    }
}
