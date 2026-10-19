//! A key's demand: an exponentially weighted moving average of attempts per
//! second.
//!
//! It counts attempts, not admissions. Counting admissions would close a
//! feedback loop: a small share admits little, so demand looks small, so the
//! share shrinks further. The weight of each tick depends on the time since
//! the previous one, so the average does not depend on how often it ticks.

use crate::gcra::Nanos;

/// One key's demand, in attempts per second.
#[derive(Debug, Clone, Copy)]
pub struct Demand {
    current_rate: f64,
    attempts_since_tick: u64,
    last_tick: Option<Nanos>,
    /// When measuring began: the first tick, or the moment given to
    /// `starting_at`.
    since: Option<Nanos>,
    time_constant: Nanos,
}
impl Demand {
    /// No attempts yet; `time_constant` is how fast the average follows a change.
    pub fn new(time_constant: Nanos) -> Self {
        Self {
            current_rate: 0.0,
            attempts_since_tick: 0,
            last_tick: None,
            since: None,
            time_constant,
        }
    }

    /// Starts measuring at `now`, so the first tick already has an interval
    /// to measure instead of spending it on a baseline.
    pub fn starting_at(time_constant: Nanos, now: Nanos) -> Self {
        Self {
            last_tick: Some(now),
            since: Some(now),
            ..Self::new(time_constant)
        }
    }

    /// Counts a request, admitted or not.
    pub fn record_attempt(&mut self) {
        self.attempts_since_tick += 1;
    }

    /// Counts `n` requests at once, admitted or not: what a runtime that
    /// enforces elsewhere collected since the last tick.
    pub fn record_attempts(&mut self, n: u64) {
        self.attempts_since_tick += n;
    }

    /// Folds the attempts since the last tick into the average.
    pub fn tick(&mut self, now: Nanos) {
        let Some(last) = self.last_tick else {
            self.last_tick = Some(now);
            self.since = Some(now);
            return;
        };

        let elapsed = now.saturating_sub(last);
        if elapsed == 0 {
            return;
        }

        let elapsed_secs = elapsed as f64 / 1_000_000_000.0;
        let instantaneous_rate = self.attempts_since_tick as f64 / elapsed_secs;

        let alpha = 1.0 - (-(elapsed as f64) / self.time_constant as f64).exp();
        self.current_rate = alpha * instantaneous_rate + (1.0 - alpha) * self.current_rate;

        self.attempts_since_tick = 0;
        self.last_tick = Some(now);
    }

    /// Whether attempts wait for a tick to be folded in: counted at the
    /// moment of the last tick, they had no interval to be measured over.
    pub fn has_pending(&self) -> bool {
        self.attempts_since_tick > 0
    }

    /// The average, in attempts per second, as of the last tick.
    pub fn rate(&self) -> f64 {
        self.current_rate
    }

    /// The average without the zero it started from: [`rate`](Demand::rate)
    /// divided by the weight the measured time has in it. The same as
    /// `rate` once measured for a few time constants; for a key seen only
    /// lately, its rate so far, where `rate` still shows a fraction of it.
    /// For comparing keys of different ages.
    pub fn unbiased_rate(&self) -> f64 {
        let (Some(since), Some(last)) = (self.since, self.last_tick) else {
            return 0.0;
        };
        let measured = last.saturating_sub(since);
        if measured == 0 {
            return 0.0;
        }
        let weight = 1.0 - (-(measured as f64) / self.time_constant as f64).exp();
        self.current_rate / weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_SEC: Nanos = 1_000_000_000;

    #[test]
    fn first_tick_sets_baseline_without_changing_rate() {
        let mut d = Demand::new(ONE_SEC);
        d.record_attempt();
        d.record_attempt();
        d.tick(0);
        assert_eq!(d.rate(), 0.0);
    }

    #[test]
    fn starting_at_measures_the_first_interval_instead_of_spending_it() {
        let mut d = Demand::starting_at(ONE_SEC, 0);
        for _ in 0..1000 {
            d.record_attempt();
        }
        d.tick(ONE_SEC);

        assert!(
            d.rate() > 0.0,
            "state created while traffic already flows cannot afford a warm-up tick"
        );
    }

    #[test]
    fn the_unbiased_rate_of_a_new_key_is_its_rate_so_far() {
        let mut d = Demand::starting_at(ONE_SEC, 0);
        assert_eq!(d.unbiased_rate(), 0.0);
        d.record_attempts(4);
        d.tick(ONE_SEC / 10);
        assert!(d.rate() < 5.0);
        assert!((d.unbiased_rate() - 40.0).abs() < 1e-9);
    }

    #[test]
    fn the_unbiased_rate_of_an_old_key_is_its_rate() {
        let mut d = Demand::starting_at(ONE_SEC, 0);
        for second in 1..=30 {
            d.record_attempts(10);
            d.tick(second * ONE_SEC);
        }
        assert!((d.unbiased_rate() - d.rate()).abs() < 1e-9);
    }

    #[test]
    fn zero_elapsed_tick_does_not_panic_or_drop_attempts() {
        let mut d = Demand::new(ONE_SEC);
        d.tick(0);
        d.record_attempt();
        d.tick(0);
        d.tick(ONE_SEC);
        assert!(d.rate() > 0.0);
    }

    #[test]
    fn rate_converges_towards_steady_load_gradually() {
        let mut d = Demand::new(ONE_SEC);
        d.tick(0);

        let mut now = 0;
        for _ in 0..1000 {
            d.record_attempt();
        }
        now += ONE_SEC;
        d.tick(now);
        let after_one_tick = d.rate();
        assert!(
            after_one_tick > 0.0 && after_one_tick < 1000.0,
            "shouldn't jump right away"
        );

        for _ in 0..4 {
            for _ in 0..1000 {
                d.record_attempt();
            }
            now += ONE_SEC;
            d.tick(now);
        }
        assert!(
            (d.rate() - 1000.0).abs() < 20.0,
            "after 5 ticks it should be close to 1000, got {}",
            d.rate()
        );
    }

    #[test]
    fn idle_key_decays_towards_zero() {
        let mut d = Demand::new(ONE_SEC);
        d.tick(0);
        for _ in 0..1000 {
            d.record_attempt();
        }
        d.tick(ONE_SEC);
        assert!(d.rate() > 0.0);

        d.tick(ONE_SEC + 10 * ONE_SEC);
        assert!(
            d.rate() < 1.0,
            "after 5 seconds of silence, the rating should almost disappear, we received {}",
            d.rate()
        );
    }
}
