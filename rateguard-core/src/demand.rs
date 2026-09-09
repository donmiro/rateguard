use crate::gcra::Nanos;

#[derive(Debug, Clone, Copy)]
pub struct Demand {
    current_rate: f64,
    attempts_since_tick: u64,
    last_tick: Option<Nanos>,
    time_constant: Nanos,
}
impl Demand {
    pub fn new(time_constant: Nanos) -> Self {
        Self {
            current_rate: 0.0,
            attempts_since_tick: 0,
            last_tick: None,
            time_constant,
        }
    }

    pub fn record_attempt(&mut self) {
        self.attempts_since_tick += 1;
    }

    pub fn tick(&mut self, now: Nanos) {
        let Some(last) = self.last_tick else {
            self.last_tick = Some(now);
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

    pub fn rate(&self) -> f64 {
        self.current_rate
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
