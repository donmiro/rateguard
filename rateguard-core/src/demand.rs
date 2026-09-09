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
