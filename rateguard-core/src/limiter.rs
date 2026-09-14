use std::collections::HashMap;

use crate::demand::Demand;
use crate::gcra::{Decision, Gcra, Nanos, Quota};
use crate::hot_set::HotSet;

const SILENT_DEMAND: f64 = 1e-3;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub limit_per_sec: u32,
    pub burst: u32,
    pub alpha: f64,
    pub cooldown: Nanos,
    pub hot_set_size: usize,
    pub max_tracked_keys: usize,
    pub demand_time_constant: Nanos,
}
impl Config {
    fn validate(&self) {
        assert!(self.limit_per_sec > 0, "limit_per_sec must be > 0");
        assert!(self.burst > 0, "burst must be > 0");
        assert!(
            self.alpha > 0.0 && self.alpha <= 1.0,
            "alpha bust be in (0, 1]"
        );
        assert!(self.hot_set_size > 0, "hot_set_size  must be > 0");
        assert!(
            self.max_tracked_keys >= self.hot_set_size,
            "max_tracked_keys must be >= hot_set_size: hot keys are never reclaimed"
        );
    }
}

#[derive(Debug)]
struct KeyState {
    demand: Demand,
    gcra: Gcra,
    applied: Quota,
}

#[derive(Debug)]
pub struct Limiter {
    config: Config,
    hot_set: HotSet,
    keys: HashMap<u64, KeyState>,
}
impl Limiter {
    pub fn new(config: Config) -> Self {
        config.validate();
        Self {
            hot_set: HotSet::new(config.cooldown, config.hot_set_size),
            keys: HashMap::new(),
            config,
        }
    }

    pub fn check(&mut self, key: u64, now: Nanos, cluster_size: usize) -> Decision {
        let cold = self.cold_quota(cluster_size);
        let time_constant = self.config.demand_time_constant;

        let state = self.keys.entry(key).or_insert_with(|| KeyState {
            demand: Demand::starting_at(time_constant, now),
            gcra: Gcra::new(cold),
            applied: cold,
        });

        state.demand.record_attempt();
        state.gcra.check(now)
    }

    pub fn tick(&mut self, now: Nanos, cluster_size: usize) {
        let threshold = self.per_node_rate(cluster_size) * self.config.alpha;
        let cold = self.cold_quota(cluster_size);
        let hot = self.hot_quota(cluster_size);

        let Self { keys, hot_set, .. } = self;
        keys.retain(|&key, state| {
            state.demand.tick(now);
            let is_hot = hot_set.update(key, state.demand.rate(), threshold, now);

            if !is_hot && !state.gcra.has_debt(now) && state.demand.rate() < SILENT_DEMAND {
                return false;
            }

            let wanted = if is_hot { hot } else { cold };
            if state.applied != wanted {
                state.gcra.set_quota(wanted, now);
                state.applied = wanted;
            }
            true
        });

        self.enforce_cap();
    }

    pub fn is_hot(&self, key: u64) -> bool {
        self.hot_set.is_hot(key)
    }

    pub fn tracked(&self, key: u64) -> bool {
        self.keys.contains_key(&key)
    }

    pub fn tracked_keys(&self) -> usize {
        self.keys.len()
    }

    pub fn hot_keys(&self) -> usize {
        self.hot_set.len()
    }

    fn enforce_cap(&mut self) {
        if self.keys.len() <= self.config.max_tracked_keys {
            return;
        }
        let excess = self.keys.len() - self.config.max_tracked_keys;

        let mut victims: Vec<(u64, f64)> = self
            .keys
            .iter()
            .filter(|&(&key, _)| !self.hot_set.is_hot(key))
            .map(|(&key, state)| (key, state.demand.rate()))
            .collect();

        victims.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));

        for (key, _) in victims.into_iter().take(excess) {
            self.keys.remove(&key);
        }
    }

    fn per_node_rate(&self, cluster_size: usize) -> f64 {
        assert!(cluster_size > 0, "cluster_size must be > 0");
        self.config.limit_per_sec as f64 / cluster_size as f64
    }

    fn cold_quota(&self, cluster_size: usize) -> Quota {
        self.quota_for(self.per_node_rate(cluster_size) * self.config.alpha)
    }

    fn hot_quota(&self, cluster_size: usize) -> Quota {
        self.quota_for(self.per_node_rate(cluster_size))
    }

    fn quota_for(&self, rate_per_sec: f64) -> Quota {
        Quota::new(rate_per_sec.round().max(1.0) as u32, self.config.burst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_SEC: Nanos = 1_000_000_000;
    const N: usize = 5;
    const KEY: u64 = 42;

    fn config() -> Config {
        Config {
            limit_per_sec: 1000,
            burst: 10,
            alpha: 0.5,
            cooldown: 5 * ONE_SEC,
            hot_set_size: 4,
            max_tracked_keys: 8,
            demand_time_constant: ONE_SEC,
        }
    }

    fn limiter() -> Limiter {
        Limiter::new(config())
    }

    fn drive_until_hot(l: &mut Limiter) {
        l.tick(0, N);
        for _ in 0..500 {
            l.check(KEY, 0, N);
        }
        l.tick(ONE_SEC, N);
        assert!(l.is_hot(KEY));
    }

    #[test]
    #[should_panic]
    fn cap_below_hot_set_size_should_panic() {
        Limiter::new(Config {
            max_tracked_keys: 3,
            hot_set_size: 4,
            ..config()
        });
    }

    #[test]
    fn cold_key_is_held_to_its_alpha_share() {
        let mut l = limiter();
        for _ in 0..config().burst {
            assert_eq!(l.check(KEY, 0, N), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(KEY, 0, N) else {
            panic!("burst must be exhausted");
        };
        assert_eq!(retry_at, 10_000_000);
    }

    #[test]
    fn denied_attempts_of_a_cold_key_still_drive_promotion() {
        let mut l = limiter();
        l.tick(0, N);

        for _ in 0..500 {
            l.check(KEY, 0, N);
        }
        assert!(
            !l.is_hot(KEY),
            "promotion belongs to the tick, not to check()"
        );

        l.tick(ONE_SEC, N);
        assert!(
            l.is_hot(KEY),
            "490 of those 500 attempts were denied; counting only admissions would never promote"
        );
    }

    #[test]
    fn promotion_widens_the_quota_from_alpha_share_to_full_share() {
        let mut l = limiter();
        drive_until_hot(&mut l);

        let t = 100 * ONE_SEC;
        for _ in 0..config().burst {
            assert_eq!(l.check(KEY, t, N), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(KEY, t, N) else {
            panic!("burst must be exhausted");
        };
        assert_eq!(retry_at - t, 5_000_000);
    }

    #[test]
    fn a_silent_key_is_reclaimed() {
        let mut l = limiter();
        l.tick(0, N);
        l.check(KEY, 0, N);

        l.tick(ONE_SEC, N);
        assert_eq!(l.tracked_keys(), 1, "still warm right after the request");

        l.tick(60 * ONE_SEC, N);
        assert_eq!(
            l.tracked_keys(),
            0,
            "debt drained and EWMAdecayed to nothing"
        );
    }

    #[test]
    fn a_hot_key_survives_silence_until_it_demotes() {
        let mut l = limiter();
        drive_until_hot(&mut l);

        let cooling_starts = 5 * ONE_SEC;
        l.tick(cooling_starts, N);
        assert!(l.is_hot(KEY), "still hot, merely cooling");

        l.tick(cooling_starts + config().cooldown, N);
        assert!(!l.is_hot(KEY), "cooldown elapsed");
        assert_eq!(
            l.tracked_keys(),
            1,
            "demotion is not reclamation: the EWMA has not decayed away yet"
        );

        l.tick(30 * ONE_SEC, N);
        assert_eq!(l.tracked_keys(), 0);
    }

    #[test]
    fn tracked_keys_are_capped_after_a_tick() {
        let mut l = limiter();
        l.tick(0, N);

        for key in 0..20u64 {
            for _ in 0..=key {
                l.check(key, 0, N);
            }
        }

        assert_eq!(l.tracked_keys(), 20, "check() lets the map overshoot");

        l.tick(ONE_SEC, N);
        assert_eq!(l.tracked_keys(), 8);
    }

    #[test]
    fn the_cap_drops_the_weakest_keys_first() {
        let mut l = limiter();
        l.tick(0, N);

        for key in 0..20u64 {
            for _ in 0..=key {
                l.check(key, 0, N);
            }
        }
        l.tick(ONE_SEC, N);

        let mut survivors: Vec<u64> = (0..20u64).filter(|&k| l.tracked(k)).collect();
        survivors.sort_unstable();
        assert_eq!(survivors, vec![12, 13, 14, 15, 16, 17, 18, 19]);
    }

    #[test]
    fn hot_keys_are_never_dropped_by_the_cap() {
        let mut l = limiter();
        l.tick(0, N);

        for key in 0..4u64 {
            for _ in 0..500 {
                l.check(key, 0, N);
            }
        }
        for key in 100..116u64 {
            l.check(key, 0, N);
        }

        l.tick(ONE_SEC, N);
        assert_eq!(l.hot_keys(), 4);
        assert_eq!(l.tracked_keys(), 8);
        for key in 0..4u64 {
            assert!(
                l.is_hot(key),
                "a hot key outranks any cold one under pressure"
            );
        }
    }

    #[test]
    fn shrinking_the_cluster_widens_the_cold_share() {
        let mut l = limiter();
        l.check(KEY, 0, N);
        l.tick(ONE_SEC, N);

        l.tick(2 * ONE_SEC, 2);
        let t = 3 * ONE_SEC;
        for _ in 0..config().burst {
            assert_eq!(l.check(KEY, t, 2), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(KEY, t, 2) else {
            panic!("burst must be exhausted");
        };
        assert_eq!(retry_at - t, 4_000_000);
    }
}
