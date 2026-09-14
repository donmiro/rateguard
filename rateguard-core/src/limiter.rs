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
}
