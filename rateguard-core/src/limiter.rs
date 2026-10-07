//! Enforcement on one node: a GCRA per key, on a share of the limit.
//!
//! A cold key gets `R/N × α`; a hot key gets the share it is given by
//! [`Limiter::tick_with_shares`], or `R/N` by [`Limiter::tick`].
//! [`Limiter::check`] is the hot path: one map lookup and one GCRA step, no
//! I/O, and no allocation for a key it already knows. The tick does
//! everything else once per protocol period.
//!
//! Memory is bounded by the config, not by how many keys there are: a key
//! with no debt and no demand is dropped without loss, and under pressure
//! the cold keys with the least demand go first.

use std::collections::HashMap;

use crate::demand::Demand;
use crate::gcra::{Decision, Gcra, Nanos, Quota};
use crate::hot_set::HotSet;

const SILENT_DEMAND: f64 = 1e-3;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// R: the cluster-wide limit for each key, in requests per second.
    pub limit_per_sec: u32,
    /// How many requests a key may get back to back on one node.
    pub burst: u32,
    /// α, in (0, 1]: the part of the per-node rate a cold key gets, and the
    /// threshold at which it turns hot.
    pub alpha: f64,
    /// β, in [0, 1]: the part of a hot key's limit split evenly among the
    /// nodes as a floor, so that a node with no demand yet can admit its
    /// first requests. The rest follows demand.
    pub floor_factor: f64,
    /// How long a hot key stays hot after its demand drops below the
    /// threshold.
    pub cooldown: Nanos,
    /// At most [`MAX_DEMAND_KEYS`](rateguard_proto::MAX_DEMAND_KEYS): one
    /// message must carry the whole hot set, or the peers would take the
    /// keys left out for keys with no demand here.
    pub hot_set_size: usize,
    /// Cap on the keys this node keeps any state for. Separate from
    /// `hot_set_size` and at least as big: hot keys are never reclaimed.
    pub max_tracked_keys: usize,
    /// How fast the demand average follows a change.
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
        assert!(
            (0.0..=1.0).contains(&self.floor_factor),
            "floor_factor must be in [0, 1]"
        );
        assert!(self.hot_set_size > 0, "hot_set_size  must be > 0");
        assert!(
            self.hot_set_size <= rateguard_proto::MAX_DEMAND_KEYS,
            "hot_set_size must be <= {}: a report carries no more keys",
            rateguard_proto::MAX_DEMAND_KEYS
        );
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

/// The limiter of one node.
#[derive(Debug)]
pub struct Limiter {
    config: Config,
    hot_set: HotSet,
    keys: HashMap<u64, KeyState>,
    cap: Option<f64>,
}
impl Limiter {
    pub fn new(config: Config) -> Self {
        config.validate();
        Self {
            hot_set: HotSet::new(config.cooldown, config.hot_set_size),
            keys: HashMap::new(),
            config,
            cap: None,
        }
    }

    /// Holds every key, hot or cold, at no more than `cap` requests per
    /// second, or lifts the cap. New keys follow at once, known ones from
    /// the next tick. For a node that has lost its quorum, see
    /// [`PartitionPolicy::Quorum`](crate::partition::PartitionPolicy).
    pub fn set_cap(&mut self, cap: Option<f64>) {
        self.cap = cap;
    }

    /// Admits or denies one request for `key`. The attempt counts toward the
    /// key's demand either way, cold keys included.
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

    /// Updates demand, moves keys between cold and hot, applies their new
    /// quotas and enforces `max_tracked_keys`. Once per protocol period.
    /// A hot key gets an even split, `R/N`: for a share that follows
    /// demand, see [`tick_with_shares`](Limiter::tick_with_shares).
    pub fn tick(&mut self, now: Nanos, cluster_size: usize) {
        let even = self.per_node_rate(cluster_size);
        self.tick_with_shares(now, cluster_size, |_| false, |_, _| even);
    }

    /// Like [`tick`](Limiter::tick), but a hot key gets the rate
    /// `share(key, own_demand)` returns, in requests per second, and a key
    /// for which `hot_elsewhere` is true is hot here too while it has any
    /// demand at all (see [`hot_set`](crate::hot_set)).
    pub fn tick_with_shares(
        &mut self,
        now: Nanos,
        cluster_size: usize,
        hot_elsewhere: impl Fn(u64) -> bool,
        mut share: impl FnMut(u64, f64) -> f64,
    ) {
        let threshold = self.per_node_rate(cluster_size) * self.config.alpha;
        let cold = self.cold_quota(cluster_size);
        let (burst, cap) = (self.config.burst, self.cap.unwrap_or(f64::INFINITY));
        let (keys, hot_set) = (&mut self.keys, &mut self.hot_set);

        keys.retain(|&key, state| {
            state.demand.tick(now);
            let rate = state.demand.rate();
            let elsewhere = rate >= SILENT_DEMAND && hot_elsewhere(key);
            let is_hot = hot_set.update_with(key, rate, threshold, now, elsewhere);

            if !is_hot && !state.gcra.has_debt(now) && state.demand.rate() < SILENT_DEMAND {
                return false;
            }

            let wanted = if is_hot {
                quota_for(share(key, state.demand.rate()).min(cap), burst)
            } else {
                cold
            };
            if state.applied != wanted {
                state.gcra.set_quota(wanted, now);
                state.applied = wanted;
            }
            true
        });

        self.enforce_cap();
    }

    /// The hot keys with their demand in attempts per second and whether
    /// they are hot by that demand (primary), busiest first: what this node
    /// has to tell its peers.
    pub fn hot_demand(&self) -> Vec<(u64, f64, bool)> {
        let mut hot: Vec<(u64, f64, bool)> = self
            .keys
            .iter()
            .filter(|&(&key, _)| self.hot_set.is_hot(key))
            .map(|(&key, state)| (key, state.demand.rate(), self.hot_set.is_primary(key)))
            .collect();
        hot.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        hot
    }

    pub fn config(&self) -> &Config {
        &self.config
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
        let cap = self.cap.unwrap_or(f64::INFINITY);
        quota_for(
            (self.per_node_rate(cluster_size) * self.config.alpha).min(cap),
            self.config.burst,
        )
    }
}

fn quota_for(rate_per_sec: f64, burst: u32) -> Quota {
    Quota::new(rate_per_sec.round().max(1.0) as u32, burst)
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
            floor_factor: 0.1,
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
    #[should_panic(expected = "hot_set_size must be <= 64")]
    fn a_hot_set_peers_cannot_hear_whole_is_refused() {
        Limiter::new(Config {
            hot_set_size: rateguard_proto::MAX_DEMAND_KEYS + 1,
            max_tracked_keys: 1000,
            ..config()
        });
    }

    #[test]
    #[should_panic(expected = "floor_factor must be in [0, 1]")]
    fn a_floor_over_the_whole_limit_is_refused() {
        Limiter::new(Config {
            floor_factor: 1.5,
            ..config()
        });
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
    fn hot_demand_lists_the_hot_keys_busiest_first() {
        let mut l = limiter();
        l.tick(0, N);
        for (key, attempts) in [(1, 600), (2, 900), (3, 10)] {
            for _ in 0..attempts {
                l.check(key, 0, N);
            }
        }
        l.tick(ONE_SEC, N);

        let listed: Vec<u64> = l.hot_demand().into_iter().map(|(key, ..)| key).collect();
        assert_eq!(listed, [2, 1], "key 3 is cold");
        let (_, busiest, primary) = l.hot_demand()[0];
        assert!(primary, "hot by its own demand");
        assert!(
            (busiest - 900.0 * (1.0 - (-1.0f64).exp())).abs() < 1.0,
            "{busiest}"
        );
    }

    #[test]
    fn a_key_hot_elsewhere_gets_a_share_here_too() {
        let mut l = limiter();
        l.tick(0, N);
        for _ in 0..50 {
            l.check(KEY, 0, N);
        }
        // 50 a second is under the hot threshold of 100: cold by itself.
        l.tick_with_shares(ONE_SEC, N, |key| key == KEY, |_, _| 250.0);
        assert!(l.is_hot(KEY));
        assert_eq!(l.hot_demand().len(), 1);
        let (_, _, primary) = l.hot_demand()[0];
        assert!(!primary, "hot only because a peer holds it so");

        let t = 2 * ONE_SEC;
        for _ in 0..config().burst {
            assert_eq!(l.check(KEY, t, N), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(KEY, t, N) else {
            panic!("burst must be exhausted");
        };
        assert_eq!(retry_at - t, 4_000_000, "the share, not the cold 100");
    }

    #[test]
    fn a_silent_key_is_not_kept_hot_by_peers() {
        let mut l = limiter();
        l.tick(0, N);
        l.check(KEY, 0, N);
        l.tick_with_shares(ONE_SEC, N, |_| true, |_, _| 250.0);
        assert!(l.is_hot(KEY), "a request a second ago is demand");
        l.tick_with_shares(60 * ONE_SEC, N, |_| true, |_, _| 250.0);
        assert!(!l.is_hot(KEY));
        assert_eq!(l.tracked_keys(), 0, "reclaimed as before");
    }

    fn interval_at(l: &mut Limiter, key: u64, t: Nanos) -> Nanos {
        for _ in 0..config().burst {
            assert_eq!(l.check(key, t, N), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(key, t, N) else {
            panic!("burst must be exhausted");
        };
        retry_at - t
    }

    #[test]
    fn a_cap_holds_every_key_down_hot_or_cold() {
        let mut l = limiter();
        drive_until_hot(&mut l);
        l.check(KEY + 1, ONE_SEC, N);

        l.set_cap(Some(20.0));
        l.tick_with_shares(2 * ONE_SEC, N, |_| false, |_, _| 250.0);
        assert_eq!(
            interval_at(&mut l, KEY, 3 * ONE_SEC),
            50_000_000,
            "hot: 20, not 250"
        );
        assert_eq!(
            interval_at(&mut l, KEY + 1, 3 * ONE_SEC),
            50_000_000,
            "cold: 20, not 100"
        );
        assert_eq!(
            interval_at(&mut l, KEY + 2, 3 * ONE_SEC),
            50_000_000,
            "a new key too, before any tick"
        );

        l.set_cap(None);
        l.tick_with_shares(4 * ONE_SEC, N, |_| false, |_, _| 250.0);
        assert_eq!(interval_at(&mut l, KEY, 5 * ONE_SEC), 4_000_000, "lifted");
    }

    #[test]
    fn a_hot_key_takes_the_share_it_is_given() {
        let mut l = limiter();
        drive_until_hot(&mut l);

        let mut asked = Vec::new();
        l.tick_with_shares(
            2 * ONE_SEC,
            N,
            |_| false,
            |key, own| {
                asked.push((key, own));
                250.0
            },
        );
        assert_eq!(asked.len(), 1, "asked for hot keys only");
        assert_eq!(asked[0].0, KEY);
        assert!(asked[0].1 > 100.0, "with its own demand: {}", asked[0].1);

        let t = 3 * ONE_SEC;
        for _ in 0..config().burst {
            assert_eq!(l.check(KEY, t, N), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(KEY, t, N) else {
            panic!("burst must be exhausted");
        };
        assert_eq!(retry_at - t, 4_000_000, "250 per second");
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
