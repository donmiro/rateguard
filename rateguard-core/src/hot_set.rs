use std::collections::HashMap;

use crate::gcra::Nanos;

const EVICTION_MARGIN: f64 = 1.1;

#[derive(Debug)]
struct HotEntry {
    cooling_since: Option<Nanos>,
    demand: f64,
}

#[derive(Debug)]
pub struct HotSet {
    cooldown: Nanos,
    max_size: usize,
    hot: HashMap<u64, HotEntry>,
}
impl HotSet {
    pub fn new(cooldown: Nanos, max_size: usize) -> Self {
        assert!(max_size > 0, "max_size must be > 0");
        Self {
            cooldown,
            max_size,
            hot: HashMap::with_capacity(max_size),
        }
    }

    pub fn update(&mut self, key: u64, demand_rate: f64, threshold: f64, now: Nanos) -> bool {
        if demand_rate > threshold {
            self.promote(key, demand_rate)
        } else {
            self.cool(key, demand_rate, now)
        }
    }

    pub fn is_hot(&self, key: u64) -> bool {
        self.hot.contains_key(&key)
    }

    pub fn len(&self) -> usize {
        self.hot.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hot.is_empty()
    }

    fn promote(&mut self, key: u64, demand_rate: f64) -> bool {
        if let Some(entry) = self.hot.get_mut(&key) {
            entry.demand = demand_rate;
            entry.cooling_since = None;
            return true;
        }

        if self.hot.len() < self.max_size {
            self.insert(key, demand_rate);
            return true;
        }

        let (weakest_key, weakest_demand) = self.weakest();
        if demand_rate > weakest_demand * EVICTION_MARGIN {
            self.hot.remove(&weakest_key);
            self.insert(key, demand_rate);
            true
        } else {
            false
        }
    }

    fn cool(&mut self, key: u64, demand_rate: f64, now: Nanos) -> bool {
        let Some(entry) = self.hot.get_mut(&key) else {
            return false;
        };
        entry.demand = demand_rate;

        let expired = match entry.cooling_since {
            None => {
                entry.cooling_since = Some(now);
                false
            }
            Some(since) => now.saturating_sub(since) >= self.cooldown,
        };

        if expired {
            self.hot.remove(&key);
        }
        !expired
    }

    fn insert(&mut self, key: u64, demand_rate: f64) {
        self.hot.insert(
            key,
            HotEntry {
                cooling_since: None,
                demand: demand_rate,
            },
        );
    }

    fn weakest(&self) -> (u64, f64) {
        self.hot
            .iter()
            .min_by(|(ak, ae), (bk, be)| ae.demand.total_cmp(&be.demand).then(ak.cmp(bk)))
            .map(|(key, entry)| (*key, entry.demand))
            .expect("hot set is not empty: max_size > 0 and len() == max_size")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_SEC: Nanos = 1_000_000_000;
    const COOLDOWN: Nanos = 5 * ONE_SEC;
    const THRESHOLD: f64 = 100.0;

    fn hot_set(max_size: usize) -> HotSet {
        HotSet::new(COOLDOWN, max_size)
    }

    #[test]
    #[should_panic]
    fn zero_max_size_should_panic() {
        HotSet::new(COOLDOWN, 0);
    }

    #[test]
    fn cold_keys_leave_no_state() {
        let mut hs = hot_set(512);
        for key in 0..100 {
            assert!(!hs.update(key, THRESHOLD - 1.0, THRESHOLD, 0));
        }
        assert!(hs.is_empty(), "cold keys should not occupy memory.");
    }

    #[test]
    fn demand_exactly_at_threshold_stays_cold() {
        let mut hs = hot_set(512);
        assert!(!hs.update(1, THRESHOLD, THRESHOLD, 0));
        assert!(!hs.is_hot(1));
    }

    #[test]
    fn promotes_once_demand_exceeds_threshold() {
        let mut hs = hot_set(512);
        assert!(hs.update(1, THRESHOLD + 0.1, THRESHOLD, 0));
        assert!(hs.is_hot(1));
        assert_eq!(hs.len(), 1);
    }

    #[test]
    fn stays_hot_until_cooldown_elapses() {
        let mut hs = hot_set(512);
        hs.update(1, 500.0, THRESHOLD, 0);

        assert!(hs.update(1, 10.0, THRESHOLD, ONE_SEC));
        assert!(hs.update(1, 10.0, THRESHOLD, ONE_SEC + COOLDOWN - 1));
        assert!(
            hs.is_hot(1),
            "the key remains hot until the cooldown expires"
        );

        assert!(!hs.update(1, 10.0, THRESHOLD, ONE_SEC + COOLDOWN));
        assert!(hs.is_empty(), "a demotion must release the record");
    }

    #[test]
    fn crossing_threshold_again_resets_cooldown() {
        let mut hs = hot_set(512);
        hs.update(1, 500.0, THRESHOLD, 0);
        hs.update(1, 10.0, THRESHOLD, ONE_SEC);
        hs.update(1, 500.0, THRESHOLD, ONE_SEC + COOLDOWN - 1);

        assert!(
            hs.update(1, 10.0, THRESHOLD, ONE_SEC + COOLDOWN + 1),
            "the cooldown timer was supposed to reset, not finish counting down the old one"
        );
        assert!(hs.is_hot(1));
    }

    #[test]
    fn refreshing_a_hot_key_does_not_consume_a_slot() {
        let mut hs = hot_set(1);
        hs.update(1, 500.0, THRESHOLD, 0);
        assert!(hs.update(1, 900.0, THRESHOLD, ONE_SEC));
        assert_eq!(hs.len(), 1);
    }

    #[test]
    fn never_exceeds_max_size() {
        let mut hs = hot_set(4);
        for key in 0..100u64 {
            hs.update(key, 1000.0 * (key + 1) as f64, THRESHOLD, 0);
            assert!(hs.len() <= 4, "hot set should not exceed config");
        }
        assert_eq!(hs.len(), 4);
    }

    #[test]
    fn a_much_stronger_key_always_gets_in() {
        let mut hs = hot_set(4);
        for key in 0..4u64 {
            hs.update(key, 500.0, THRESHOLD, 0);
        }
        assert!(hs.update(99, 5000.0, THRESHOLD, 0));
        assert!(hs.is_hot(99));
        assert_eq!(hs.len(), 4);
    }

    #[test]
    fn incumbents_are_sticky_against_merginally_stronger_keys() {
        let mut hs = hot_set(1);
        hs.update(1, 1000.0, THRESHOLD, 0);

        assert!(!hs.update(2, 1050.0, THRESHOLD, 0));
        assert!(hs.is_hot(1) && !hs.is_hot(2));
    }

    #[test]
    fn evicts_the_weakest_key() {
        let mut hs = hot_set(3);
        hs.update(1, 900.0, THRESHOLD, 0);
        hs.update(2, 200.0, THRESHOLD, 0);
        hs.update(3, 700.0, THRESHOLD, 0);

        assert!(hs.update(4, 500.0, THRESHOLD, 0));
        assert!(!hs.is_hot(2), "the weakest is displaced");
        assert!(hs.is_hot(1) && hs.is_hot(3) && hs.is_hot(4));
        assert_eq!(hs.len(), 3);
    }

    #[test]
    fn candidate_without_margin_does_not_evict() {
        let mut hs = hot_set(2);
        hs.update(1, 500.0, THRESHOLD, 0);
        hs.update(2, 400.0, THRESHOLD, 0);

        assert!(
            !hs.update(3, 420.0, THRESHOLD, 0),
            "demand is above the threshold but not sufficiently above that of the weakest performer — the promotion does not go through"
        );
        assert!(!hs.is_hot(3));
        assert!(hs.is_hot(2));
        assert_eq!(hs.len(), 2);
    }

    #[test]
    fn cooling_key_is_evicted_before_active_ones() {
        let mut hs = hot_set(2);
        hs.update(1, 900.0, THRESHOLD, 0);
        hs.update(2, 800.0, THRESHOLD, 0);

        hs.update(2, 5.0, THRESHOLD, ONE_SEC);
        assert!(hs.is_hot(2), "the cooling key is still in the set");

        assert!(hs.update(3, 600.0, THRESHOLD, ONE_SEC));
        assert!(!hs.is_hot(2));
        assert!(hs.is_hot(1) && hs.is_hot(3));
    }

    #[test]
    fn fully_decayed_hot_key_can_always_be_displaced() {
        let mut hs = hot_set(1);
        hs.update(1, 900.0, THRESHOLD, 0);
        hs.update(1, 0.0, THRESHOLD, ONE_SEC);

        assert!(
            hs.update(2, 150.0, THRESHOLD, ONE_SEC),
            "the margin must not protect a dead key"
        );
        assert!(hs.is_hot(2) && !hs.is_hot(1));
    }

    #[test]
    fn eviction_is_deterministic_under_ties() {
        for _ in 0..20 {
            let mut hs = hot_set(3);
            for key in [10u64, 20, 30] {
                hs.update(key, 500.0, THRESHOLD, 0);
            }
            assert!(hs.update(40, 600.0, THRESHOLD, 0));
            assert!(
                !hs.is_hot(10),
                "given equal demand, the least significant key is displaced"
            );
        }
    }
}
