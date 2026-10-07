//! The keys that need coordination.
//!
//! A key is hot while its demand is above the threshold `R/N × α` (see
//! [`limiter`](crate::limiter)). Below it a key is served locally on a fixed
//! share, and all such keys together cannot exceed `R × α` cluster-wide. Hot
//! keys have their demand gossiped and get a share proportional to it.
//!
//! A key that drops below the threshold stays hot for `cooldown` before it
//! is demoted, so a key near the line does not flap. When the set is full a
//! newcomer evicts the weakest key, but only if it beats it by 10%, for the
//! same reason.
//!
//! A key hot by its own demand here, cooldown included, is *primary*. A key
//! a peer reports primary is hot here too, as *secondary*, whatever the
//! demand here: otherwise a key hot on one node and cold on the others
//! would overshoot, the hot node taking all of `R` and the cold ones their
//! local share on top (spec §4.1). A secondary key leaves as soon as no
//! peer holds it primary, and only primary news makes a key hot elsewhere,
//! so two nodes cannot keep a key hot for each other forever.

use std::collections::HashMap;

use crate::gcra::Nanos;

const EVICTION_MARGIN: f64 = 1.1;

#[derive(Debug)]
struct HotEntry {
    cooling_since: Option<Nanos>,
    demand: f64,
    primary: bool,
    since: Nanos,
}

/// The hot keys of one node, bounded in size.
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

    /// Feeds a key's current demand and returns whether the key is hot
    /// afterwards.
    pub fn update(&mut self, key: u64, demand_rate: f64, threshold: f64, now: Nanos) -> bool {
        self.update_with(key, demand_rate, threshold, now, false)
    }

    /// Like [`update`](HotSet::update), knowing whether a peer holds the
    /// key primary.
    pub fn update_with(
        &mut self,
        key: u64,
        demand_rate: f64,
        threshold: f64,
        now: Nanos,
        hot_elsewhere: bool,
    ) -> bool {
        if demand_rate > threshold {
            return self.promote(key, demand_rate, true, now);
        }
        if !self.cool(key, demand_rate, now) {
            return false;
        }
        match self.hot.get(&key) {
            Some(entry) if entry.primary => true,
            Some(_) if hot_elsewhere => true,
            Some(_) => {
                self.hot.remove(&key);
                false
            }
            None => hot_elsewhere && self.promote(key, demand_rate, false, now),
        }
    }

    pub fn is_hot(&self, key: u64) -> bool {
        self.hot.contains_key(&key)
    }

    /// Since when the key has been hot without a break, if it is.
    pub fn hot_since(&self, key: u64) -> Option<Nanos> {
        self.hot.get(&key).map(|entry| entry.since)
    }

    /// Whether the key is hot by its own demand here, cooldown included.
    pub fn is_primary(&self, key: u64) -> bool {
        self.hot.get(&key).is_some_and(|entry| entry.primary)
    }

    pub fn len(&self) -> usize {
        self.hot.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hot.is_empty()
    }

    fn promote(&mut self, key: u64, demand_rate: f64, primary: bool, now: Nanos) -> bool {
        if let Some(entry) = self.hot.get_mut(&key) {
            entry.demand = demand_rate;
            entry.cooling_since = None;
            entry.primary = primary;
            return true;
        }

        if self.hot.len() < self.max_size {
            self.insert(key, demand_rate, primary, now);
            return true;
        }

        let (weakest_key, weakest_demand) = self.weakest();
        if demand_rate > weakest_demand * EVICTION_MARGIN {
            self.hot.remove(&weakest_key);
            self.insert(key, demand_rate, primary, now);
            true
        } else {
            false
        }
    }

    // Below the threshold. A primary key counts down its cooldown and then
    // stops being primary; whether it stays as secondary is the caller's
    // call. Returns false only for a key that is not in the set at all and
    // so has nothing to cool: the caller may still promote it.
    fn cool(&mut self, key: u64, demand_rate: f64, now: Nanos) -> bool {
        let Some(entry) = self.hot.get_mut(&key) else {
            return true;
        };
        entry.demand = demand_rate;
        if !entry.primary {
            return true;
        }

        let expired = match entry.cooling_since {
            None => {
                entry.cooling_since = Some(now);
                false
            }
            Some(since) => now.saturating_sub(since) >= self.cooldown,
        };
        if expired {
            entry.primary = false;
            entry.cooling_since = None;
        }
        true
    }

    fn insert(&mut self, key: u64, demand_rate: f64, primary: bool, now: Nanos) {
        self.hot.insert(
            key,
            HotEntry {
                cooling_since: None,
                demand: demand_rate,
                primary,
                since: now,
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

    #[test]
    fn a_key_hot_elsewhere_is_hot_here_but_not_primary() {
        let mut hs = hot_set(4);
        assert!(hs.update_with(1, 10.0, THRESHOLD, 0, true));
        assert!(hs.is_hot(1));
        assert!(!hs.is_primary(1));
    }

    #[test]
    fn a_secondary_key_leaves_as_soon_as_no_peer_holds_it_hot() {
        let mut hs = hot_set(4);
        hs.update_with(1, 10.0, THRESHOLD, 0, true);
        assert!(!hs.update_with(1, 10.0, THRESHOLD, 1, false));
        assert!(hs.is_empty());
    }

    #[test]
    fn a_cooling_key_is_still_primary() {
        let mut hs = hot_set(4);
        hs.update(1, 500.0, THRESHOLD, 0);
        hs.update_with(1, 10.0, THRESHOLD, ONE_SEC, true);
        assert!(hs.is_primary(1), "within its own cooldown");
    }

    #[test]
    fn a_cooled_key_stays_on_as_secondary_while_hot_elsewhere() {
        let mut hs = hot_set(4);
        hs.update(1, 500.0, THRESHOLD, 0);
        hs.update_with(1, 10.0, THRESHOLD, ONE_SEC, true);
        assert!(hs.update_with(1, 10.0, THRESHOLD, ONE_SEC + COOLDOWN, true));
        assert!(!hs.is_primary(1));

        assert!(!hs.update_with(1, 10.0, THRESHOLD, ONE_SEC + COOLDOWN + 1, false));
        assert!(hs.is_empty());
    }

    #[test]
    fn own_demand_makes_a_secondary_key_primary() {
        let mut hs = hot_set(4);
        hs.update_with(1, 10.0, THRESHOLD, 0, true);
        hs.update_with(1, 500.0, THRESHOLD, ONE_SEC, true);
        assert!(hs.is_primary(1));
    }

    #[test]
    fn secondary_keys_count_against_the_size() {
        let mut hs = hot_set(2);
        hs.update(1, 900.0, THRESHOLD, 0);
        hs.update_with(2, 50.0, THRESHOLD, 0, true);
        assert!(
            !hs.update_with(3, 40.0, THRESHOLD, 0, true),
            "full, and not 10% over the weakest"
        );
        assert_eq!(hs.len(), 2);
    }

    #[test]
    fn a_hot_key_remembers_since_when() {
        let mut hs = hot_set(4);
        assert_eq!(hs.hot_since(1), None);
        hs.update(1, 500.0, THRESHOLD, ONE_SEC);
        hs.update(1, 600.0, THRESHOLD, 2 * ONE_SEC);
        assert_eq!(hs.hot_since(1), Some(ONE_SEC));

        // Primary to secondary is no break: still hot all along.
        hs.update_with(1, 10.0, THRESHOLD, 3 * ONE_SEC, true);
        hs.update_with(1, 10.0, THRESHOLD, 3 * ONE_SEC + COOLDOWN, true);
        assert_eq!(hs.hot_since(1), Some(ONE_SEC));

        hs.update(1, 10.0, THRESHOLD, 3 * ONE_SEC + COOLDOWN + 1);
        hs.update(1, 500.0, THRESHOLD, 20 * ONE_SEC);
        assert_eq!(hs.hot_since(1), Some(20 * ONE_SEC), "hot again, anew");
    }
}
