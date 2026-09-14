use std::collections::HashMap;

use crate::gcra::Nanos;

const EVICTION_MARGIN: f64 = 1.1;

struct HotEntry {
    cooling_since: Option<Nanos>,
    demand: f64,
}

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

    fn weakest(&mut self) -> (u64, f64) {
        self.hot
            .iter()
            .min_by(|(ak, ae), (bk, be)| ae.demand.total_cmp(&be.demand).then(ak.cmp(bk)))
            .map(|(key, entry)| (*key, entry.demand))
            .expect("hot set is not empty: max_size > 0 and len() == max_size")
    }
}
