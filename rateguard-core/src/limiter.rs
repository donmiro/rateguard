//! Enforcement on one node: a GCRA per key, on a share of the limit.
//!
//! Every key has a [`Limit`] of its own, R: the config's by default, or one
//! given with it. A cold key gets `R/N × α`; a hot key gets the share it is
//! given by [`Limiter::tick_with_shares`], or `R/N` by [`Limiter::tick`]. A
//! key a peer holds hot that finds no room in the hot set here gets the
//! floor `R × β / N`: the peer takes all of the limit but that.
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

/// How fast the per-key counts, requests, denials and shadow denials,
/// follow a change: slower than demand, so that a key denied in bursts
/// stays listed between them, and all three share one window, so that
/// their ratios mean something.
const DENIAL_TIME_CONSTANT: Nanos = 10_000_000_000;

/// One key's limit: R, the cluster-wide rate, and the burst one node may
/// admit back to back.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limit {
    /// R, in requests per second, across the whole cluster. Fractional: a
    /// limit of 1 an hour is `1.0 / 3600.0`. Finite and above zero.
    pub rate: f64,
    /// How many requests the key may get back to back on one node.
    pub burst: u32,
}
impl Limit {
    fn validate(&self) {
        assert!(
            self.rate.is_finite() && self.rate > 0.0,
            "a limit must be finite and > 0 a second"
        );
        assert!(
            self.burst > 0 && self.burst <= crate::gcra::MAX_BURST,
            "a burst must be in 1..={}",
            crate::gcra::MAX_BURST
        );
    }
}

#[derive(Debug, Clone, Copy)]
/// How one node enforces the limit; see the fields.
pub struct Config {
    /// The cluster-wide limit R of a key given none of its own, and the
    /// burst such a key may get back to back on one node.
    pub limit: Limit,
    /// α, in (0, 1]: the part of the per-node rate a cold key gets, and the
    /// threshold at which it turns hot.
    pub alpha: f64,
    /// β, in [0, 1]: the part of a hot key's limit split evenly among the
    /// nodes as a floor, so that a node with no demand yet can admit its
    /// first requests. The rest follows demand. A share, however small,
    /// zero included, still lets `burst` through on every node: that is the
    /// `R + N × burst` of every bound.
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
    ///
    /// Enforced at every tick: in between, each key not seen before adds
    /// an entry, so the map holds at most this many plus the new keys of
    /// one protocol period. The rateguard runtime feeds the limiter only
    /// keys from its fixed-size table, twice `max_tracked_keys` slots, so
    /// there it never holds more than three times this.
    pub max_tracked_keys: usize,
    /// How fast the demand average follows a change.
    pub demand_time_constant: Nanos,
}
impl Config {
    fn validate(&self) {
        assert!(
            self.limit.rate.is_finite() && self.limit.rate > 0.0,
            "limit.rate must be finite and > 0"
        );
        assert!(self.limit.burst > 0, "burst must be > 0");
        assert!(
            self.limit.burst <= crate::gcra::MAX_BURST,
            "burst must be <= {}: a quota holds no more",
            crate::gcra::MAX_BURST
        );
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

/// What a runtime that enforces in its own table counted for one key
/// since it last told: see [`Limiter::record`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    /// Requests, admitted or not.
    pub attempts: u64,
    /// Of those, the ones denied.
    pub denials: u64,
    /// Of the attempts, the ones the key's shadow limit, if it has one,
    /// would have denied, whatever the real limit decided.
    pub shadow_denials: u64,
}

/// A key's requests, denials and shadow denials a second, over the last
/// ten seconds or so: one window for all three, so that a key's share of
/// denials is their ratio.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KeyRates {
    /// Requests, admitted or not.
    pub requests_per_sec: f64,
    /// Of those, the ones denied.
    pub denied_per_sec: f64,
    /// The ones the key's shadow limit would have denied.
    pub shadow_denied_per_sec: f64,
}

#[derive(Debug)]
struct KeyState {
    limit: Limit,
    /// Attempts over the node's demand window: what the shares follow.
    demand: Demand,
    /// The same attempts over the window of the counts below.
    requests: Demand,
    denials: Demand,
    shadow_denials: Demand,
    gcra: Gcra,
    applied: Quota,
}

impl KeyState {
    // Unbiased, for reading: one seen only lately would otherwise show a
    // fraction of its rates. Not for ranking, see `Limiter::most_denied`.
    fn rates(&self) -> KeyRates {
        KeyRates {
            requests_per_sec: self.requests.unbiased_rate(),
            denied_per_sec: self.denials.unbiased_rate(),
            shadow_denied_per_sec: self.shadow_denials.unbiased_rate(),
        }
    }
}

/// The limiter of one node.
#[derive(Debug)]
pub struct Limiter {
    config: Config,
    hot_set: HotSet,
    keys: HashMap<u64, KeyState>,
    /// The shadow limiters of [`check_in_traced`](Limiter::check_in_traced),
    /// for the tracked keys that have one: apart from `keys`, so that a
    /// runtime, which keeps its shadows in its own table, pays nothing for
    /// them per key.
    shadows: HashMap<u64, Gcra>,
    /// A part of each key's R that no key gets more than.
    cap: Option<f64>,
    /// When the last tick was, if there was one: where a new key's demand
    /// starts, so that the next tick measures it over the interval it
    /// folds for every other key.
    last_tick: Option<Nanos>,
}
impl Limiter {
    /// A limiter with no key yet.
    ///
    /// # Panics
    ///
    /// If the config is out of range; the messages say what.
    pub fn new(config: Config) -> Self {
        config.validate();
        Self {
            hot_set: HotSet::new(config.cooldown, config.hot_set_size),
            keys: HashMap::new(),
            shadows: HashMap::new(),
            config,
            cap: None,
            last_tick: None,
        }
    }

    /// Holds every key, hot or cold, at no more than `cap` times its own
    /// R, or lifts the cap: the floor `R × β / k` of a node waiting, learning
    /// or without a quorum is the cap `β / k`. New keys follow at once,
    /// known ones from the next tick. For a node that has lost its quorum,
    /// see [`PartitionPolicy::Quorum`](crate::partition::PartitionPolicy).
    pub fn set_cap(&mut self, cap: Option<f64>) {
        self.cap = cap;
    }

    /// Admits or denies one request for `key` at the config's limit. The
    /// attempt counts toward the key's demand either way, cold keys
    /// included.
    pub fn check(&mut self, key: u64, now: Nanos, cluster_size: usize) -> Decision {
        self.check_in(key, self.config.limit, now, cluster_size)
    }

    /// Like [`check`](Limiter::check), for a key with a limit of its own.
    /// A key whose limit changes keeps its state; the new limit applies
    /// from the next tick.
    ///
    /// # Panics
    ///
    /// If the limit is zero, or its burst zero or over
    /// [`MAX_BURST`](crate::gcra::MAX_BURST).
    pub fn check_in(
        &mut self,
        key: u64,
        limit: Limit,
        now: Nanos,
        cluster_size: usize,
    ) -> Decision {
        let state = self.entry(key, limit, now, cluster_size);
        state.demand.record_attempt();
        state.requests.record_attempt();
        let decision = state.gcra.check(now);
        if decision != Decision::Allow {
            state.denials.record_attempt();
        }
        decision
    }

    /// Like [`check_in`](Limiter::check_in), with the decision `shadow`, a
    /// limit only tried out, would have taken: for a caller that enforces
    /// here, as the simulator does. The decision is `check_in`'s, whatever
    /// the shadow says.
    ///
    /// The shadow's quota is the key's quota scaled by the ratio of the two
    /// limits, `quota × shadow.rate / limit.rate`, with the shadow's burst,
    /// as the rateguard runtime sizes its own; see the runtime's
    /// `Guard::set_shadow` for why that errs on the strict side. A key given
    /// a shadow after none starts it fresh; `None` drops it.
    ///
    /// # Panics
    ///
    /// As [`check_in`](Limiter::check_in) does, for either limit.
    pub fn check_in_traced(
        &mut self,
        key: u64,
        limit: Limit,
        shadow: Option<Limit>,
        now: Nanos,
        cluster_size: usize,
    ) -> (Decision, Option<Decision>) {
        let decision = self.check_in(key, limit, now, cluster_size);
        let Some(shadow) = shadow else {
            self.shadows.remove(&key);
            return (decision, None);
        };
        shadow.validate();
        let state = self.keys.get_mut(&key).expect("just checked");
        let wanted = quota_for(
            state.applied.rate_per_sec() * shadow.rate / limit.rate,
            shadow.burst,
        );
        let gcra = self.shadows.entry(key).or_insert_with(|| Gcra::new(wanted));
        if gcra.quota() != wanted {
            gcra.set_quota(wanted, now);
        }
        let traced = gcra.check(now);
        if traced != Decision::Allow {
            state.shadow_denials.record_attempt();
        }
        (decision, Some(traced))
    }

    /// Counts what a runtime that enforces in its own table and only needs
    /// the allocation collected for `key` since it last told: attempts,
    /// denials, and shadow denials. `limit` is the key's, as the runtime
    /// last knew it.
    ///
    /// # Panics
    ///
    /// As [`check_in`](Limiter::check_in) does.
    pub fn record(
        &mut self,
        key: u64,
        tally: Tally,
        limit: Limit,
        now: Nanos,
        cluster_size: usize,
    ) {
        let state = self.entry(key, limit, now, cluster_size);
        state.demand.record_attempts(tally.attempts);
        state.requests.record_attempts(tally.attempts);
        state.denials.record_attempts(tally.denials);
        state.shadow_denials.record_attempts(tally.shadow_denials);
    }

    /// The key's requests, denials and shadow denials a second, averaged
    /// over the last ten seconds or so, if it is tracked. A key seen only
    /// lately shows its rates so far, not a fraction of them.
    pub fn key_rates(&self, key: u64) -> Option<KeyRates> {
        self.keys.get(&key).map(KeyState::rates)
    }

    /// The quota `key` is held to, if it is tracked.
    pub fn quota(&self, key: u64) -> Option<Quota> {
        self.keys.get(&key).map(|state| state.applied)
    }

    /// The limit `key` was last given, if it is tracked.
    pub fn limit(&self, key: u64) -> Option<Limit> {
        self.keys.get(&key).map(|state| state.limit)
    }

    /// The quota a key not seen before starts with in `limit`: the cold
    /// share, under the cap if one is set.
    pub fn new_key_quota(&self, limit: Limit, cluster_size: usize) -> Quota {
        self.cold_quota(limit, cluster_size)
    }

    /// The rate a key not seen before starts with, as a part of its R: for
    /// a runtime that sizes the quota of a new key on the request path.
    pub fn new_key_factor(&self, cluster_size: usize) -> f64 {
        (self.config.alpha / cluster_size as f64).min(self.cap.unwrap_or(f64::INFINITY))
    }

    fn entry(&mut self, key: u64, limit: Limit, now: Nanos, cluster_size: usize) -> &mut KeyState {
        limit.validate();
        let cold = self.cold_quota(limit, cluster_size);
        let time_constant = self.config.demand_time_constant;
        // From the previous tick, not from now: the next tick then folds
        // the key's attempts over the same interval as every other key's,
        // so `unbiased_rate` compares like with like. From now, a key first
        // seen at the moment of a tick, as the runtime records them, would
        // have no interval at that tick, rank last under the cap, and then
        // have its attempts folded with the next period's over one period.
        // Before the first tick ever there is no earlier one: from now.
        let since = self.last_tick.unwrap_or(now);
        let state = self.keys.entry(key).or_insert_with(|| KeyState {
            limit,
            demand: Demand::starting_at(time_constant, since),
            requests: Demand::starting_at(DENIAL_TIME_CONSTANT, since),
            denials: Demand::starting_at(DENIAL_TIME_CONSTANT, since),
            shadow_denials: Demand::starting_at(DENIAL_TIME_CONSTANT, since),
            gcra: Gcra::new(cold),
            applied: cold,
        });
        state.limit = limit;
        state
    }

    /// Updates demand, moves keys between cold and hot, applies their new
    /// quotas and enforces `max_tracked_keys`. Once per protocol period.
    /// A hot key gets an even split, `R/N`: for a share that follows
    /// demand, see [`tick_with_shares`](Limiter::tick_with_shares).
    pub fn tick(&mut self, now: Nanos, cluster_size: usize) {
        let n = cluster_size as f64;
        self.tick_with_shares(now, cluster_size, |_| false, |_, _, limit| limit / n);
    }

    /// Like [`tick`](Limiter::tick), but a hot key gets the rate
    /// `share(key, own_demand, limit)` returns, in requests per second, for
    /// a key whose R is `limit`; and a key for which `hot_elsewhere` is
    /// true is hot here too while it has any demand at all (see
    /// [`hot_set`](crate::hot_set)).
    pub fn tick_with_shares(
        &mut self,
        now: Nanos,
        cluster_size: usize,
        hot_elsewhere: impl Fn(u64) -> bool,
        mut share: impl FnMut(u64, f64, f64) -> f64,
    ) {
        assert!(cluster_size > 0, "cluster_size must be > 0");
        let (n, alpha, beta) = (
            cluster_size as f64,
            self.config.alpha,
            self.config.floor_factor,
        );
        let cap = self.cap.unwrap_or(f64::INFINITY);
        let (keys, hot_set) = (&mut self.keys, &mut self.hot_set);

        // Two passes: a key promoted late in the first may evict one seen
        // earlier, so quotas wait until the hot set is settled.
        let elsewhere =
            |key: u64, state: &KeyState| state.demand.rate() >= SILENT_DEMAND && hot_elsewhere(key);
        keys.retain(|&key, state| {
            state.demand.tick(now);
            state.requests.tick(now);
            state.denials.tick(now);
            state.shadow_denials.tick(now);
            let rate = state.demand.rate();
            let threshold = state.limit.rate / n * alpha;
            let is_hot = hot_set.update_with(key, rate, threshold, now, elsewhere(key, state));
            // Attempts not folded in yet are demand too: a key first seen
            // at the moment of the tick before the first one, or of a
            // second tick at the same moment, has had no interval to
            // measure them over, and would go with them.
            is_hot
                || state.gcra.has_debt(now)
                || rate >= SILENT_DEMAND
                || state.demand.has_pending()
        });
        for (&key, state) in keys.iter_mut() {
            let (limit, burst) = (state.limit.rate, state.limit.burst);
            let wanted = if hot_set.is_hot(key) {
                quota_for(
                    share(key, state.demand.rate(), limit).min(limit * cap),
                    burst,
                )
            } else if elsewhere(key, state) {
                // A key a peer holds hot, with no room for it here: that
                // peer takes all of the limit but the floors, so the floor
                // is all there is.
                let floor = limit * beta / n;
                quota_for(floor.min(limit / n * alpha).min(limit * cap), burst)
            } else {
                quota_for((limit / n * alpha).min(limit * cap), burst)
            };
            if state.applied != wanted {
                state.gcra.set_quota(wanted, now);
                state.applied = wanted;
            }
        }

        self.last_tick = Some(now);
        self.enforce_cap();
        if !self.shadows.is_empty() {
            let keys = &self.keys;
            self.shadows.retain(|key, _| keys.contains_key(key));
        }
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

    /// Up to `k` keys with the most denials, in denials a second over the
    /// last ten seconds or so, most denied first, with their rates. A key
    /// denied nothing lately is not listed.
    ///
    /// Ranked by the plain average, which still counts the time before a
    /// new key was seen as silent: a key denied once just now ranks below
    /// one denied as often for ten seconds, where its rate so far, one
    /// denial over one tick, would put it first. The rates listed are
    /// those of [`key_rates`](Limiter::key_rates), a new key's so far.
    pub fn most_denied(&self, k: usize) -> Vec<(u64, KeyRates)> {
        self.most_by(k, |state| state.denials.rate())
    }

    /// Like [`most_denied`](Limiter::most_denied), by the denials the
    /// keys' shadow limits would have made.
    pub fn most_shadow_denied(&self, k: usize) -> Vec<(u64, KeyRates)> {
        self.most_by(k, |state| state.shadow_denials.rate())
    }

    // The first `k` keys by `by`, those silent in it left out, with their
    // rates.
    fn most_by(&self, k: usize, by: impl Fn(&KeyState) -> f64) -> Vec<(u64, KeyRates)> {
        if k == 0 {
            return Vec::new();
        }
        let mut found: Vec<(u64, f64, &KeyState)> = self
            .keys
            .iter()
            .map(|(&key, state)| (key, by(state), state))
            .filter(|&(_, rank, _)| rank >= SILENT_DEMAND)
            .collect();
        // Once a tick over every tracked key: the first k found in linear
        // time, and only they sorted.
        let order = |a: &(u64, f64, &KeyState), b: &(u64, f64, &KeyState)| {
            b.1.total_cmp(&a.1).then(a.0.cmp(&b.0))
        };
        if k < found.len() {
            found.select_nth_unstable_by(k - 1, order);
            found.truncate(k);
        }
        found.sort_by(order);
        found
            .into_iter()
            .map(|(key, _, state)| (key, state.rates()))
            .collect()
    }

    /// The config it was made with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Since when the key has been hot without a break, if it is.
    pub fn hot_since(&self, key: u64) -> Option<Nanos> {
        self.hot_set.hot_since(key)
    }

    /// Whether the key is hot here.
    pub fn is_hot(&self, key: u64) -> bool {
        self.hot_set.is_hot(key)
    }

    /// Whether the key has state here, hot or cold.
    pub fn tracked(&self, key: u64) -> bool {
        self.keys.contains_key(&key)
    }

    /// How many keys have state here.
    pub fn tracked_keys(&self) -> usize {
        self.keys.len()
    }

    /// How many keys are hot here.
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
            // Unbiased: a key seen only lately would otherwise show a
            // fraction of its demand and go first, every round.
            .map(|(&key, state)| (key, state.demand.unbiased_rate()))
            .collect();

        victims.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));

        for (key, _) in victims.into_iter().take(excess) {
            self.keys.remove(&key);
        }
    }

    fn cold_quota(&self, limit: Limit, cluster_size: usize) -> Quota {
        assert!(cluster_size > 0, "cluster_size must be > 0");
        quota_for(limit.rate * self.new_key_factor(cluster_size), limit.burst)
    }
}

fn quota_for(rate_per_sec: f64, burst: u32) -> Quota {
    Quota::per_second(rate_per_sec, burst)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_SEC: Nanos = 1_000_000_000;
    const N: usize = 5;
    const KEY: u64 = 42;

    fn tally(attempts: u64, denials: u64) -> Tally {
        Tally {
            attempts,
            denials,
            shadow_denials: 0,
        }
    }

    #[test]
    fn shadow_denials_recorded_are_averaged_apart() {
        let mut l = Limiter::new(config());
        let shadowed = Tally {
            attempts: 50,
            denials: 0,
            shadow_denials: 40,
        };
        l.record(KEY, shadowed, config().limit, 0, N);
        l.record(KEY + 1, tally(50, 40), config().limit, 0, N);
        l.tick(ONE_SEC, N);
        assert!(l.key_rates(KEY).unwrap().shadow_denied_per_sec > 0.0);
        assert_eq!(
            l.key_rates(KEY + 1).unwrap().shadow_denied_per_sec,
            0.0,
            "denials are not shadow denials"
        );
        assert_eq!(l.most_denied(10).len(), 1, "nor the other way round");
        assert_eq!(l.most_shadow_denied(10).len(), 1);
        assert_eq!(l.most_shadow_denied(10)[0].0, KEY);
        assert_eq!(l.key_rates(KEY + 2), None, "not tracked");
    }

    #[test]
    fn the_rates_of_a_key_share_one_window() {
        let mut l = Limiter::new(config());
        l.tick(0, N);
        for second in 1..=30 {
            l.record(
                KEY,
                tally(100, 50),
                config().limit,
                (second - 1) * ONE_SEC,
                N,
            );
            l.tick(second * ONE_SEC, N);
        }
        let rates = l.key_rates(KEY).unwrap();
        assert!(
            (rates.requests_per_sec / 100.0 - 1.0).abs() < 0.05,
            "{rates:?}"
        );
        assert!(
            (rates.denied_per_sec / 50.0 - 1.0).abs() < 0.05,
            "{rates:?}"
        );
        // A pause of a second with nothing: both fall at the same pace.
        l.tick(31 * ONE_SEC, N);
        let paused = l.key_rates(KEY).unwrap();
        assert!(paused.requests_per_sec < rates.requests_per_sec);
        let ratio = paused.denied_per_sec / paused.requests_per_sec;
        assert!((ratio / 0.5 - 1.0).abs() < 0.05, "{ratio} {paused:?}");
    }

    fn config() -> Config {
        Config {
            limit: Limit {
                rate: 1000.0,
                burst: 10,
            },
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
    #[should_panic(expected = "burst must be <=")]
    fn a_burst_a_quota_cannot_hold_is_refused() {
        Limiter::new(Config {
            limit: Limit {
                burst: crate::gcra::MAX_BURST + 1,
                ..config().limit
            },
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

    const PRO: Limit = Limit {
        rate: 200.0,
        burst: 4,
    };

    fn quota_rate(l: &Limiter, key: u64) -> f64 {
        l.quota(key).expect("tracked").rate_per_sec()
    }

    #[test]
    fn a_key_with_a_limit_of_its_own_gets_the_cold_share_of_it() {
        let mut l = limiter();
        l.check_in(KEY, PRO, 0, N);
        l.tick(0, N);
        // α × 200 / 5, not α × 1000 / 5.
        assert_eq!(quota_rate(&l, KEY), 20.0);
        assert_eq!(l.quota(KEY).unwrap().burst(), 4);
        assert_eq!(l.limit(KEY), Some(PRO));
    }

    #[test]
    fn a_new_key_starts_at_the_cold_share_of_its_limit() {
        let l = limiter();
        assert_eq!(l.new_key_quota(PRO, N).rate_per_sec(), 20.0);
        assert_eq!(l.new_key_quota(config().limit, N).rate_per_sec(), 100.0);
    }

    // A limit of 1000 an hour is a rate that is not a whole number a second.
    #[test]
    fn a_fractional_limit_is_shared_by_its_rate() {
        let l = limiter();
        let limit = Limit {
            rate: 1000.0 / 60.0,
            burst: 1,
        };
        let quota = l.new_key_quota(limit, 4);
        let expected = config().alpha * limit.rate / 4.0;
        assert!(
            (quota.rate_per_sec() - expected).abs() < 1e-9,
            "{} against {expected}",
            quota.rate_per_sec()
        );
    }

    #[test]
    #[should_panic(expected = "finite and > 0")]
    fn a_limit_that_is_not_finite_is_refused() {
        Limiter::new(Config {
            limit: Limit {
                rate: f64::NAN,
                ..config().limit
            },
            ..config()
        });
    }

    #[test]
    fn a_key_turns_hot_at_the_threshold_of_its_own_limit() {
        let mut l = limiter();
        l.tick(0, N);
        // 60 in a second, an average of 38 after one tick: under the
        // default threshold of 100, over PRO's 20.
        for _ in 0..60 {
            l.record(KEY, tally(1, 0), PRO, 0, N);
            l.record(KEY + 1, tally(1, 0), config().limit, 0, N);
        }
        l.tick(ONE_SEC, N);
        assert!(l.is_hot(KEY), "hot by its own limit");
        assert!(!l.is_hot(KEY + 1), "cold by the default one");
    }

    #[test]
    fn a_hot_key_is_shared_out_of_its_own_limit() {
        let mut l = limiter();
        l.tick(0, N);
        for _ in 0..100 {
            l.record(KEY, tally(1, 0), PRO, 0, N);
        }
        let mut limits = Vec::new();
        l.tick_with_shares(
            ONE_SEC,
            N,
            |_| false,
            |_, _, limit| {
                limits.push(limit);
                limit / N as f64
            },
        );
        assert_eq!(limits, vec![200.0]);
        assert_eq!(quota_rate(&l, KEY), 40.0);
    }

    #[test]
    fn the_cap_is_a_part_of_each_key_s_own_limit() {
        let mut l = limiter();
        l.set_cap(Some(0.02));
        l.check_in(KEY, PRO, 0, N);
        l.check(KEY + 1, 0, N);
        l.tick(0, N);
        assert_eq!(quota_rate(&l, KEY), 4.0, "200 × 0.02");
        assert_eq!(quota_rate(&l, KEY + 1), 20.0, "1000 × 0.02");
    }

    #[test]
    fn a_new_limit_for_a_known_key_applies_at_the_next_tick() {
        let mut l = limiter();
        l.check(KEY, 0, N);
        l.tick(0, N);
        assert_eq!(quota_rate(&l, KEY), 100.0);
        l.record(KEY, tally(1, 0), PRO, 1, N);
        l.tick(ONE_SEC, N);
        assert_eq!(quota_rate(&l, KEY), 20.0);
    }

    // A cap of 0, a `floor_factor` of 0 on a node without its quorum,
    // holds a key at the slowest rate there is; with a burst of 18 or more,
    // `interval × (burst − 1)` no longer fits in 64 bits. The key must
    // still get no more than its burst, not every request.
    #[test]
    fn a_cap_of_zero_admits_no_more_than_the_burst() {
        for burst in [18, 1000] {
            let mut l = Limiter::new(Config {
                floor_factor: 0.0,
                limit: Limit {
                    rate: 1000.0,
                    burst,
                },
                ..config()
            });
            l.set_cap(Some(0.0));
            let allowed = (0..2 * burst)
                .filter(|_| l.check(KEY, 0, N) == Decision::Allow)
                .count();
            assert!(allowed <= burst as usize, "{allowed} of burst {burst}");
        }
    }

    // The floor of one an hour on 40 nodes, `R × β / k` with β = 0.05,
    // and a burst of 10,000: an interval of 33 days.
    #[test]
    fn a_slow_floor_admits_no_more_than_the_burst() {
        let mut l = Limiter::new(Config {
            floor_factor: 0.05,
            limit: Limit {
                rate: 1.0 / 3600.0,
                burst: 10_000,
            },
            ..config()
        });
        l.set_cap(Some(0.05 / 40.0));
        let allowed = (0..20_000)
            .filter(|_| l.check(KEY, 0, 40) == Decision::Allow)
            .count();
        assert!(allowed <= 10_000, "{allowed}");
    }

    #[test]
    fn the_most_denied_keys_come_busiest_first() {
        let mut l = limiter();
        l.tick(0, N);
        l.record(KEY, tally(50, 40), config().limit, 0, N);
        l.record(KEY + 1, tally(50, 10), config().limit, 0, N);
        l.record(KEY + 2, tally(50, 0), config().limit, 0, N);
        l.tick(ONE_SEC, N);
        let denied: Vec<u64> = l.most_denied(10).into_iter().map(|(key, _)| key).collect();
        assert_eq!(
            denied,
            vec![KEY, KEY + 1],
            "a key never denied is not listed"
        );
        assert_eq!(l.most_denied(1).len(), 1);
        assert_eq!(l.most_denied(1)[0].0, KEY, "the busiest, not any one");
        assert!(l.most_denied(0).is_empty());
    }

    // Under key churn, a key denied once just now must not outrank one
    // denied steadily: its unbiased rate is its rate over one tick, which a
    // single denial makes high. The ranking takes the biased rate, which
    // weighs how long a key has been denied; the rates listed stay
    // unbiased, so their ratio holds.
    #[test]
    fn a_key_denied_once_does_not_outrank_one_denied_steadily() {
        const ONE_SHOT: u64 = KEY + 1;
        let mut l = limiter();
        let tick = ONE_SEC / 5;
        l.tick(0, N);
        for i in 1..=50u64 {
            l.record(
                KEY,
                tally(10, u64::from(i % 3 == 0)),
                config().limit,
                i * tick,
                N,
            );
            l.tick(i * tick, N);
        }
        l.record(ONE_SHOT, tally(1, 1), config().limit, 50 * tick, N);
        l.tick(51 * tick, N);
        let denied = l.most_denied(10);
        let keys: Vec<u64> = denied.iter().map(|(key, _)| *key).collect();
        assert_eq!(keys, vec![KEY, ONE_SHOT], "{denied:?}");
        assert_eq!(l.most_denied(1)[0].0, KEY);
        let one_shot = denied[1].1;
        assert!(
            one_shot.denied_per_sec > denied[0].1.denied_per_sec,
            "listed unbiased: {denied:?}"
        );
        assert_eq!(one_shot.denied_per_sec, one_shot.requests_per_sec);
    }

    #[test]
    fn a_denial_in_check_counts_toward_the_denied_keys() {
        let mut l = limiter();
        l.tick(0, N);
        for _ in 0..20 {
            l.check(KEY, 0, N); // burst 10, then denials
        }
        l.tick(ONE_SEC, N);
        let denied = l.most_denied(10);
        assert_eq!(denied.len(), 1);
        assert!(denied[0].1.denied_per_sec > 0.0, "{denied:?}");
    }

    // A runtime hands over the attempts of a new key in the very tick of a
    // round: the demand has had no time to measure them yet, and the key
    // must wait for the next tick, not be dropped with them.
    #[test]
    fn attempts_counted_at_the_moment_of_a_tick_are_kept_for_the_next() {
        let mut l = limiter();
        l.record(KEY, tally(50, 0), config().limit, ONE_SEC, N);
        l.tick(ONE_SEC, N);
        assert!(l.tracked(KEY), "dropped with its attempts");
        l.tick(ONE_SEC + ONE_SEC / 5, N);
        assert!(l.hot_demand().is_empty() && l.tracked(KEY));
        let demand = l.keys[&KEY].demand.rate();
        assert!(demand > 10.0, "the attempts count: {demand}");
    }

    #[test]
    fn cold_key_is_held_to_its_alpha_share() {
        let mut l = limiter();
        for _ in 0..config().limit.burst {
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
        l.tick_with_shares(ONE_SEC, N, |key| key == KEY, |_, _, _| 250.0);
        assert!(l.is_hot(KEY));
        assert_eq!(l.hot_demand().len(), 1);
        let (_, _, primary) = l.hot_demand()[0];
        assert!(!primary, "hot only because a peer holds it so");

        let t = 2 * ONE_SEC;
        for _ in 0..config().limit.burst {
            assert_eq!(l.check(KEY, t, N), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(KEY, t, N) else {
            panic!("burst must be exhausted");
        };
        assert_eq!(retry_at - t, 4_000_000, "the share, not the cold 100");
    }

    // The hot node takes all of the limit but the peers' floors. A key it
    // holds hot that finds no room in a full hot set here must stay within
    // the floor too, not take the cold share on top (spec §4.1).
    #[test]
    fn a_key_hot_elsewhere_with_no_room_here_gets_the_floor() {
        let mut l = limiter();
        l.tick(0, N);
        for strong in 1..=4 {
            for _ in 0..400 {
                l.check(KEY + strong, 0, N);
            }
        }
        l.tick(ONE_SEC, N);
        assert_eq!(l.hot_keys(), 4, "the set is full");
        for _ in 0..50 {
            l.check(KEY, ONE_SEC, N);
        }
        l.tick_with_shares(2 * ONE_SEC, N, |key| key == KEY, |_, _, _| 250.0);
        assert!(
            !l.is_hot(KEY),
            "no room: KEY does not beat the weakest by 10%"
        );
        assert_eq!(
            interval_at(&mut l, KEY, 3 * ONE_SEC),
            50_000_000,
            "R × β / N = 20 a second, not the cold 100"
        );
    }

    // Eviction happens mid-tick: a key that got its hot share earlier in
    // the same pass and was evicted later must not keep it, the peers no
    // longer counting its demand.
    #[test]
    fn an_evicted_key_drops_its_hot_share_in_the_same_tick() {
        let mut l = limiter();
        l.tick(0, N);
        for old in 1..=4 {
            for _ in 0..400 {
                l.check(old, 0, N);
            }
        }
        l.tick(ONE_SEC, N);
        assert_eq!(l.hot_keys(), 4);
        for new in 11..=14 {
            for _ in 0..4000 {
                l.check(new, ONE_SEC, N);
            }
        }
        l.tick_with_shares(2 * ONE_SEC, N, |_| false, |_, _, _| 900.0);

        for key in 1..=4 {
            assert!(!l.is_hot(key), "key {key} evicted");
            assert_eq!(
                interval_at(&mut l, key, 3 * ONE_SEC),
                10_000_000,
                "key {key}: the cold 100 a second, not the hot 900"
            );
        }
    }

    #[test]
    fn a_silent_key_is_not_kept_hot_by_peers() {
        let mut l = limiter();
        l.tick(0, N);
        l.check(KEY, 0, N);
        l.tick_with_shares(ONE_SEC, N, |_| true, |_, _, _| 250.0);
        assert!(l.is_hot(KEY), "a request a second ago is demand");
        l.tick_with_shares(60 * ONE_SEC, N, |_| true, |_, _, _| 250.0);
        assert!(!l.is_hot(KEY));
        assert_eq!(l.tracked_keys(), 0, "reclaimed as before");
    }

    fn interval_at(l: &mut Limiter, key: u64, t: Nanos) -> Nanos {
        for _ in 0..config().limit.burst {
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

        l.set_cap(Some(0.02)); // 20 of R = 1000
        l.tick_with_shares(2 * ONE_SEC, N, |_| false, |_, _, _| 250.0);
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
        l.tick_with_shares(4 * ONE_SEC, N, |_| false, |_, _, _| 250.0);
        assert_eq!(interval_at(&mut l, KEY, 5 * ONE_SEC), 4_000_000, "lifted");
    }

    #[test]
    fn recorded_attempts_heat_a_key_like_checks_do() {
        let (mut by_check, mut by_record) = (limiter(), limiter());
        by_check.tick(0, N);
        by_record.tick(0, N);
        for _ in 0..500 {
            by_check.check(KEY, 0, N);
        }
        by_record.record(KEY, tally(500, 0), config().limit, 0, N);
        by_check.tick(ONE_SEC, N);
        by_record.tick(ONE_SEC, N);
        assert!(by_record.is_hot(KEY));
        assert_eq!(by_record.quota(KEY), by_check.quota(KEY));
    }

    #[test]
    fn a_new_key_starts_at_the_capped_cold_quota() {
        let mut l = limiter();
        assert_eq!(
            l.new_key_quota(config().limit, N),
            Quota::new(100, config().limit.burst),
            "α × R / N"
        );
        l.set_cap(Some(0.02)); // 20 of R = 1000
        assert_eq!(
            l.new_key_quota(config().limit, N),
            Quota::new(20, config().limit.burst)
        );
        assert_eq!(l.quota(KEY), None, "not tracked");
    }

    // Ten a second over twenty nodes: a cold share of 0.25 a second. Rounded
    // up to 1, as it once was, the fleet admitted twice its limit.
    #[test]
    fn a_share_under_one_per_second_is_kept_not_rounded_up() {
        let l = Limiter::new(Config {
            limit: Limit {
                rate: 10.0,
                burst: 1,
            },
            ..config()
        });
        assert_eq!(
            l.new_key_quota(l.config().limit, 20),
            Quota::per_second(0.25, 1)
        );
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
            |key, own, _| {
                asked.push((key, own));
                250.0
            },
        );
        assert_eq!(asked.len(), 1, "asked for hot keys only");
        assert_eq!(asked[0].0, KEY);
        assert!(asked[0].1 > 100.0, "with its own demand: {}", asked[0].1);

        let t = 3 * ONE_SEC;
        for _ in 0..config().limit.burst {
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
        for _ in 0..config().limit.burst {
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

    // A key's average starts from zero: one round in, it shows a tenth of
    // its demand. Ranked on that, a new key busier than every tracked one
    // would be the first dropped, round after round, and never tracked.
    #[test]
    fn a_new_key_busier_than_the_tracked_ones_outranks_them() {
        let mut l = limiter();
        let round = ONE_SEC / 10;
        let mut now = 0;
        l.tick(now, N);
        for _ in 0..50 {
            for key in 0..8u64 {
                l.record(key, tally(2, 0), config().limit, now, N);
            }
            now += round;
            l.tick(now, N);
        }
        assert_eq!(l.tracked_keys(), 8);

        // Twice as busy as each of them, cold all the same.
        for key in 0..8u64 {
            l.record(key, tally(2, 0), config().limit, now, N);
        }
        l.record(99, tally(4, 0), config().limit, now, N);
        now += round;
        l.tick(now, N);
        assert!(!l.is_hot(99));
        assert!(l.tracked(99));
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
        for _ in 0..config().limit.burst {
            assert_eq!(l.check(KEY, t, 2), Decision::Allow);
        }
        let Decision::Deny { retry_at } = l.check(KEY, t, 2) else {
            panic!("burst must be exhausted");
        };
        assert_eq!(retry_at - t, 4_000_000);
    }

    // The rateguard runtime's sync: every slot's tally recorded at `now`,
    // then the tick at the same `now`.
    #[test]
    fn runtime_new_heavy_key_is_tracked_under_key_pressure() {
        let mut l = limiter();
        let limit = config().limit;
        let heavy = 999u64;
        for second in 0..=7u64 {
            let now = second * ONE_SEC;
            // Eight steady keys at 1 a second fill the cap.
            for key in 0..8u64 {
                l.record(key, tally(1, 0), limit, now, N);
            }
            if second >= 5 {
                // Far over the hot threshold of 100 a second.
                l.record(heavy, tally(5000, 0), limit, now, N);
            }
            l.tick(now, N);
        }
        assert!(
            l.tracked(heavy),
            "the heavy key is evicted before it is measured"
        );
        assert!(l.is_hot(heavy), "the heavy key never turns hot");
    }

    #[test]
    fn a_new_key_shows_its_rate_from_its_first_tick() {
        let mut l = limiter();
        let limit = config().limit;
        l.tick(0, N);
        for second in 1..=3u64 {
            let now = second * ONE_SEC;
            l.record(KEY, tally(50, 0), limit, now, N);
            l.tick(now, N);
            let rate = l.key_rates(KEY).unwrap().requests_per_sec;
            assert!(
                (rate / 50.0 - 1.0).abs() < 0.05,
                "tick {second}: {rate}, not 50"
            );
        }
    }

    #[test]
    fn one_off_keys_seen_just_before_the_tick_do_not_evict_steady_keys() {
        let mut l = limiter();
        let mut one_off = 1000u64;
        for second in 0..20u64 {
            for tenth in 0..10u64 {
                for key in 0..8u64 {
                    l.check(key, second * ONE_SEC + tenth * ONE_SEC / 10, N);
                }
            }
            if second >= 10 {
                for _ in 0..8 {
                    l.check(one_off, (second + 1) * ONE_SEC - 1_000_000, N);
                    one_off += 1;
                }
            }
            l.tick((second + 1) * ONE_SEC, N);
        }
        let steady = (0..8u64).filter(|&key| l.tracked(key)).count();
        assert_eq!(steady, 8, "steady 10/s keys evicted for one-request keys");
    }

    #[test]
    fn the_first_tick_ever_still_measures_from_now() {
        let mut l = limiter();
        // Never ticked: there is no earlier tick to measure from, so the
        // key starts at the moment it is first seen.
        l.record(KEY, tally(50, 0), config().limit, 5 * ONE_SEC, N);
        l.tick(6 * ONE_SEC, N);
        // 50 over the second since 5 s; measured from 0, a sixth of that.
        let rate = l.key_rates(KEY).unwrap().requests_per_sec;
        assert!((rate / 50.0 - 1.0).abs() < 0.05, "{rate}");
    }
}
