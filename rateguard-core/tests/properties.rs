use proptest::prelude::*;
use rateguard_core::demand::Demand;
use rateguard_core::gcra::{Decision, Gcra, Nanos, Quota};
use rateguard_core::hot_set::HotSet;
use rateguard_core::limiter::{Config, Limiter};

const ONE_SEC: Nanos = 1_000_000_000;
const THRESHOLD: f64 = 100.0;

fn interval(rate_per_sec: u32) -> Nanos {
    ONE_SEC.div_ceil(rate_per_sec as u64)
}

fn quota() -> impl Strategy<Value = (u32, u32)> {
    (1u32..10_000, 1u32..64)
}

fn timeline(max_len: usize) -> impl Strategy<Value = Vec<Nanos>> {
    prop::collection::vec(0u64..2 * ONE_SEC, 1..=max_len).prop_map(|gaps| {
        gaps.iter()
            .scan(0u64, |now, gap| {
                *now += gap;
                Some(*now)
            })
            .collect()
    })
}

proptest! {
    #[test]
    fn admissions_never_outrun_the_quota((rate, burst) in quota(), times in timeline(200)) {
        let mut g = Gcra::new(Quota::new(rate, burst));
        let admitted = times
            .iter()
            .filter(|&&now| g.check(now) == Decision::Allow)
            .count() as u64;

        let window = times.last().unwrap() - times.first().unwrap();
        let budget = window / interval(rate) + burst as u64;

        prop_assert!(admitted <= budget, "admitted {admitted} in {window} ns at {rate}/s burst {burst}, budget {budget}");
    }

    #[test]
    fn an_idle_key_is_handed_exactly_its_burst((rate, burst) in quota(), now in 0u64..(u64::MAX / 4)) {
        let mut g = Gcra::new(Quota::new(rate, burst));

        for slot in 0..burst {
            prop_assert_eq!(g.check(now), Decision::Allow, "slot {} of {}", slot, burst);
        }

        prop_assert!(matches!(g.check(now), Decision::Deny { .. }), "burst {} must be the ceiling", burst);
    }

    #[test]
    fn a_denial_never_names_a_time_that_does_not_work((rate, burst) in quota(), times in timeline(200)) {
        let mut g = Gcra::new(Quota::new(rate, burst));

        for now in times {
            if let Decision::Deny { retry_at } = g.check(now) {
                prop_assert!(retry_at > now);
                let mut probe = g;
                prop_assert_eq!(probe.check(retry_at), Decision::Allow);
            }
        }
    }

    #[test]
    fn changing_the_quota_never_refills_the_bucket((rate, burst) in quota(), changes in prop::collection::vec((1u32..10_000, 1u32..64, 0u64..2 * ONE_SEC), 1..32)) {
        let mut g = Gcra::new(Quota::new(rate, burst));
        let mut now = 0;
        for _ in 0..burst {
            g.check(now);
        }

        for (new_rate, new_burst, gap) in changes {
            now += gap;
            g.set_quota(Quota::new(new_rate, new_burst), now);

            let mut probe = g;
            let mut admitted = 0u32;
            for _ in 0..=new_burst {
                if probe.check(now) == Decision::Allow {
                    admitted += 1;
                } else {
                    break;
                }
            }
            prop_assert!(admitted <= new_burst, "{} slots opened at once, burst is {}", admitted, new_burst);
        }
    }

    #[test]
    fn the_hot_set_obeys_its_ceiling(max_size in 1usize..16, ops in prop::collection::vec((0u64..64, 0.0f64..2_000.0, 0u64..2 * ONE_SEC), 1..300)) {
        let mut hs = HotSet::new(5 * ONE_SEC, max_size);
        let mut now = 0;

        for (key, demand, gap) in ops {
            now += gap;
            let was_hot = hs.is_hot(key);
            let is_hot = hs.update(key, demand, THRESHOLD, now);

            prop_assert_eq!(is_hot, hs.is_hot(key), "the verdict must much the set");
            prop_assert!(hs.len() <= max_size, "hot set grew past {}", max_size);
            if demand <= THRESHOLD {
                prop_assert!(!is_hot || was_hot, "a key nelow the threshols may stay not by inertia, never enter");
            }
        }
    }

    #[test]
    fn memory_is_bounded_by_config_not_by_key_cardinality(hot_set_size in 1usize..8, slack in 0usize..8, cluster_size in 1usize..64, ops in prop::collection::vec((0u64..5_000, 0u64..(ONE_SEC / 4)), 1..400)) {
        let max_tracked_keys = hot_set_size + slack;
        let mut l = Limiter::new(Config {
            limit_per_sec: 1_000,
            burst: 10,
            alpha: 0.5,
            cooldown: 5 * ONE_SEC,
            hot_set_size,
            max_tracked_keys,
            demand_time_constant: ONE_SEC,
        });

        let mut now = 0;
        let mut seen = Vec::new();
        for (key, gap) in ops {
            now += gap;
            l.check(key, now, cluster_size);
            seen.push(key);
            if seen.len() % 16 == 0 {
                l.tick(now, cluster_size);
            }
        }
        l.tick(now + 1, cluster_size);

        prop_assert!(l.tracked_keys() <= max_tracked_keys, "tracking {} keys, cap is {}", l.tracked_keys(), max_tracked_keys);
        prop_assert!(l.hot_keys() <= hot_set_size);
        for key in seen {
            prop_assert!(!l.is_hot(key) || l.tracked(key), "a hot key must keep its per-key state");
        }
    }

    #[test]
    fn the_demand_estimate_stays_between_zero_and_the_peak(ops in prop::collection::vec((0u32..2_000, 1u64..(2 * ONE_SEC)), 1..200)) {
        let mut d = Demand::starting_at(ONE_SEC, 0);
        let mut now = 0;
        let mut peak = 0.0f64;

        for (attempts, gap) in ops {
            for _ in 0..attempts {
                d.record_attempt();
            }
            now += gap;
            peak = peak.max(attempts as f64 / (gap as f64 / ONE_SEC as f64));
            d.tick(now);

            prop_assert!(d.rate().is_finite());
            prop_assert!(d.rate() >= 0.0);
            prop_assert!(d.rate() <= peak * (1.0 + 1e-9), "rate {} above every instantaneous rate seen ({})", d.rate(), peak);
        }
    }

    #[test]
    fn silence_always_decays_the_estimate(attempts in 1u32..5_000, silence in (ONE_SEC / 10)..(10 * ONE_SEC)) {
        let mut d = Demand::starting_at(ONE_SEC, 0);
        for _ in 0..attempts {
            d.record_attempt();
        }
        d.tick(ONE_SEC);
        let busy = d.rate();
        prop_assume!(busy > 0.0);

        d.tick(ONE_SEC + silence);
        prop_assert!(d.rate() < busy, "an idle key must give its share back");
    }
}
