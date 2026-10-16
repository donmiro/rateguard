// Keys with limits of their own (classes in the runtime), and limits that
// change while the cluster runs. On the shipped defaults, R = 1000 for the
// default class, five nodes, skewed demand on every key.
//
// The standard invariant checks every second of every run against the
// widest limit each key has had at any node.

use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::{Config, Limit};
use rateguard_sim::link::{Link, PerfectLink};
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const NODES: usize = 5;
const FREE: u64 = 1;
const PRO: u64 = 2;

fn defaults() -> Config {
    Config {
        limit_per_sec: 1000,
        burst: 50,
        alpha: 0.5,
        floor_factor: 0.05,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 64,
        max_tracked_keys: 4096,
        demand_time_constant: ONE_SEC,
    }
}

fn limit(per_second: u32) -> Limit {
    Limit {
        per_second,
        burst: (per_second / 20).max(1),
    }
}

// Twice what any limit here admits, skewed: one busy node, four quieter.
fn demand(s: &mut Sim<impl Link>, key: u64, total: u32, until: Nanos) {
    s.schedule_request_stream(0, key, total * 6 / 10, 0, until);
    for node in 1..NODES {
        s.schedule_request_stream(node, key, total / 10, 0, until);
    }
}

fn rate(s: &Sim<impl Link>, key: u64, from: Nanos, until: Nanos) -> f64 {
    let admitted = s
        .admissions()
        .iter()
        .filter(|a| a.key == key && (from..until).contains(&a.at))
        .filter(|a| a.decision == Decision::Allow)
        .count();
    admitted as f64 / ((until - from) as f64 / ONE_SEC as f64)
}

// The busiest whole second of a key's admissions in a span.
fn peak(s: &Sim<impl Link>, key: u64, from: Nanos, until: Nanos) -> f64 {
    (from / ONE_SEC..until / ONE_SEC)
        .map(|sec| rate(s, key, sec * ONE_SEC, (sec + 1) * ONE_SEC))
        .fold(0.0, f64::max)
}

// R plus a burst on each node, over a window of `secs`.
fn ceiling(limit: Limit, secs: u64) -> f64 {
    limit.per_second as f64 + (NODES as f64 * limit.burst as f64) / secs as f64
}

fn assert_between(what: &str, rate: f64, low: f64, high: f64) {
    assert!(
        (low..=high).contains(&rate),
        "{what}: {rate:.1} a second, expected {low:.1}..={high:.1}"
    );
}

#[test]
fn two_classes_in_one_cluster_each_hold_their_own_limit() {
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    s.set_key_limit(PRO, limit(200));
    let run = 40 * ONE_SEC;
    demand(&mut s, FREE, 2000, run);
    demand(&mut s, PRO, 400, run);
    s.run_until(run);

    let (from, secs) = (20 * ONE_SEC, 20);
    assert_between(
        "the default class",
        rate(&s, FREE, from, run),
        950.0,
        ceiling(defaults().limit(), secs),
    );
    assert_between(
        "a class of 200",
        rate(&s, PRO, from, run),
        190.0,
        ceiling(limit(200), secs),
    );
}

// A new limit reaches the nodes one by one, a second apart. Raised, the
// cluster never goes over the new one; settled, it admits all of it.
#[test]
fn a_raised_limit_is_never_overshot_and_is_reached() {
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    s.set_key_limit(PRO, limit(200));
    let run = 50 * ONE_SEC;
    demand(&mut s, PRO, 800, run);
    for node in 0..NODES {
        s.schedule_key_limit((20 + node as u64) * ONE_SEC, node, PRO, limit(400));
    }
    s.run_until(run);

    let (raised, settled) = (20 * ONE_SEC, 35 * ONE_SEC);
    assert!(
        peak(&s, PRO, raised, run) <= 400.0 * 1.05,
        "over the new limit: {:.1}",
        peak(&s, PRO, raised, run)
    );
    assert_between(
        "settled",
        rate(&s, PRO, settled, run),
        380.0,
        ceiling(limit(400), 15),
    );
}

// Lowered, the cluster holds no more than the old limit while the change
// spreads, and the new one once it has.
#[test]
fn a_lowered_limit_holds_the_old_one_in_passing_and_the_new_one_after() {
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    s.set_key_limit(PRO, limit(400));
    let run = 50 * ONE_SEC;
    demand(&mut s, PRO, 800, run);
    for node in 0..NODES {
        s.schedule_key_limit((20 + node as u64) * ONE_SEC, node, PRO, limit(200));
    }
    s.run_until(run);

    let (lowered, settled) = (20 * ONE_SEC, 35 * ONE_SEC);
    assert!(
        peak(&s, PRO, lowered, settled) <= 400.0 * 1.05,
        "over the old limit in passing: {:.1}",
        peak(&s, PRO, lowered, settled)
    );
    assert!(
        peak(&s, PRO, settled, run) <= 200.0 * 1.05,
        "over the new limit once settled: {:.1}",
        peak(&s, PRO, settled, run)
    );
    assert_between(
        "settled",
        rate(&s, PRO, settled, run),
        180.0,
        ceiling(limit(200), 15),
    );
}
