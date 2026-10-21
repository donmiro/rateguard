// Keys with limits of their own (classes in the runtime), and limits that
// change while the cluster runs. On the shipped defaults, R = 1000 for the
// default class, five nodes, skewed demand on every key.
//
// The standard invariant checks every second of every run against the
// widest limit each key has had at any node.

use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::{Config, Limit};
use rateguard_core::partition::PartitionPolicy;
use rateguard_sim::link::{Fate, Link, NodeIndex, PerfectLink};
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const NODES: usize = 5;
const FREE: u64 = 1;
const PRO: u64 = 2;

fn defaults() -> Config {
    Config {
        limit: Limit {
            rate: 1000.0,
            burst: 50,
        },
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
        rate: per_second as f64,
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
    limit.rate + (NODES as f64 * limit.burst as f64) / secs as f64
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
        ceiling(defaults().limit, secs),
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

// R of one an hour on a cluster of 40, every node held at its floor for the
// whole run: each is cut off from all the others under the quorum policy,
// so none has a majority and the floor, R × β / N, is all it gets. That is
// far under one request in four hours, so each node admits its burst of 1
// and what trickles in after it, and the whole cluster stays within
// R × 4 h + N × burst. The nodes are held at the floor this way, not left
// to `learning`: that ends after a few seconds of gossip, and the run is
// hours. The quorum memory is stretched past the run so that the hold lasts.
#[test]
fn a_per_hour_limit_holds_at_the_floor_on_40_nodes() {
    struct Islands;
    impl Link for Islands {
        fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
            if from == to {
                Fate::Delivered(now + ONE_MS)
            } else {
                Fate::Lost
            }
        }
    }

    const NODES: usize = 40;
    const HOURS: u64 = 4;
    let config = Config {
        limit: Limit {
            rate: 1.0 / 3600.0,
            burst: 1,
        },
        ..defaults()
    };
    let mut s = Sim::new(NODES, config, Islands);
    s.set_partition_policy(PartitionPolicy::Quorum);
    // The default memory is an hour, after which a node takes the members it
    // cannot reach for gone and gets its share of what it sees: all of R.
    s.set_quorum_memory(100 * 3600 * ONE_SEC);
    let run = HOURS * 3600 * ONE_SEC;
    for node in 0..NODES {
        s.schedule_request_stream(node, FREE, 1, 0, run);
    }
    s.run_until(run);

    let admitted = s.admitted_between(0, run);
    assert!(admitted > 0, "every node's burst, at least");
    assert!(
        admitted <= HOURS as usize + NODES,
        "{admitted} admitted: R × 4 h + 40 × burst is 44"
    );
}

// A shadow at R/5 next to a limit of R = 1000, on ten nodes, against the
// same run with R/5 enforced. The shadow's quota is the enforced one scaled
// by R_shadow / R, so it errs only for a key hot under the shadow's limit
// and cold under the real one: per node, between α × 200 / 10 = 10 and
// α × 1000 / 10 = 50 a second. Such a key gets the cold share of the
// shadow's limit where enforcing it would give its share by demand, so the
// shadow denies it more, never less.
#[test]
fn the_shadow_errs_strict() {
    const NODES: usize = 10;
    const HOT: u64 = 1;
    const BETWEEN: u64 = 2;
    const COLD: u64 = 3;
    const LOPSIDED: u64 = 4;
    let run = 30 * ONE_SEC;
    let from = 10 * ONE_SEC;
    let shadow = limit(200);
    let load = |s: &mut Sim<PerfectLink>| {
        // Hot under both limits, unevenly: 200 on one node, 60 on each
        // other.
        s.schedule_request_stream(0, HOT, 200, 0, run);
        // Between the thresholds: 25 a second on every node.
        // Cold under both: 5 a second on every node.
        for node in 0..NODES {
            if node > 0 {
                s.schedule_request_stream(node, HOT, 60, 0, run);
            }
            s.schedule_request_stream(node, BETWEEN, 25, 0, run);
            s.schedule_request_stream(node, COLD, 5, 0, run);
        }
        // Hot under both, on two nodes only.
        s.schedule_request_stream(2, LOPSIDED, 300, 0, run);
        s.schedule_request_stream(7, LOPSIDED, 300, 0, run);
    };
    let keys = [HOT, BETWEEN, COLD, LOPSIDED];

    let mut shadowed = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    for key in keys {
        shadowed.set_shadow(key, Some(shadow));
    }
    load(&mut shadowed);
    shadowed.run_until(run);

    let mut enforced = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    for key in keys {
        enforced.set_key_limit(key, shadow);
    }
    load(&mut enforced);
    enforced.run_until(run);

    let in_window = |a: &&rateguard_sim::sim::Admission| (from..run).contains(&a.at);
    let shadow_denied = |key: u64| {
        shadowed
            .shadow_denials()
            .iter()
            .filter(in_window)
            .filter(|a| a.key == key)
            .count()
    };
    let denied = |key: u64| {
        enforced
            .admissions()
            .iter()
            .filter(in_window)
            .filter(|a| a.key == key && a.decision != Decision::Allow)
            .count()
    };
    // Between the thresholds, the strict side: more, never fewer.
    assert!(
        shadow_denied(BETWEEN) >= denied(BETWEEN),
        "between the thresholds: {} shadow denials, fewer than the {} denied",
        shadow_denied(BETWEEN),
        denied(BETWEEN)
    );
    // Hot under both, or cold under both: the shadow is the enforced share,
    // scaled, so it denies what enforcing it does, within 1% or 5 requests.
    for key in [HOT, COLD, LOPSIDED] {
        let (shadow, denied) = (shadow_denied(key) as f64, denied(key) as f64);
        assert!(
            (shadow - denied).abs() <= (0.01 * denied).max(5.0),
            "key {key}: {shadow} shadow denials, {denied} denied"
        );
    }
    assert_eq!(
        shadowed
            .admissions()
            .iter()
            .filter(in_window)
            .filter(|a| a.decision != Decision::Allow)
            .count(),
        0,
        "the limit of 1000 is never reached once the keys are hot"
    );
}
