// The overshoot bounds of spec §5.1, policy by policy, on a clean 3/2
// split. Every node serves 400 attempts a second on one key, 2000 in all
// against a limit of 1000: whatever is admitted is what the shares allow.

use rateguard_core::gcra::Nanos;
use rateguard_core::limiter::Config;
use rateguard_core::partition::PartitionPolicy;
use rateguard_sim::invariant::AdmissionWindow;
use rateguard_sim::link::{Fate, Link, NodeIndex};
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const LATENCY: Nanos = ONE_MS;
const NODES: usize = 5;
const KEY: u64 = 42;
const CUT: Nanos = 10 * ONE_SEC;
const HEAL: Nanos = 40 * ONE_SEC;
const END: Nanos = 55 * ONE_SEC;

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

// Nodes 0-2 and 3-4 cannot reach each other in [CUT, HEAL).
struct Split;
impl Link for Split {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if (CUT..HEAL).contains(&now) && (from < 3) != (to < 3) {
            Fate::Lost
        } else {
            Fate::Delivered(now + LATENCY)
        }
    }
}

fn split_under(policy: PartitionPolicy) -> Sim<Split> {
    let mut s = Sim::new(NODES, config(), Split);
    s.set_partition_policy(policy);
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 400, 0, END);
    }
    s
}

fn rate(s: &Sim<impl Link>, from: Nanos, until: Nanos, nodes: &[NodeIndex]) -> f64 {
    let admitted: usize = nodes
        .iter()
        .map(|&node| {
            s.admissions()
                .iter()
                .filter(|a| a.node == node && (from..until).contains(&a.at))
                .filter(|a| a.decision == rateguard_core::gcra::Decision::Allow)
                .count()
        })
        .sum();
    admitted as f64 / ((until - from) as f64 / ONE_SEC as f64)
}

const ALL: [NodeIndex; NODES] = [0, 1, 2, 3, 4];

// R, plus one burst per node over the shortest window measured (5 s): the
// bound of spec §5.4 allows each node its burst on top of its share.
const LIMIT_WITH_BURSTS: f64 = 1000.0 + (NODES * 10) as f64 / 5.0;

fn assert_between(what: &str, rate: f64, low: f64, high: f64) {
    assert!(
        (low..=high).contains(&rate),
        "{what}: {rate:.3} a second, expected {low}..={high}"
    );
}

fn assert_healthy_before_and_after(s: &Sim<Split>) {
    assert_between(
        "before the split",
        rate(s, 5 * ONE_SEC, CUT, &ALL),
        950.0,
        LIMIT_WITH_BURSTS,
    );
    assert_between(
        "after the heal",
        rate(s, HEAL + 10 * ONE_SEC, END, &ALL),
        950.0,
        LIMIT_WITH_BURSTS,
    );
}

// Each side takes what its own demand earns: the three-node side 1000, the
// two-node side all of its 800. Up to k·R, by design.
#[test]
fn optimistic_lets_each_side_take_the_whole_limit() {
    let mut s = split_under(PartitionPolicy::Optimistic);
    s.remove_invariant("admission-window");
    s.add_invariant(AdmissionWindow::scaled(ONE_SEC, 2));
    s.run_until(END);

    assert_between(
        "split",
        rate(&s, CUT + 10 * ONE_SEC, HEAL, &ALL),
        1700.0,
        1850.0,
    );
    assert_healthy_before_and_after(&s);
}

// Held longer than the split: shares computed as if the cluster were
// whole, about R all along. The standard invariant checks every second.
#[test]
fn a_hold_longer_than_the_split_keeps_the_limit() {
    let mut s = split_under(PartitionPolicy::HoldDown(60 * ONE_SEC));
    s.run_until(END);

    assert_between("split", rate(&s, CUT, HEAL, &ALL), 950.0, LIMIT_WITH_BURSTS);
    assert_healthy_before_and_after(&s);
}

// Held for 5 s: about R until the hold runs out, k·R after.
#[test]
fn a_short_hold_keeps_the_limit_then_lets_go() {
    let mut s = split_under(PartitionPolicy::HoldDown(5 * ONE_SEC));
    s.remove_invariant("admission-window");
    s.add_invariant(AdmissionWindow::scaled(ONE_SEC, 2));
    s.run_until(END);

    assert_between(
        "held",
        rate(&s, CUT, CUT + 5 * ONE_SEC, &ALL),
        950.0,
        LIMIT_WITH_BURSTS,
    );
    assert_between(
        "let go",
        rate(&s, CUT + 20 * ONE_SEC, HEAL, &ALL),
        1700.0,
        1850.0,
    );
    assert_healthy_before_and_after(&s);
}

// The majority side shares R among itself; the minority drops to the
// floor, R·β/N = 20 a node. R plus 40, within the standard bound.
#[test]
fn quorum_keeps_the_limit_and_starves_the_minority() {
    let mut s = split_under(PartitionPolicy::Quorum);
    s.run_until(END);

    let settled = CUT + 10 * ONE_SEC;
    assert_between(
        "majority",
        rate(&s, settled, HEAL, &[0, 1, 2]),
        950.0,
        LIMIT_WITH_BURSTS,
    );
    assert_between("minority", rate(&s, settled, HEAL, &[3, 4]), 35.0, 45.0);
    assert_healthy_before_and_after(&s);
}

// Node 4 cannot reach anyone for its first 5 s.
struct LateJoiner;
impl Link for LateJoiner {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if now < 5 * ONE_SEC && (from == 4 || to == 4) {
            Fate::Lost
        } else {
            Fate::Delivered(now + LATENCY)
        }
    }
}

// A node that has seeds but has not joined yet does not know N. Taking
// itself for the whole cluster, it would admit as if alone on top of what
// the others share.
#[test]
fn a_node_that_has_not_joined_yet_does_not_take_the_limit() {
    let mut s = Sim::with_seeds(NODES, config(), LateJoiner, &[0, 1, 2, 3]);
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 400, 0, 20 * ONE_SEC);
    }
    s.run_until(20 * ONE_SEC);

    assert_between(
        "joined",
        rate(&s, 15 * ONE_SEC, 20 * ONE_SEC, &ALL),
        950.0,
        LIMIT_WITH_BURSTS,
    );
}

// Nodes 0-2 and 3-4 cannot reach each other in [CUT, LONG_HEAL): longer
// than it takes the dead to be forgotten.
const LONG_HEAL: Nanos = 640 * ONE_SEC;
const LONG_END: Nanos = LONG_HEAL + 40 * ONE_SEC;
struct LongSplit;
impl Link for LongSplit {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if (CUT..LONG_HEAL).contains(&now) && (from < 3) != (to < 3) {
            Fate::Lost
        } else {
            Fate::Delivered(now + LATENCY)
        }
    }
}

// Ten minutes in, each side has forgotten the other. The minority must not
// take itself for the whole cluster then; after the heal, through the
// seed, the cluster is whole again. The standard invariant checks every
// second of it.
#[test]
fn quorum_outlasts_the_tombstones() {
    let mut s = Sim::with_seeds(NODES, config(), LongSplit, &[0]);
    s.set_partition_policy(PartitionPolicy::Quorum);
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 400, 0, LONG_END);
    }
    s.run_until(LONG_END);

    let forgotten = LONG_HEAL - 20 * ONE_SEC;
    assert_eq!(s.node(3).members().dead().count(), 0, "the dead forgotten");
    assert_between(
        "minority, the dead forgotten",
        rate(&s, forgotten, LONG_HEAL, &[3, 4]),
        35.0,
        45.0,
    );
    assert_between(
        "after the heal",
        rate(&s, LONG_HEAL + 20 * ONE_SEC, LONG_END, &ALL),
        950.0,
        LIMIT_WITH_BURSTS,
    );
}
