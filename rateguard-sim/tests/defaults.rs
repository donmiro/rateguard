// The guarantees of spec §5.4 on the settings the runtime ships with:
// β = 0.05, 64 hot keys, 4096 tracked, a burst of limit / 20. The other
// scenario files use small tables and β = 0.1, to keep their arithmetic
// round; these check that nothing rests on that.
//
// R = 1000 over 5 nodes. The standard invariant checks every second of
// every run: no key above R plus a burst per node.

use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::Config;
use rateguard_core::partition::PartitionPolicy;
use rateguard_sim::link::{Fate, Link, NodeIndex, PerfectLink};
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const NODES: usize = 5;
const KEY: u64 = 42;
const LIMIT: f64 = 1000.0;
const BETA: f64 = 0.05;

// rateguard's Builder with `.limit(1000)` and nothing else.
fn defaults() -> Config {
    Config {
        limit_per_sec: 1000,
        burst: 50,
        alpha: 0.5,
        floor_factor: BETA,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 64,
        max_tracked_keys: 4096,
        demand_time_constant: ONE_SEC,
    }
}

fn rate(s: &Sim<impl Link>, from: Nanos, until: Nanos, nodes: &[NodeIndex]) -> f64 {
    let admitted = s
        .admissions()
        .iter()
        .filter(|a| a.key == KEY && nodes.contains(&a.node) && (from..until).contains(&a.at))
        .filter(|a| a.decision == Decision::Allow)
        .count();
    admitted as f64 / ((until - from) as f64 / ONE_SEC as f64)
}

const ALL: [NodeIndex; NODES] = [0, 1, 2, 3, 4];

// R plus one burst per node, spread over a window of `secs`.
fn ceiling(secs: u64) -> f64 {
    LIMIT + (NODES as f64 * defaults().burst as f64) / secs as f64
}

fn assert_between(what: &str, rate: f64, low: f64, high: f64) {
    assert!(
        (low..=high).contains(&rate),
        "{what}: {rate:.1} a second, expected {low:.1}..={high:.1}"
    );
}

#[test]
fn a_skewed_cluster_admits_close_to_the_limit() {
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    let run = 40 * ONE_SEC;
    s.schedule_request_stream(0, KEY, 1200, 0, run);
    for node in 1..NODES {
        s.schedule_request_stream(node, KEY, 150, 0, run);
    }
    s.run_until(run);
    assert_between(
        "skewed",
        rate(&s, 20 * ONE_SEC, run, &ALL),
        950.0,
        ceiling(20),
    );
}

// Guarantee 2, its lower bound: with demand on k of N nodes the floors of
// the other N − k go unused, R × (1 − β(N − k)/N) at least.
fn demand_on(k: usize) -> f64 {
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    let run = 40 * ONE_SEC;
    for node in 0..k {
        s.schedule_request_stream(node, KEY, 1500, 0, run);
    }
    s.run_until(run);
    rate(&s, 20 * ONE_SEC, run, &ALL)
}

fn lower_bound(k: usize) -> f64 {
    LIMIT * (1.0 - BETA * (NODES - k) as f64 / NODES as f64)
}

#[test]
fn demand_on_one_node_admits_all_but_the_idle_floors() {
    assert_between("k = 1", demand_on(1), lower_bound(1) - 5.0, ceiling(20));
}

#[test]
fn demand_on_three_nodes_admits_all_but_the_idle_floors() {
    assert_between("k = 3", demand_on(3), lower_bound(3) - 5.0, ceiling(20));
}

// Nodes 0-2 and 3-4 cannot reach each other in [CUT, HEAL).
const CUT: Nanos = 10 * ONE_SEC;
const HEAL: Nanos = 40 * ONE_SEC;
struct Split;
impl Link for Split {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if (CUT..HEAL).contains(&now) && (from < 3) != (to < 3) {
            Fate::Lost
        } else {
            Fate::Delivered(now + ONE_MS)
        }
    }
}

// The minority drops to its floors: R × β / N = 10 a node, 20 in all.
#[test]
fn quorum_holds_the_minority_to_its_floors() {
    let mut s = Sim::new(NODES, defaults(), Split);
    s.set_partition_policy(PartitionPolicy::Quorum);
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 400, 0, 55 * ONE_SEC);
    }
    s.run_until(55 * ONE_SEC);

    let settled = CUT + 10 * ONE_SEC;
    let window = (HEAL - settled) / ONE_SEC;
    assert_between(
        "majority",
        rate(&s, settled, HEAL, &[0, 1, 2]),
        950.0,
        ceiling(window),
    );
    let minority_ceiling = 2.0 * LIMIT * BETA / NODES as f64 + 2.0 * 50.0 / window as f64;
    assert_between(
        "minority",
        rate(&s, settled, HEAL, &[3, 4]),
        15.0,
        minority_ceiling,
    );
}

// Spec §6.2 under traffic: a 5 s GC pause on node 4. Its peers suspect it
// but HoldDown keeps the shares where they were: the other four admit what
// they admitted before, no more, no less.
#[test]
fn a_gc_pause_leaves_the_other_shares_alone() {
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    let run = 50 * ONE_SEC;
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 400, 0, run);
    }
    s.pause(4, 25 * ONE_SEC, 30 * ONE_SEC);
    s.run_until(run);

    let others = [0, 1, 2, 3];
    let before = rate(&s, 15 * ONE_SEC, 25 * ONE_SEC, &others);
    let during = rate(&s, 25 * ONE_SEC, 30 * ONE_SEC, &others);
    assert!(
        (during - before).abs() <= 0.05 * before,
        "the four admitted {before:.1} a second before the pause, {during:.1} during it"
    );
}

// Spec §6.2 under traffic: every node restarted in turn, down 1 s each,
// joining back through the seeds. No second goes without service.
#[test]
fn a_rolling_restart_never_stops_service() {
    let mut s = Sim::with_seeds(NODES, defaults(), PerfectLink::new(ONE_MS), &[0, 1]);
    let run = 60 * ONE_SEC;
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 400, 0, run);
    }
    for node in 0..NODES {
        let down = (15 + 8 * node as u64) * ONE_SEC;
        s.schedule_restart(node, down, down + ONE_SEC);
    }
    s.run_until(run);

    for second in 15..60 {
        let from = second * ONE_SEC;
        let admitted = rate(&s, from, from + ONE_SEC, &ALL);
        assert!(admitted >= 500.0, "second {second}: {admitted:.0} admitted");
    }
}

// Spec §6.2: traffic jumps a hundredfold, from 10 a second a node, cold,
// to 1000, hot everywhere at once. The invariant holds through the jump,
// and the cluster settles on R.
#[test]
fn a_hundredfold_jump_in_traffic_stays_within_the_limit() {
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    let jump = 20 * ONE_SEC;
    let run = 50 * ONE_SEC;
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 10, 0, jump);
        s.schedule_request_stream(node, KEY, 1000, jump, run);
    }
    s.run_until(run);
    assert_between("before", rate(&s, 5 * ONE_SEC, jump, &ALL), 49.0, 51.0);
    assert_between(
        "settled",
        rate(&s, jump + 15 * ONE_SEC, run, &ALL),
        950.0,
        ceiling(15),
    );
}

// Node 0's datagrams to node 1 are lost; everything else gets through.
// Membership survives it through PING-REQ; demand goes first hand only, so
// node 1 never hears node 0's.
struct OneWay;
impl Link for OneWay {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if (from, to) == (0, 1) {
            Fate::Lost
        } else {
            Fate::Delivered(now + ONE_MS)
        }
    }
}

// Node 1 hears node 0's demand only through the helpers of its probes,
// on the ACK they relay. Without that it forgot it once stale and took the
// difference: 1061 a second.
#[test]
fn an_asymmetric_partition_under_load_stays_within_the_limit() {
    let mut s = Sim::new(NODES, defaults(), OneWay);
    let run = 40 * ONE_SEC;
    s.schedule_request_stream(0, KEY, 1200, 0, run);
    for node in 1..NODES {
        s.schedule_request_stream(node, KEY, 150, 0, run);
    }
    s.run_until(run);
    assert_between(
        "one way",
        rate(&s, 20 * ONE_SEC, run, &ALL),
        950.0,
        ceiling(20),
    );
}

// The same link, failing only from 15 s on, after node 1 has heard node 0
// directly. Node 1's demand then quadruples. Its share must follow: it
// weighs its own demand as of when the peers' demand dates from, and node
// 0's dates from the last relayed report, not from the last direct word.
struct OneWayLater;
impl Link for OneWayLater {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if (from, to) == (0, 1) && now >= 15 * ONE_SEC {
            Fate::Lost
        } else {
            Fate::Delivered(now + ONE_MS)
        }
    }
}

#[test]
fn demand_rising_behind_an_asymmetric_partition_gets_its_share() {
    let mut s = Sim::new(NODES, defaults(), OneWayLater);
    let run = 60 * ONE_SEC;
    s.schedule_request_stream(0, KEY, 1200, 0, run);
    s.schedule_request_stream(1, KEY, 150, 0, 20 * ONE_SEC);
    s.schedule_request_stream(1, KEY, 600, 20 * ONE_SEC, run);
    for node in 2..NODES {
        s.schedule_request_stream(node, KEY, 150, 0, run);
    }
    s.run_until(run);
    assert_between(
        "node 1 risen",
        rate(&s, 40 * ONE_SEC, run, &ALL),
        950.0,
        ceiling(20),
    );
    // Node 1 asks 600 of 2250: about a quarter of R, not the 150 it had.
    assert_between(
        "node 1 alone",
        rate(&s, 40 * ONE_SEC, run, &[1]),
        230.0,
        290.0,
    );
}
