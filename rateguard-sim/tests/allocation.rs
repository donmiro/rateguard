// The allocation layer across a cluster: demand exchanged first hand on the
// SWIM messages themselves (spec §4.2, §10.8).

use rateguard_core::gcra::Nanos;
use rateguard_core::limiter::Config;
use rateguard_core::peer_demand::stale_after_rounds;
use rateguard_sim::link::{Link, NetConfig, PerfectLink, SeededLink};
use rateguard_sim::sim::{Sim, peer_of};

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const PERIOD: Nanos = 200 * ONE_MS;
const NODES: usize = 5;
const KEY: u64 = 42;
const RATE: u32 = 800;

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

// Node 0 serves RATE attempts a second on KEY for `run`; every other node
// must know it, and have heard it recently.
fn assert_every_peer_heard_node_0(s: &mut Sim<impl Link>, run: Nanos, fresh_within: Nanos) {
    s.schedule_request_stream(0, KEY, RATE, 0, run);
    s.run_until(run);

    for node in 1..NODES {
        let heard = s
            .node(node)
            .peer_demand()
            .get(peer_of(0), KEY)
            .unwrap_or_else(|| panic!("node {node} never heard node 0's demand"));
        assert!(
            heard.primary,
            "node {node}: hot at node 0 by its own demand"
        );
        assert!(
            (0.9 * RATE as f32..=RATE as f32).contains(&heard.demand),
            "node {node} heard {} attempts a second, node 0 serves {RATE}",
            heard.demand
        );
        assert!(
            s.now() - heard.heard_at <= fresh_within,
            "node {node} last heard node 0 {} ms ago",
            (s.now() - heard.heard_at) / ONE_MS
        );
    }
}

#[test]
fn every_peer_hears_the_demand_of_a_busy_node() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    // Over 5 nodes the direct contacts of a pair are at most ~7 rounds
    // apart (spec §10.8); 2N rounds is the margin.
    assert_every_peer_heard_node_0(&mut s, 10 * ONE_SEC, 2 * NODES as Nanos * PERIOD);
}

#[test]
fn demand_gets_through_30_percent_loss() {
    let lossy = NetConfig {
        loss: 0.3,
        ..NetConfig::perfect(ONE_MS)
    };
    let mut s = Sim::new(NODES, config(), SeededLink::new(8371, lossy));
    assert_every_peer_heard_node_0(&mut s, 20 * ONE_SEC, 4 * NODES as Nanos * PERIOD);
}

#[test]
fn a_quiet_cluster_exchanges_no_demand() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    s.run_until(10 * ONE_SEC);
    for node in 0..NODES {
        assert!(s.node(node).peer_demand().is_empty(), "node {node}");
    }
}

#[test]
fn a_node_that_goes_quiet_drops_out_of_its_peers_tables() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    s.schedule_request_stream(0, KEY, RATE, 0, 10 * ONE_SEC);
    s.run_until(10 * ONE_SEC);
    assert!(!s.node(1).peer_demand().is_empty());

    // The EWMA falls under the hot threshold in ~2 s, then the key stays
    // hot for the 5 s cooldown.
    while s.node(0).limiter().is_hot(KEY) {
        s.run_for(PERIOD);
    }
    let cooled = s.now();

    // From then on node 0 sends no report, and its next message to each
    // peer says it has no hot key. That must beat the staleness threshold,
    // which would clear the tables anyway.
    let peers_forgot =
        |s: &Sim<PerfectLink>| (1..NODES).all(|node| s.node(node).peer_demand().is_empty());
    while !peers_forgot(&s) {
        s.run_for(PERIOD);
    }
    let rounds = (s.now() - cooled) / PERIOD;
    assert!(
        rounds < stale_after_rounds(NODES) * 2 / 3,
        "{rounds} rounds after the key cooled"
    );
}

#[test]
fn peer_tables_stay_bounded_while_hot_keys_come_and_go() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    let keys = 20;
    for k in 0..keys {
        let start = k * 10 * ONE_SEC;
        s.schedule_request_stream(0, KEY + k, RATE, start, start + 10 * ONE_SEC);
    }

    let mut largest = 0;
    while s.now() < keys * 10 * ONE_SEC {
        s.run_for(PERIOD);
        largest = (1..NODES)
            .map(|node| s.node(node).peer_demand().len())
            .chain([largest])
            .max()
            .unwrap();
    }
    // A key is hot for its 10 s and the cooldown after, so two overlap.
    assert!(largest <= 2, "{largest} keys of node 0 known at once");
}

// Guarantee 2 of spec §5.4. Every node is over the hot threshold (100 a
// second at N = 5), node 0 far over the rest: an even split would admit
// 200 + 4 × 150 = 800 a second, shares that follow demand close to R.
#[test]
fn a_skewed_cluster_admits_close_to_the_limit() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    let run = 40 * ONE_SEC;
    s.schedule_request_stream(0, KEY, 1200, 0, run);
    for node in 1..NODES {
        s.schedule_request_stream(node, KEY, 150, 0, run);
    }
    s.run_until(run);

    let settled = 20 * ONE_SEC;
    let rate = s.admitted_between(settled, run) as f64 / ((run - settled) / ONE_SEC) as f64;
    assert!(
        (950.0..=1000.0).contains(&rate),
        "admitted {rate:.1} a second"
    );
}

// A key hot at node 0 and lukewarm everywhere else: 50 a second is under
// the hot threshold of 100. Were the others left cold, node 0 would take
// all but their floors, 920, and they their 50 each on top: 1120 a second
// (spec §4.1). Hot by node 0's news, they take shares instead.
fn mixed_key(s: &mut Sim<PerfectLink>, until: Nanos) {
    s.schedule_request_stream(0, KEY, 1200, 0, until);
    for node in 1..NODES {
        s.schedule_request_stream(node, KEY, 50, 0, 60 * ONE_SEC);
    }
}

fn admitted_per_sec(s: &Sim<impl Link>, from: Nanos, until: Nanos) -> f64 {
    s.admitted_between(from, until) as f64 / ((until - from) as f64 / ONE_SEC as f64)
}

#[test]
fn a_key_hot_on_one_node_only_stays_within_the_limit() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    let run = 40 * ONE_SEC;
    mixed_key(&mut s, run);
    s.run_until(run);

    for node in 1..NODES {
        assert!(s.node(node).limiter().is_hot(KEY), "node {node}");
    }
    let rate = admitted_per_sec(&s, 20 * ONE_SEC, run);
    assert!(
        (950.0..=1000.0).contains(&rate),
        "admitted {rate:.1} a second"
    );
}

#[test]
fn once_the_busy_node_cools_nobody_keeps_the_key_hot() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    mixed_key(&mut s, 20 * ONE_SEC);
    s.run_until(20 * ONE_SEC);
    assert!(s.node(1).limiter().is_hot(KEY));

    // Node 0's demand decays under the threshold in ~2.5 s, its cooldown
    // takes 5 more, then its next message to each peer drops the key.
    s.run_until(35 * ONE_SEC);
    for node in 0..NODES {
        assert!(!s.node(node).limiter().is_hot(KEY), "node {node}");
    }
}

// A restarted node knows nobody's demand yet. Without learning mode it
// would take all but the others' floors, 920, on top of what the others
// admit.
#[test]
fn a_node_restarted_under_load_does_not_overshoot() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    let run = 40 * ONE_SEC;
    s.schedule_request_stream(0, KEY, 1200, 0, run);
    for node in 1..NODES {
        s.schedule_request_stream(node, KEY, 150, 0, run);
    }
    let (down, up) = (20 * ONE_SEC, 21 * ONE_SEC);
    s.schedule_restart(0, down, up);
    s.run_until(run);

    let mut worst: f64 = 0.0;
    let mut from = up;
    while from + ONE_SEC <= run {
        worst = worst.max(admitted_per_sec(&s, from, from + ONE_SEC));
        from += PERIOD;
    }
    assert!(worst <= 1050.0, "{worst:.0} admitted in a second");
}

// A key nobody had seen turns hot on three nodes at once, well after they
// have all learned each other. Each knows nothing yet of the others'
// demand for it; taking its demand for the whole of it, each would take all
// but the others' floors, close to 3 × R between them (spec §10.5).
#[test]
fn a_key_that_turns_hot_on_several_nodes_at_once_stays_within_the_limit() {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    let (start, run) = (10 * ONE_SEC, 25 * ONE_SEC);
    for node in [0, 2, 4] {
        s.schedule_request_stream(node, KEY, 700, start, run);
    }
    s.run_until(run);

    let rate = admitted_per_sec(&s, start + 10 * ONE_SEC, run);
    assert!(
        (950.0..=1000.0).contains(&rate),
        "admitted {rate:.1} a second"
    );
}
