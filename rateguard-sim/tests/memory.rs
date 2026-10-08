// Guarantee 4 of spec §5.4: a node's memory is bounded by its config and
// grows with hot keys × N, not with traffic, nor with how many distinct
// keys or members come and go. Every collection a node keeps is sampled
// every protocol period and held to its bound.

use rateguard_core::gcra::Nanos;
use rateguard_core::limiter::Config;
use rateguard_core::node::{Footprint, SwimConfig};
use rateguard_core::peer_demand::stale_after_rounds;
use rateguard_proto::MAX_DEMAND_KEYS;
use rateguard_sim::link::{Fate, Link, NetConfig, NodeIndex, SeededLink};
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const PERIOD: Nanos = 200 * ONE_MS;

fn config() -> Config {
    Config {
        limit_per_sec: 1000,
        burst: 50,
        alpha: 0.5,
        floor_factor: 0.05,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 64,
        max_tracked_keys: 256,
        demand_time_constant: ONE_SEC,
    }
}

// The most keys the scenario below brings that a node has not seen before,
// within one round: 80 hot keys at once when a phase turns over, and the
// cold stream's 20 a round.
const NEW_KEYS_PER_ROUND: usize = 80 + 20;

// What a node may hold, from its config and the members it knows of. Tracked
// keys are capped at each round (the standard invariant checks it right
// after); in between, the keys new since then come on top.
fn assert_bounded(node: NodeIndex, now: Nanos, f: &Footprint) {
    let c = config();
    let known = f.members;
    let n = known; // the live part is at most the whole table
    let checks = [
        (
            "tracked keys",
            f.tracked_keys,
            c.max_tracked_keys + NEW_KEYS_PER_ROUND,
        ),
        ("hot keys", f.hot_keys, c.hot_set_size),
        (
            "peers' demand",
            f.peer_demand_keys,
            known.saturating_sub(1) * MAX_DEMAND_KEYS,
        ),
        ("last heard", f.last_heard, known),
        ("gossip", f.gossip, known),
        (
            "own reports",
            f.reported_rounds,
            stale_after_rounds(n) as usize + 2,
        ),
        (
            "own reported keys",
            f.reported_keys,
            (stale_after_rounds(n) as usize + 2) * MAX_DEMAND_KEYS,
        ),
        ("recent shares", f.recent_shares, c.hot_set_size),
        ("relays", f.relays, known),
        ("probe order", f.probe_order, known),
    ];
    for (what, have, bound) in checks {
        assert!(
            have <= bound,
            "node {node} at {} ms: {what} {have}, bound {bound} ({f:?})",
            now / ONE_MS
        );
    }
}

// Runs to `until`, sampling every live node every period; returns the
// largest number of peers' demand entries any node held.
fn watch(s: &mut Sim<impl Link>, until: Nanos) -> usize {
    let mut peak = 0;
    while s.now() < until {
        let next = (s.now() + PERIOD).min(until);
        s.run_until(next);
        for node in (0..s.node_count()).filter(|&node| !s.is_down(node)) {
            let f = s.node(node).footprint();
            assert_bounded(node, next, &f);
            peak = peak.max(f.peer_demand_keys);
        }
    }
    peak
}

// Every node turns 80 keys hot at a time, more than the 64 it can keep,
// and moves on to 80 new ones every 10 s; on top, a stream of keys never
// seen before, more than it can track. 30% of datagrams are lost.
fn churning_hot_keys(nodes: usize) -> usize {
    let lossy = NetConfig {
        loss: 0.3,
        ..NetConfig::perfect(ONE_MS)
    };
    let mut s = Sim::new(nodes, config(), SeededLink::new(8371, lossy));
    // Over the hot threshold R/N × α at any N.
    let per_key = (2.0 * 1000.0 * 0.5 / nodes as f64) as u32;
    let run = 30 * ONE_SEC;
    for node in 0..nodes {
        for phase in 0..3u64 {
            let (from, until) = (phase * 10 * ONE_SEC, (phase + 1) * 10 * ONE_SEC);
            for k in 0..80 {
                let key = 1_000_000 * (node as u64 + 1) + 1000 * phase + k;
                s.schedule_request_stream(node, key, per_key, from, until);
            }
        }
        for i in 0..3000u64 {
            let key = u64::MAX - 10_000 * (node as u64 + 1) - i;
            s.schedule_request(i * 10 * ONE_MS, node, key);
        }
    }
    watch(&mut s, run)
}

#[test]
fn churning_hot_keys_keep_every_collection_bounded_at_5_nodes() {
    churning_hot_keys(5);
}

// The coordination state grows with N, and no faster: at most linearly.
#[test]
fn coordination_state_grows_no_faster_than_the_cluster() {
    let (small, large) = (churning_hot_keys(5), churning_hot_keys(20));
    let ratio = large as f64 / small.max(1) as f64;
    assert!(
        ratio <= 19.0 / 4.0 * 1.2,
        "peers' demand: {small} at N = 5, {large} at N = 20, {ratio:.1}×"
    );
}

// Nodes 0-2 run throughout. Nodes 3.. are pods that come and go, each
// under its own ID: node 3 + i joins at 30 s × i and leaves 30 s later, for
// good. Until its time a pod hears nothing and is heard by nobody.
const RESIDENTS: usize = 3;
const PODS: usize = 24;
const STAY: Nanos = 30 * ONE_SEC;

struct Pods;
impl Link for Pods {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        let joined =
            |node: NodeIndex| node < RESIDENTS || now >= (node - RESIDENTS) as Nanos * STAY;
        if joined(from) && joined(to) {
            Fate::Delivered(now + ONE_MS)
        } else {
            Fate::Lost
        }
    }
}

#[test]
fn members_that_come_and_go_are_forgotten_with_all_kept_about_them() {
    let swim = SwimConfig::default();
    let mut s = Sim::with_seeds(RESIDENTS + PODS, config(), Pods, &[0, 1, 2]);
    for pod in 0..PODS {
        let node = RESIDENTS + pod;
        let leaves = (pod as Nanos + 1) * STAY;
        s.schedule_restart(node, leaves, Nanos::MAX / 2);
        s.schedule_request_stream(node, 42, 400, pod as Nanos * STAY, leaves);
    }
    for node in 0..RESIDENTS {
        s.schedule_request_stream(node, 42, 400, 0, 15 * 60 * ONE_SEC);
    }
    // Past the tombstones of the first pods to leave.
    let run = PODS as Nanos * STAY + swim.suspicion_timeout + 2 * 60 * ONE_SEC;
    watch(&mut s, run);

    // At most the residents, the pods still around, and the dead not yet
    // forgotten: those that left in the last tombstone_ttl.
    let remembered = (swim.tombstone_ttl / STAY) as usize + 2;
    for node in 0..RESIDENTS {
        let f = s.node(node).footprint();
        assert!(
            f.members <= RESIDENTS + remembered,
            "node {node} remembers {} members, {} pods having come and gone",
            f.members,
            PODS
        );
    }
}
