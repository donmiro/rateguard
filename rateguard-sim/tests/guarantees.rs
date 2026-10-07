// The guarantees of spec §5.4 that the membership layer already owns.

use rateguard_core::gcra::Nanos;
use rateguard_core::limiter::Config;
use rateguard_proto::MAX_DATAGRAM;
use rateguard_sim::link::{Fate, Link, NetConfig, NodeIndex, PerfectLink, SeededLink};
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const PERIOD: Nanos = 200 * ONE_MS;
const RUN: Nanos = 30 * ONE_SEC;

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

fn per_node_per_period(s: &Sim<impl Link>) -> f64 {
    let total: u64 = (0..s.node_count()).map(|node| s.datagrams_sent(node)).sum();
    total as f64 / s.node_count() as f64 / (s.now() / PERIOD) as f64
}

// Guarantee 5. In a healthy cluster a node sends its own PING and answers,
// on average, the one PING aimed at it: two datagrams a period, whatever
// the traffic and whatever the size of the cluster.
#[test]
fn bandwidth_is_two_datagrams_per_node_per_period() {
    for node_count in [5, 20, 50] {
        let mut s = Sim::new(node_count, config(), PerfectLink::new(ONE_MS));
        s.run_until(RUN);
        let rate = per_node_per_period(&s);
        assert!(
            (1.95..=2.05).contains(&rate),
            "{node_count} nodes: {rate:.3} datagrams per node per period"
        );
    }
}

#[test]
fn bandwidth_does_not_depend_on_traffic() {
    let sent = |requests_per_sec: u32| {
        let mut s = Sim::new(5, config(), PerfectLink::new(ONE_MS));
        if requests_per_sec > 0 {
            for node in 0..5 {
                s.schedule_request_stream(node, node as u64, requests_per_sec, 0, RUN);
            }
        }
        s.run_until(RUN);
        (0..5)
            .map(|node| s.datagrams_sent(node))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        sent(0),
        sent(5_000),
        "requests are served locally and never reach the wire"
    );
}

// rateguard-proto reserves at most 300 bytes of the datagram for membership
// and leaves the rest to demand[]. In practice gossip spreads over time even
// when a lot happens at once, and the real datagrams stay far smaller.
const MEMBERSHIP_BUDGET: usize = 300;

fn assert_within_budget(scenario: &str, s: &Sim<impl Link>) {
    let largest = s.largest_datagram();
    assert!(largest <= MAX_DATAGRAM, "{scenario}: {largest} bytes");
    assert!(
        largest <= MEMBERSHIP_BUDGET,
        "{scenario}: membership took {largest} bytes, its budget is {MEMBERSHIP_BUDGET}"
    );
}

#[test]
fn membership_leaves_the_datagram_to_demand_even_in_a_storm() {
    let mut s = Sim::with_seeds(100, config(), PerfectLink::new(ONE_MS), &[0]);
    s.run_until(RUN);
    assert_within_budget("100 nodes joining through one seed", &s);

    struct HalfDies;
    impl Link for HalfDies {
        fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
            let dead = |node: NodeIndex| node >= 25;
            if now >= 5 * ONE_SEC && (dead(from) || dead(to)) {
                Fate::Lost
            } else {
                Fate::Delivered(now + ONE_MS)
            }
        }
    }
    let mut s = Sim::new(50, config(), HalfDies);
    s.run_until(RUN);
    assert_within_budget("25 of 50 nodes failing at once", &s);

    let lossy = NetConfig {
        loss: 0.3,
        ..NetConfig::perfect(ONE_MS)
    };
    let mut s = Sim::new(20, config(), SeededLink::new(7, lossy));
    s.run_until(RUN);
    assert_within_budget("30% loss", &s);
}
