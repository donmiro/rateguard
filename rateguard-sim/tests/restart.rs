// A node restarted under the same ID starts again at incarnation 0, while
// its peers may hold it alive at a higher one: a pause it had to refute
// raised it. A restart too quick for any probe to miss goes unnoticed, and
// unless someone tells the node where it stands, its next leave is older
// than what the cluster holds and is dropped as stale.

use rateguard_core::gcra::Nanos;
use rateguard_core::limiter::{Config, Limit};
use rateguard_core::node::SwimConfig;
use rateguard_core::partition::PartitionPolicy;
use rateguard_proto::Status;
use rateguard_sim::link::{Link, NodeIndex, PerfectLink};
use rateguard_sim::sim::{Sim, peer_of};

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;

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

fn cluster() -> Sim<PerfectLink> {
    Sim::new(3, config(), PerfectLink::new(ONE_MS))
}

fn status(s: &Sim<impl Link>, viewer: NodeIndex, subject: NodeIndex) -> Option<Status> {
    s.node(viewer).members().status(peer_of(subject))
}

// Node 2 is paused long enough to be suspected and refutes, which takes it
// to incarnation 1 in everyone's table; then it restarts, at 0, between
// two probes.
fn quick_restart(s: &mut Sim<impl Link>, up_at: Nanos) {
    s.pause(2, 2 * ONE_SEC, 3 * ONE_SEC);
    s.run_until(8 * ONE_SEC);
    let held = s.node(0).members().update_about(peer_of(2)).unwrap();
    assert!(held.incarnation >= 1, "the pause must be refuted: {held:?}");
    s.schedule_restart(2, 10 * ONE_SEC, up_at);
}

#[test]
fn a_leave_after_an_unnoticed_restart_is_heard() {
    let mut s = cluster();
    quick_restart(&mut s, 10 * ONE_SEC + 1);
    s.schedule_leave(2, 15 * ONE_SEC);
    s.run_until(15 * ONE_SEC + 50 * ONE_MS);
    assert_eq!(status(&s, 0, 2), Some(Status::Dead));
    assert!(
        s.node(0).members().has_departed(peer_of(2)),
        "node 0 must take the leave as one, not as a crash"
    );
}

#[test]
fn a_restarted_node_catches_up_on_its_incarnation_within_a_round() {
    let up_at = 10 * ONE_SEC + 1;
    let mut s = cluster();
    quick_restart(&mut s, up_at);
    s.run_until(up_at + 2 * SwimConfig::default().protocol_period);
    let own = s.node(2).members().incarnation();
    assert!(own > 1, "node 2 is still at incarnation {own}");
}

#[test]
fn scaling_down_under_quorum_after_a_quick_restart_keeps_the_quorum() {
    // Two of three leave, one after a quick restart. Were that leave
    // dropped, node 0 would count node 2 as dead and possibly serving, and
    // with node 1 gone too it would sit at the floor without a majority.
    let mut s = cluster();
    s.set_partition_policy(PartitionPolicy::Quorum);
    s.pause(2, 2 * ONE_SEC, 4 * ONE_SEC);
    s.schedule_restart(2, 10 * ONE_SEC, 10 * ONE_SEC + 100 * ONE_MS);
    s.schedule_leave(2, 15 * ONE_SEC);
    s.schedule_leave(1, 30 * ONE_SEC);
    s.run_until(40 * ONE_SEC);
    assert!(!s.node(0).held().without_quorum, "{:?}", s.node(0).held());
}

#[test]
fn a_single_quick_restart_is_one_identity_conflict() {
    let mut s = cluster();
    quick_restart(&mut s, 10 * ONE_SEC + 1);
    s.run_until(14 * ONE_SEC);
    assert_eq!(s.node(2).identity_conflicts(), 1);
    for node in [0, 1] {
        assert_eq!(s.node(node).identity_conflicts(), 0);
    }
}
