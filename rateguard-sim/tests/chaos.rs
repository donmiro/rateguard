// The named chaos scenarios of the roadmap. Each one watches the cluster
// every 100 ms of virtual time, not only at the end: a false death that
// heals before the deadline still reshuffles everyone's shares.

use rateguard_core::gcra::Nanos;
use rateguard_core::limiter::Config;
use rateguard_core::membership::Membership;
use rateguard_core::node::SwimConfig;
use rateguard_proto::Status;
use rateguard_sim::link::{Fate, Link, NetConfig, NodeIndex, SeededLink};
use rateguard_sim::seed;
use rateguard_sim::sim::{Sim, peer_of};

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const STEP: Nanos = 100 * ONE_MS;
const LATENCY: Nanos = ONE_MS;

fn config() -> Config {
    Config {
        limit_per_sec: 1000,
        burst: 10,
        alpha: 0.5,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 4,
        max_tracked_keys: 8,
        demand_time_constant: ONE_SEC,
    }
}

fn status(s: &Sim<impl Link>, viewer: NodeIndex, subject: NodeIndex) -> Option<Status> {
    s.node(viewer).members().status(peer_of(subject))
}

// Runs to `until`, calling `check` every STEP.
fn watch<L: Link>(s: &mut Sim<L>, until: Nanos, mut check: impl FnMut(&Sim<L>, Nanos)) {
    while s.now() < until {
        let next = (s.now() + STEP).min(until);
        s.run_until(next);
        check(s, next);
    }
}

fn nobody_buried(s: &Sim<impl Link>, now: Nanos) {
    for viewer in (0..s.node_count()).filter(|&viewer| !s.is_down(viewer)) {
        for subject in (0..s.node_count()).filter(|&subject| subject != viewer) {
            let seen = status(s, viewer, subject);
            assert!(
                matches!(seen, Some(Status::Alive | Status::Suspect)),
                "at {} ms node {viewer} holds node {subject} as {seen:?}",
                now / ONE_MS
            );
        }
    }
}

fn everyone_sees_everyone(s: &Sim<impl Link>) {
    for viewer in 0..s.node_count() {
        for subject in (0..s.node_count()).filter(|&subject| subject != viewer) {
            assert_eq!(
                status(s, viewer, subject),
                Some(Status::Alive),
                "at {} ms node {viewer} on node {subject}",
                s.now() / ONE_MS
            );
        }
    }
}

fn anyone_suspected(s: &Sim<impl Link>, subject: NodeIndex) -> bool {
    (0..s.node_count())
        .filter(|&viewer| viewer != subject)
        .any(|viewer| status(s, viewer, subject) == Some(Status::Suspect))
}

struct Perfect;
impl Link for Perfect {
    fn fate(&mut self, _from: NodeIndex, _to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        Fate::Delivered(now + LATENCY)
    }
}

// Nodes in different groups cannot reach each other while the split lasts.
struct Partition {
    groups: Vec<usize>,
    from: Nanos,
    until: Nanos,
}
impl Link for Partition {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        let split = (self.from..self.until).contains(&now);
        if split && self.groups[from] != self.groups[to] {
            Fate::Lost
        } else {
            Fate::Delivered(now + LATENCY)
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Both,
    Inbound,
}

// Lifeguard's Interval anomaly: the node loses its traffic in bursts and
// comes back between them.
struct Flapping {
    node: NodeIndex,
    direction: Direction,
    bad: Nanos,
    good: Nanos,
    until: Nanos,
}
impl Link for Flapping {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        let in_burst = now < self.until && now % (self.bad + self.good) < self.bad;
        let hit = match self.direction {
            Direction::Both => from == self.node || to == self.node,
            Direction::Inbound => to == self.node,
        };
        if in_burst && hit {
            Fate::Lost
        } else {
            Fate::Delivered(now + LATENCY)
        }
    }
}

#[test]
fn a_clean_3_2_partition_splits_the_cluster_and_heals() {
    let (cut, heal) = (2 * ONE_SEC, 20 * ONE_SEC);
    let mut s = Sim::new(
        5,
        config(),
        Partition {
            groups: vec![0, 0, 0, 1, 1],
            from: cut,
            until: heal,
        },
    );

    s.run_until(heal);
    for (node, size) in [(0, 3), (1, 3), (2, 3), (3, 2), (4, 2)] {
        assert_eq!(
            s.node(node).cluster_size(),
            size,
            "node {node}: each side believes it is the whole cluster"
        );
    }
    assert_eq!(status(&s, 0, 3), Some(Status::Dead));
    assert_eq!(status(&s, 3, 0), Some(Status::Dead));

    s.run_until(heal + 10 * ONE_SEC);
    everyone_sees_everyone(&s);
}

#[test]
fn an_asymmetric_partition_buries_nobody() {
    // Node 1 never hears node 0, while node 0 hears node 1 fine. Direct
    // probes fail in one direction or the other; PING-REQ through the
    // other nodes must cover both.
    struct OneWay;
    impl Link for OneWay {
        fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
            if (from, to) == (0, 1) {
                Fate::Lost
            } else {
                Fate::Delivered(now + LATENCY)
            }
        }
    }

    let mut s = Sim::new(5, config(), OneWay);
    watch(&mut s, 60 * ONE_SEC, |s, now| {
        nobody_buried(s, now);
        for node in [0, 1] {
            assert!(
                !anyone_suspected(s, node),
                "at {} ms node {node} is suspected despite PING-REQ",
                now / ONE_MS
            );
        }
    });
}

#[test]
fn a_30_percent_lossy_wire_buries_nobody() {
    seed::each_seed(3, |seed| {
        let lossy = NetConfig {
            loss: 0.3,
            ..NetConfig::perfect(LATENCY)
        };
        let mut s = Sim::new(5, config(), SeededLink::new(seed, lossy));
        watch(&mut s, 60 * ONE_SEC, nobody_buried);
    });
}

#[test]
fn a_5_second_gc_pause_is_not_taken_for_death() {
    let (start, length) = (3 * ONE_SEC, 5 * ONE_SEC);
    let mut s = Sim::new(5, config(), Perfect);
    s.pause(2, start, start + length);

    let mut suspected = false;
    watch(&mut s, start + length + 10 * ONE_SEC, |s, now| {
        nobody_buried(s, now);
        suspected |= anyone_suspected(s, 2);
    });
    assert!(suspected, "the pause was long enough to raise a suspicion");
    everyone_sees_everyone(&s);
}

#[test]
fn flapping_in_short_bursts_buries_nobody() {
    for direction in [Direction::Both, Direction::Inbound] {
        let mut s = Sim::new(
            5,
            config(),
            Flapping {
                node: 2,
                direction,
                bad: ONE_SEC,
                good: ONE_SEC,
                until: 60 * ONE_SEC,
            },
        );
        watch(&mut s, 60 * ONE_SEC, nobody_buried);
        s.run_until(70 * ONE_SEC);
        everyone_sees_everyone(&s);
    }
}

#[test]
fn a_quick_restart_under_the_same_id_goes_unnoticed() {
    let mut s = Sim::new(5, config(), Perfect);
    s.schedule_restart(2, 5 * ONE_SEC, 6 * ONE_SEC);
    watch(&mut s, 20 * ONE_SEC, nobody_buried);
    everyone_sees_everyone(&s);
}

#[test]
fn a_long_restart_under_the_same_id_rejoins_over_its_own_grave() {
    let swim = SwimConfig::default();
    let down = 5 * ONE_SEC;
    let up = down + swim.suspicion_timeout + 5 * ONE_SEC;
    let mut s = Sim::new(5, config(), Perfect);
    s.schedule_restart(2, down, up);

    s.run_until(up);
    assert_eq!(
        status(&s, 0, 2),
        Some(Status::Dead),
        "the cluster buried it"
    );
    assert_eq!(s.node(2).members().incarnation(), 0, "a fresh start");

    s.run_until(up + 5 * ONE_SEC);
    everyone_sees_everyone(&s);
    assert!(
        s.node(2).members().incarnation() >= 1,
        "it had to refute its own death to come back"
    );
}

#[test]
fn a_rolling_restart_of_the_whole_cluster_converges() {
    let mut s = Sim::with_seeds(5, config(), Perfect, &[0, 1]);
    s.run_until(5 * ONE_SEC);
    everyone_sees_everyone(&s);

    let (down, gap, rejoin) = (2 * ONE_SEC, 8 * ONE_SEC, 3 * ONE_SEC);
    let up_at = |node: NodeIndex| 5 * ONE_SEC + node as Nanos * gap + down;
    for node in 0..5 {
        s.schedule_restart(node, up_at(node) - down, up_at(node));
    }

    watch(&mut s, up_at(4) + 10 * ONE_SEC, |s, now| {
        for viewer in 0..5 {
            let rejoining = (up_at(viewer)..up_at(viewer) + rejoin).contains(&now);
            if s.is_down(viewer) || rejoining {
                continue;
            }
            let missing = 4 - s.node(viewer).members().peers().len();
            assert!(
                missing <= 1,
                "at {} ms node {viewer} misses {missing} peers: one restart at a time",
                now / ONE_MS
            );
        }
    });
    everyone_sees_everyone(&s);
}
