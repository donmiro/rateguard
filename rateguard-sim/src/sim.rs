//! The event loop: a priority queue of events in virtual time.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use rateguard_core::boundary::{Action, Event, PeerId};
use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::Config;
use rateguard_core::node::{Node, SwimConfig};

use crate::invariant::{self, Happened, Invariant, View};
use crate::link::{Link, NodeIndex};

/// How often every node ticks: a quarter of the protocol period, so the ACK
/// timeout is checked in time.
pub const TICK: Nanos = 50_000_000;

pub fn peer_of(index: NodeIndex) -> PeerId {
    PeerId::new(index as u64)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Tick { generation: u64 },
    Request { key: u64 },
    Deliver { from: PeerId, bytes: Vec<u8> },
    Crash,
    Restart,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Bootstrap {
    Static,
    Seeds(Vec<NodeIndex>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pause {
    node: NodeIndex,
    from: Nanos,
    until: Nanos,
}

#[derive(Debug, Clone)]
struct Scheduled {
    at: Nanos,
    seq: u64,
    target: NodeIndex,
    kind: Kind,
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}
impl Eq for Scheduled {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub at: Nanos,
    pub node: NodeIndex,
    pub key: u64,
    pub decision: Decision,
}

/// A simulated cluster.
pub struct Sim<L: Link> {
    nodes: Vec<Node>,
    swim: SwimConfig,
    bootstrap: Bootstrap,
    generations: Vec<u64>,
    down: Vec<bool>,
    pauses: Vec<Pause>,
    queue: BinaryHeap<Scheduled>,
    link: L,
    now: Nanos,
    next_seq: u64,
    admissions: Vec<Admission>,
    config: Config,
    invariants: Vec<Box<dyn Invariant>>,
    events: u64,
    datagrams: Vec<u64>,
    largest_datagram: usize,
}

impl<L: Link> Sim<L> {
    /// A cluster where every node knows every other from the start.
    pub fn new(node_count: usize, config: Config, link: L) -> Self {
        Self::build(node_count, config, link, Bootstrap::Static)
    }

    /// A cluster where every node knows only the seeds and joins through
    /// them.
    pub fn with_seeds(node_count: usize, config: Config, link: L, seeds: &[NodeIndex]) -> Self {
        assert!(!seeds.is_empty(), "with no seed nobody can find anybody");
        assert!(
            seeds.iter().all(|&seed| seed < node_count),
            "a seed outside the cluster: {seeds:?}"
        );
        Self::build(node_count, config, link, Bootstrap::Seeds(seeds.to_vec()))
    }

    fn build(node_count: usize, config: Config, link: L, bootstrap: Bootstrap) -> Self {
        assert!(
            node_count > 0,
            "a cluster on nobody has nothing to simulate"
        );

        let swim = SwimConfig::default();
        assert_eq!(
            swim.protocol_period % TICK,
            0,
            "the tick must divide the protocol period, or rounds drift"
        );

        let mut sim = Self {
            nodes: Vec::with_capacity(node_count),
            swim,
            bootstrap,
            generations: vec![0; node_count],
            down: vec![false; node_count],
            pauses: Vec::new(),
            queue: BinaryHeap::new(),
            link,
            now: 0,
            next_seq: 0,
            admissions: Vec::new(),
            config,
            invariants: invariant::standard(),
            events: 0,
            datagrams: vec![0; node_count],
            largest_datagram: 0,
        };

        for index in 0..node_count {
            let node = sim.fresh_node(index);
            sim.nodes.push(node);
            sim.schedule(
                first_tick(index, node_count, swim.protocol_period),
                index,
                Kind::Tick { generation: 0 },
            );
        }
        sim
    }

    // A restarted node keeps its ID and address but nothing else: a new
    // incarnation 0, an empty table, the same bootstrap as at the start.
    fn fresh_node(&self, index: NodeIndex) -> Node {
        let node_count = self.generations.len();
        let seed = (self.generations[index] << 32) | index as u64;
        let mut node = Node::new(self.config, self.swim, peer_of(index), seed);
        match &self.bootstrap {
            Bootstrap::Static => {
                for other in (0..node_count).filter(|&other| other != index) {
                    node.introduce(peer_of(other));
                }
            }
            Bootstrap::Seeds(seeds) => {
                for &seed in seeds.iter().filter(|&&seed| seed != index) {
                    node.add_seed(peer_of(seed));
                }
            }
        }
        node
    }

    /// A stop-the-world pause: the node handles nothing in `[from, until)`,
    /// and everything that came for it meanwhile is handled at `until`, in
    /// the order it came, like datagrams waiting in a socket buffer.
    pub fn pause(&mut self, node: NodeIndex, from: Nanos, until: Nanos) {
        assert!(node < self.nodes.len(), "no such node: {node}");
        assert!(from < until, "an empty pause: {from}..{until}");
        self.pauses.push(Pause { node, from, until });
    }

    /// The node is down in `[down_at, up_at)`, and whatever is sent to it is
    /// lost. At `up_at` a fresh instance starts under the same ID.
    pub fn schedule_restart(&mut self, node: NodeIndex, down_at: Nanos, up_at: Nanos) {
        assert!(node < self.nodes.len(), "no such node: {node}");
        assert!(down_at < up_at, "a restart must take some time");
        self.schedule(down_at, node, Kind::Crash);
        self.schedule(up_at, node, Kind::Restart);
    }

    pub fn is_down(&self, node: NodeIndex) -> bool {
        self.down[node]
    }

    /// Datagrams the node sent, counted before the link decides their fate.
    pub fn datagrams_sent(&self, node: NodeIndex) -> u64 {
        self.datagrams[node]
    }

    /// The largest datagram any node sent, in bytes.
    pub fn largest_datagram(&self) -> usize {
        self.largest_datagram
    }

    pub fn now(&self) -> Nanos {
        self.now
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    pub fn node(&self, index: NodeIndex) -> &Node {
        &self.nodes[index]
    }

    pub fn admissions(&self) -> &[Admission] {
        &self.admissions
    }

    pub fn add_invariant(&mut self, invariant: impl Invariant + 'static) {
        self.invariants.push(Box::new(invariant));
    }

    pub fn schedule_request(&mut self, at: Nanos, node: NodeIndex, key: u64) {
        assert!(node < self.nodes.len(), "no such node: {node}");
        self.schedule(at, node, Kind::Request { key })
    }

    pub fn schedule_request_stream(
        &mut self,
        node: NodeIndex,
        key: u64,
        rate_per_sec: u32,
        from: Nanos,
        until: Nanos,
    ) {
        assert!(rate_per_sec > 0, "rate_per_sec must be > 0");
        let interval = 1_000_000_000u64.div_ceil(rate_per_sec as u64);

        let mut at = from;
        while at < until {
            self.schedule_request(at, node, key);
            at += interval;
        }
    }

    pub fn schedule_message(&mut self, at: Nanos, from: NodeIndex, to: NodeIndex, bytes: Vec<u8>) {
        assert!(to < self.nodes.len(), "no such node: {to}");
        self.schedule(
            at,
            to,
            Kind::Deliver {
                from: peer_of(from),
                bytes,
            },
        );
    }
    /// Runs every event up to and including `deadline`.
    pub fn run_until(&mut self, deadline: Nanos) {
        assert!(
            deadline >= self.now,
            "time doesn't run backwards in the simulator either: {deadline} < {}",
            self.now
        );

        while let Some(next) = self.queue.peek() {
            if next.at > deadline {
                break;
            }
            let event = self.queue.pop().expect("just peeked");
            if let Some(until) = self.paused_until(&event) {
                self.queue.push(Scheduled {
                    at: until,
                    seq: self.next_seq,
                    ..event
                });
                self.next_seq += 1;
                continue;
            }
            self.now = event.at;
            self.step(event);
        }
        self.now = deadline;
    }

    pub fn run_for(&mut self, duration: Nanos) {
        self.run_until(self.now + duration);
    }

    pub fn admitted_between(&self, from: Nanos, until: Nanos) -> usize {
        self.admissions
            .iter()
            .filter(|a| a.at >= from && a.at < until && a.decision == Decision::Allow)
            .count()
    }

    pub fn admitted_by(&self, node: NodeIndex) -> usize {
        self.admissions
            .iter()
            .filter(|a| a.node == node && a.decision == Decision::Allow)
            .count()
    }

    fn paused_until(&self, event: &Scheduled) -> Option<Nanos> {
        if matches!(event.kind, Kind::Crash | Kind::Restart) {
            return None;
        }
        self.pauses
            .iter()
            .find(|pause| {
                pause.node == event.target && (pause.from..pause.until).contains(&event.at)
            })
            .map(|pause| pause.until)
    }

    fn step(&mut self, event: Scheduled) {
        let now = self.now;
        let target = event.target;

        match event.kind {
            Kind::Crash => {
                self.down[target] = true;
                return;
            }
            Kind::Restart => {
                self.generations[target] += 1;
                self.nodes[target] = self.fresh_node(target);
                self.down[target] = false;
                let generation = self.generations[target];
                self.schedule(now, target, Kind::Tick { generation });
                self.check_invariants(target, &Happened::Restart);
                self.events += 1;
                return;
            }
            Kind::Tick { generation } if generation != self.generations[target] => return,
            _ if self.down[target] => return,
            _ => {}
        }

        let happened = match event.kind {
            Kind::Request { key } => {
                let decision = self.nodes[target].check(key, now);
                let admission = Admission {
                    at: now,
                    node: target,
                    key,
                    decision,
                };
                self.admissions.push(admission.clone());
                Happened::Admission(admission)
            }
            Kind::Tick { generation } => {
                self.dispatch(target, Event::Tick);
                self.schedule(now + TICK, target, Kind::Tick { generation });
                Happened::Tick
            }
            Kind::Deliver { from, bytes } => {
                self.dispatch(
                    target,
                    Event::MessageReceived {
                        from,
                        bytes: &bytes,
                    },
                );
                Happened::Delivery
            }
            Kind::Crash | Kind::Restart => unreachable!("handled above"),
        };

        self.check_invariants(target, &happened);
        self.events += 1;
    }

    fn check_invariants(&mut self, target: NodeIndex, happened: &Happened) {
        let Self {
            nodes,
            invariants,
            config,
            now,
            events,
            ..
        } = self;
        let view = View {
            event: *events,
            now: *now,
            target,
            happened,
            nodes: nodes.as_slice(),
            config,
        };

        for invariant in invariants.iter_mut() {
            if let Err(why) = invariant.check(&view) {
                panic!(
                    "invariant `{}` violated at event #{}, t = {} ns, node {target} after {happened:?}: {why}",
                    invariant.name(),
                    view.event,
                    view.now,
                );
            }
        }
    }

    fn dispatch(&mut self, source: NodeIndex, event: Event<'_>) {
        let Self {
            nodes,
            queue,
            link,
            now,
            next_seq,
            datagrams,
            largest_datagram,
            ..
        } = self;
        let now = *now;
        let node_count = nodes.len();

        for action in nodes[source].handle(event, now) {
            match action {
                Action::SendTo { peer, bytes } => {
                    datagrams[source] += 1;
                    *largest_datagram = (*largest_datagram).max(bytes.len());
                    let target = peer.get() as usize;
                    assert!(
                        target < node_count,
                        "node {source} addressed an unknown peer {}",
                        peer.get()
                    );
                    for at in link.fate(source, target, now, bytes.len()).arrivals() {
                        assert!(at >= now, "the link delivered into the past: {at} < {now}");

                        queue.push(Scheduled {
                            at,
                            seq: *next_seq,
                            target,
                            kind: Kind::Deliver {
                                from: peer_of(source),
                                bytes: bytes.clone(),
                            },
                        });
                        *next_seq += 1;
                    }
                }
            }
        }
    }

    fn schedule(&mut self, at: Nanos, target: NodeIndex, kind: Kind) {
        assert!(
            at >= self.now,
            "an event may not be scheduled into the past: {at} < {}",
            self.now
        );

        self.queue.push(Scheduled {
            at,
            seq: self.next_seq,
            target,
            kind,
        });
        self.next_seq += 1;
    }
}

fn first_tick(index: NodeIndex, node_count: usize, period: Nanos) -> Nanos {
    (index as u64 * period) / node_count as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::{Fate, NetConfig, PerfectLink, SeededLink};
    use rateguard_proto::Status;
    use std::cell::RefCell;
    use std::rc::Rc;

    const ONE_SEC: Nanos = 1_000_000_000;
    const ONE_MS: Nanos = 1_000_000;
    const KEY: u64 = 42;

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

    fn sim(node_count: usize) -> Sim<PerfectLink> {
        Sim::new(node_count, config(), PerfectLink::new(ONE_MS))
    }

    #[test]
    fn ticks_are_staggered_acros_the_cluster() {
        let period = 200 * ONE_MS;
        assert_eq!(first_tick(0, 5, period), 0);
        assert_eq!(first_tick(1, 5, period), 40_000_000);
        assert_eq!(first_tick(4, 5, period), 160_000_000);
        assert_eq!(first_tick(0, 1, period), 0);
    }

    #[test]
    fn an_hour_of_virtual_time_is_just_a_loop() {
        let mut s = sim(3);
        let an_hour = 3600 * ONE_SEC;
        s.schedule_request(an_hour - ONE_SEC, 1, KEY);

        s.run_until(an_hour);

        assert_eq!(s.now(), an_hour);
        assert_eq!(
            s.admissions().len(),
            1,
            "a request scheduled an hour out ,ust still be answered"
        );
        assert_eq!(s.admissions()[0].at, an_hour - ONE_SEC);
    }

    #[test]
    fn the_run_stops_at_the_deadline_and_keeps_the_rest() {
        let mut s = sim(1);
        s.schedule_request(ONE_SEC, 0, KEY);
        s.schedule_request(3 * ONE_SEC, 0, KEY);

        s.run_until(2 * ONE_SEC);
        assert_eq!(s.now(), 2 * ONE_SEC);
        assert_eq!(s.admissions().len(), 1, "the later request must still wait");

        s.run_for(2 * ONE_SEC);
        assert_eq!(s.admissions().len(), 2);
    }

    #[test]
    #[should_panic(expected = "time doesn't run backwards")]
    fn a_run_may_not_end_before_it_starts() {
        let mut s = sim(1);
        s.run_until(ONE_SEC);
        s.run_until(0);
    }

    #[test]
    fn nodes_do_not_shares_a_limiter() {
        let mut s = sim(2);
        s.schedule_request_stream(0, KEY, 4000, 0, ONE_SEC);
        s.schedule_request(ONE_SEC / 2, 1, KEY);

        s.run_until(ONE_SEC);

        assert_eq!(
            s.admissions()
                .iter()
                .filter(|a| a.node == 1)
                .map(|a| a.decision.clone())
                .collect::<Vec<_>>(),
            vec![Decision::Allow],
            "a node hammered next door must spend this node's budget"
        );
    }

    #[test]
    fn the_cluster_size_comes_from_the_number_of_nodes() {
        let window = 200 * ONE_MS;

        let mut alone = sim(1);
        alone.schedule_request_stream(0, KEY, 2000, 0, window);
        alone.run_until(window);

        let mut crowded = sim(2);
        crowded.schedule_request_stream(0, KEY, 2000, 0, window);
        crowded.run_until(window);

        let one = alone.admitted_between(0, window);
        let two = crowded.admitted_between(0, window);

        assert!(
            (100..=125).contains(&one),
            "alone node holds a cold key to R * alpha = 500/s, admitted {one} in 200 ms"
        );
        assert!(
            (50..=70).contains(&two),
            "two nodes halve, admitted {two} in 200 ms"
        );
    }

    #[test]
    fn sustained_demand_widens_the_share_over_virtual_seconds() {
        let mut s = sim(1);
        s.schedule_request_stream(0, KEY, 2000, 0, 10 * ONE_SEC);
        s.run_until(10 * ONE_SEC);

        let cold = s.admitted_between(0, 200 * ONE_MS);
        let hot = s.admitted_between(9 * ONE_SEC, 10 * ONE_SEC);

        assert!(
            cold * 5 < 700,
            "before any tick the key is cold: alpha share, {} per second",
            cold * 5
        );
        assert!(
            hot > 900,
            "ticks promoted it: the full share, {hot} per second"
        );
    }

    #[test]
    fn a_datagram_is_carried_to_the_node_and_shallowed() {
        let mut s = sim(2);
        s.schedule_message(ONE_MS, 1, 0, vec![0xde, 0xad]);
        s.schedule_request(2 * ONE_MS, 0, KEY);
        s.run_until(ONE_SEC);

        assert_eq!(s.admitted_by(0), 1, "an unparsed datagram changes nothing");
    }

    #[test]
    fn the_same_scenario_twice_gives_the_same_trace() {
        fn scenario() -> Vec<Admission> {
            let mut s = sim(5);
            for node in 0..5 {
                s.schedule_request_stream(node, KEY + node as u64 % 2, 700, 0, 3 * ONE_SEC);
            }
            s.run_until(3 * ONE_SEC);
            s.admissions().to_vec()
        }

        let first = scenario();
        let second = scenario();

        assert!(!first.is_empty());
        assert_eq!(
            first, second,
            "the simulator must be a pure function of its schedule"
        );
    }

    #[test]
    fn a_perfect_wire_keeps_everyone_alive() {
        let mut s = sim(5);
        s.run_until(30 * ONE_SEC);

        for index in 0..s.node_count() {
            let node = s.node(index);
            assert_eq!(node.cluster_size(), 5, "node {index}");
            for other in (0..5).filter(|&other| other != index) {
                assert_eq!(
                    node.members().status(peer_of(other)),
                    Some(Status::Alive),
                    "node {index} doubts node {other} on a wire that loses nothing"
                );
            }
        }
    }

    #[test]
    fn a_silent_cluster_falls_apart_into_loners() {
        let dead = NetConfig {
            loss: 1.0,
            ..NetConfig::perfect(ONE_MS)
        };
        let mut s = Sim::new(5, config(), SeededLink::new(1, dead));
        s.run_until(10 * ONE_SEC);

        for index in 0..s.node_count() {
            assert_eq!(s.node(index).cluster_size(), 1, "node {index}");
        }
    }

    struct Isolate {
        node: NodeIndex,
        from: Nanos,
        until: Nanos,
    }
    impl Isolate {
        fn forever(node: NodeIndex) -> Self {
            Self {
                node,
                from: 0,
                until: Nanos::MAX,
            }
        }
    }
    impl Link for Isolate {
        fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
            let cut = (self.from..self.until).contains(&now);
            if cut && (from == self.node || to == self.node) {
                Fate::Lost
            } else {
                Fate::Delivered(now + ONE_MS)
            }
        }
    }

    #[test]
    fn an_isolated_node_and_the_rest_bury_each_other() {
        let mut s = Sim::new(5, config(), Isolate::forever(4));
        s.run_until(10 * ONE_SEC);

        assert_eq!(s.node(4).cluster_size(), 1, "the isolated node is alone");
        for index in 0..4 {
            let node = s.node(index);
            assert_eq!(node.cluster_size(), 4, "node {index}");
            assert_eq!(node.members().status(peer_of(4)), Some(Status::Dead));
        }
    }

    fn everyone_sees_everyone(s: &Sim<impl Link>) -> Result<(), String> {
        for index in 0..s.node_count() {
            let node = s.node(index);
            for other in (0..s.node_count()).filter(|&other| other != index) {
                let status = node.members().status(peer_of(other));
                if status == Some(Status::Dead) || status.is_none() {
                    return Err(format!("node {index} holds node {other} as {status:?}"));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn a_false_suspicion_is_refuted_across_the_cluster() {
        // Cut off from everyone, so no helper can vouch for it either.
        let blackout = 1500 * ONE_MS;
        let mut s = Sim::new(
            5,
            config(),
            Isolate {
                node: 1,
                from: 0,
                until: blackout,
            },
        );
        s.run_until(blackout);
        assert_eq!(
            s.node(0).members().status(peer_of(1)),
            Some(Status::Suspect),
            "node 0 probed node 1 within the first circuit and heard nothing"
        );

        s.run_until(5 * ONE_SEC);
        let refuted = s.node(1).members().incarnation();
        assert!(refuted >= 1, "node 1 never refuted");
        for index in (0..5).filter(|&index| index != 1) {
            assert_eq!(
                s.node(index).members().update_about(peer_of(1)),
                Some(rateguard_proto::Update {
                    member: 1,
                    incarnation: refuted,
                    status: Status::Alive,
                }),
                "node {index} missed the refutation"
            );
        }
    }

    #[test]
    fn a_buried_node_comes_back_once_it_can_talk() {
        let heal = 10 * ONE_SEC;
        let mut s = Sim::new(
            5,
            config(),
            Isolate {
                node: 4,
                from: 0,
                until: heal,
            },
        );
        s.run_until(heal);
        assert_eq!(s.node(0).members().status(peer_of(4)), Some(Status::Dead));
        assert_eq!(s.node(4).cluster_size(), 1);

        s.run_until(heal + 3 * ONE_SEC);
        everyone_sees_everyone(&s).unwrap();
        for index in 0..5 {
            assert_eq!(s.node(index).cluster_size(), 5, "node {index}");
        }
    }

    #[test]
    fn a_node_gone_for_good_is_forgotten() {
        let swim = SwimConfig::default();
        let mut s = Sim::new(5, config(), Isolate::forever(4));
        s.run_until(swim.suspicion_timeout + swim.tombstone_ttl + 10 * ONE_SEC);

        for index in 0..4 {
            let members = s.node(index).members();
            assert_eq!(members.status(peer_of(4)), None, "node {index}");
            assert_eq!(members.dead().count(), 0, "node {index}");
        }
        assert_eq!(s.node(4).members().dead().count(), 0);
    }

    #[test]
    fn a_broken_link_between_two_nodes_raises_no_suspicion() {
        struct CutPair;
        impl Link for CutPair {
            fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
                if (from, to) == (0, 1) || (from, to) == (1, 0) {
                    Fate::Lost
                } else {
                    Fate::Delivered(now + ONE_MS)
                }
            }
        }

        let mut s = Sim::new(5, config(), CutPair);
        for step in 1..=300 {
            s.run_until(step * 100 * ONE_MS);
            for index in 0..5 {
                for other in (0..5).filter(|&other| other != index) {
                    assert_eq!(
                        s.node(index).members().status(peer_of(other)),
                        Some(Status::Alive),
                        "at {} ms node {index} doubts node {other}: PING-REQ must vouch for it",
                        step * 100
                    );
                }
            }
        }
    }

    #[test]
    fn a_cluster_assembles_through_one_seed() {
        let mut s = Sim::with_seeds(5, config(), PerfectLink::new(ONE_MS), &[0]);
        assert_eq!(s.node(3).cluster_size(), 1, "nobody knows anybody yet");

        s.run_until(3 * ONE_SEC);
        everyone_sees_everyone(&s).unwrap();
    }

    #[test]
    fn a_larger_cluster_assembles_through_one_seed() {
        let mut s = Sim::with_seeds(20, config(), PerfectLink::new(ONE_MS), &[0]);
        s.run_until(10 * ONE_SEC);
        everyone_sees_everyone(&s).unwrap();
    }

    #[test]
    fn a_split_longer_than_the_tombstones_heals_through_the_seed() {
        let swim = SwimConfig::default();
        let cut = 5 * ONE_SEC;
        let heal = cut + swim.suspicion_timeout + swim.tombstone_ttl + 10 * ONE_SEC;
        let mut s = Sim::with_seeds(
            5,
            config(),
            Isolate {
                node: 4,
                from: cut,
                until: heal,
            },
            &[0],
        );

        s.run_until(cut);
        everyone_sees_everyone(&s).unwrap();

        s.run_until(heal);
        assert_eq!(
            s.node(0).members().status(peer_of(4)),
            None,
            "node 4 is forgotten, so reconnect alone could never bring it back"
        );

        s.run_until(heal + 5 * ONE_SEC);
        everyone_sees_everyone(&s).unwrap();
    }

    #[test]
    fn a_paused_node_handles_what_came_once_it_wakes() {
        let mut s = sim(2);
        s.pause(0, ONE_SEC / 2, 3 * ONE_SEC / 2);
        s.schedule_request(ONE_SEC, 0, KEY);
        s.schedule_request(ONE_SEC + 1, 0, KEY + 1);
        s.run_until(2 * ONE_SEC);

        let late: Vec<(Nanos, u64)> = s.admissions().iter().map(|a| (a.at, a.key)).collect();
        assert_eq!(
            late,
            [(3 * ONE_SEC / 2, KEY), (3 * ONE_SEC / 2, KEY + 1)],
            "deferred, not lost, and in the order they came"
        );
    }

    #[test]
    fn a_crashed_node_loses_what_comes_and_restarts_fresh() {
        let mut s = sim(2);
        s.schedule_restart(1, ONE_SEC, 2 * ONE_SEC);
        s.schedule_request(3 * ONE_SEC / 2, 1, KEY);

        s.run_until(3 * ONE_SEC / 2);
        assert!(s.is_down(1));
        s.run_until(2 * ONE_SEC);
        assert!(!s.is_down(1));
        assert!(
            s.admissions().is_empty(),
            "a request to a dead process is lost"
        );
        assert_eq!(s.node(1).cluster_size(), 2, "the bootstrap is replayed");
    }

    #[test]
    fn a_quick_restart_leaves_one_tick_chain() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut s = sim(1);
        s.add_invariant(Recorder(seen.clone()));
        s.schedule_restart(0, ONE_SEC, ONE_SEC + 10 * ONE_MS);
        s.run_until(2 * ONE_SEC);

        let ticks = seen
            .borrow()
            .iter()
            .filter(|(_, h)| *h == Happened::Tick)
            .count();
        assert_eq!(
            ticks,
            20 + 20,
            "0..950 ms before the crash, 1010..1960 ms after: the old chain must stop"
        );
    }

    struct Recorder(Rc<RefCell<Vec<(u64, Happened)>>>);
    impl Invariant for Recorder {
        fn name(&self) -> &'static str {
            "recorder"
        }

        fn check(&mut self, view: &View<'_>) -> Result<(), String> {
            self.0
                .borrow_mut()
                .push((view.event, view.happened.clone()));
            Ok(())
        }
    }

    struct AlwaysFails;
    impl Invariant for AlwaysFails {
        fn name(&self) -> &'static str {
            "always-fails"
        }

        fn check(&mut self, _: &View<'_>) -> Result<(), String> {
            Err("by design".into())
        }
    }

    #[test]
    fn invariants_are_checked_after_every_event() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut s = sim(1);
        s.add_invariant(Recorder(seen.clone()));

        s.schedule_request(ONE_MS, 0, KEY);
        s.schedule_request(2 * ONE_MS, 0, KEY);
        s.schedule_message(3 * ONE_MS, 0, 0, vec![0xde, 0xad]);
        s.run_until(ONE_SEC);

        let seen = seen.borrow();
        let ticks = seen.iter().filter(|(_, h)| *h == Happened::Tick).count();
        let deliveries = seen
            .iter()
            .filter(|(_, h)| *h == Happened::Delivery)
            .count();
        let admissions = seen
            .iter()
            .filter(|(_, h)| matches!(h, Happened::Admission(_)))
            .count();

        assert_eq!(ticks, 21, "ticks at 0, 50, ..., 1000 ms");
        assert_eq!(deliveries, 1);
        assert_eq!(admissions, 2);
        assert!(
            seen.iter().map(|(n, _)| *n).eq(0..seen.len() as u64),
            "event numbers must count every step without gaps"
        );
    }

    #[test]
    fn every_probe_on_a_perfect_wire_is_answered() {
        let mut s = sim(5);
        s.run_until(ONE_SEC + 10 * ONE_MS);

        for index in 0..s.node_count() {
            assert_eq!(
                s.node(index).awaiting_ack(),
                None,
                "node {index} pinged at most 50 ms ago, the round trip is 2 ms"
            );
        }
    }

    #[test]
    fn every_probe_on_a_dead_wire_hangs() {
        let dead = NetConfig {
            loss: 1.0,
            ..NetConfig::perfect(ONE_MS)
        };
        let mut s = Sim::new(5, config(), SeededLink::new(1, dead));
        s.run_until(ONE_SEC + 10 * ONE_MS);

        for index in 0..s.node_count() {
            assert!(
                s.node(index).awaiting_ack().is_some(),
                "node {index} heard back over a wire that loses everything"
            );
        }
    }

    #[test]
    #[should_panic(expected = "invariant `always-fails` violated at event #0")]
    fn a_violation_stops_the_run_at_the_offending_event() {
        let mut s = sim(1);
        s.add_invariant(AlwaysFails);
        s.run_until(ONE_SEC);
    }
}
