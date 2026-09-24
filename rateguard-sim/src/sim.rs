use std::cmp::Ordering;
use std::collections::BinaryHeap;

use rateguard_core::boundary::{Action, Event, PeerId};
use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::Config;
use rateguard_core::node::Node;

use crate::link::{Link, NodeIndex};

pub const PROTOCOL_PERIOD: Nanos = 200_000_000;

pub fn peer_of(index: NodeIndex) -> PeerId {
    PeerId::new(index as u64)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Tick,
    Request { key: u64 },
    Deliver { from: PeerId, bytes: Vec<u8> },
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

pub struct Sim<L: Link> {
    nodes: Vec<Node>,
    queue: BinaryHeap<Scheduled>,
    link: L,
    now: Nanos,
    next_seq: u64,
    admissions: Vec<Admission>,
}

impl<L: Link> Sim<L> {
    pub fn new(node_count: usize, config: Config, link: L) -> Self {
        assert!(
            node_count > 0,
            "a cluster on nobody has nothing to simulate"
        );

        let mut sim = Self {
            nodes: Vec::with_capacity(node_count),
            queue: BinaryHeap::new(),
            link,
            now: 0,
            next_seq: 0,
            admissions: Vec::new(),
        };

        for index in 0..node_count {
            let mut node = Node::new(config);
            node.set_cluster_size(node_count);
            sim.nodes.push(node);
            sim.schedule(first_tick(index, node_count), index, Kind::Tick);
        }
        sim
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

    pub fn admissions(&self) -> &[Admission] {
        &self.admissions
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

    fn step(&mut self, event: Scheduled) {
        let now = self.now;

        match event.kind {
            Kind::Request { key } => {
                let decision = self.nodes[event.target].check(key, now);
                self.admissions.push(Admission {
                    at: now,
                    node: event.target,
                    key,
                    decision,
                });
            }
            Kind::Tick => {
                self.dispatch(event.target, Event::Tick);
                self.schedule(now + PROTOCOL_PERIOD, event.target, Kind::Tick);
            }
            Kind::Deliver { from, bytes } => {
                self.dispatch(
                    event.target,
                    Event::MessageReceived {
                        from,
                        bytes: &bytes,
                    },
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
            ..
        } = self;
        let now = *now;
        let node_count = nodes.len();

        for action in nodes[source].handle(event, now) {
            match action {
                Action::SendTo { peer, bytes } => {
                    let target = peer.get() as usize;
                    assert!(
                        target < node_count,
                        "node {source} addressed an unknown peer {}",
                        peer.get()
                    );
                    let Some(at) = link.deliver_at(source, target, now, bytes.len()) else {
                        continue;
                    };
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

fn first_tick(index: NodeIndex, node_count: usize) -> Nanos {
    (index as u64 * PROTOCOL_PERIOD) / node_count as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::PerfectLink;

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
        assert_eq!(first_tick(0, 5), 0);
        assert_eq!(first_tick(1, 5), 40_000_000);
        assert_eq!(first_tick(4, 5), 160_000_000);
        assert_eq!(first_tick(0, 1), 0);
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
}
