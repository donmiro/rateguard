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
            sim.schedule(Self::first_tick(index, node_count), index, Kind::Tick);
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

    pub fn scheduled_request_stream(
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

    fn first_tick(index: NodeIndex, node_count: usize) -> Nanos {
        (index as u64 * PROTOCOL_PERIOD) / node_count as u64
    }
}
