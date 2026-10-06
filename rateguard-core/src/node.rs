use rateguard_proto::{self as proto, Message, Status, Update};

use crate::{
    boundary::{Action, Event, PeerId},
    gcra::{Decision, Nanos},
    gossip::{self, Gossip},
    limiter::{Config, Limiter},
    member::{Applied, MemberTable},
    membership::{Change, Membership},
    rng::Rng,
};

const ONE_MS: Nanos = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    pub protocol_period: Nanos,
    pub suspicion_timeout: Nanos,
}
impl Default for Timing {
    // The suspicion outlasts the 5 s GC pause of the chaos scenarios, with
    // room left for the paused node to hear of it and refute.
    fn default() -> Self {
        Self {
            protocol_period: 200 * ONE_MS,
            suspicion_timeout: 6_000 * ONE_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Probe {
    target: PeerId,
    seq: u32,
}

pub struct Node {
    limiter: Limiter,
    members: MemberTable,
    gossip: Gossip,
    timing: Timing,
    rng: Rng,
    order: Vec<PeerId>,
    next: usize,
    seq: u32,
    probe: Option<Probe>,
    next_round_at: Nanos,
    last_round: Option<Nanos>,
    last_now: Nanos,
    actions: Vec<Action>,
}
impl Node {
    pub fn new(config: Config, timing: Timing, local: PeerId, seed: u64) -> Self {
        assert!(timing.protocol_period > 0, "protocol_period must be > 0");
        assert!(
            timing.suspicion_timeout >= timing.protocol_period,
            "a suspicion shorter than a protocol period expires before anyone can refute it"
        );

        Self {
            limiter: Limiter::new(config),
            members: MemberTable::new(local),
            gossip: Gossip::new(),
            timing,
            rng: Rng::new(seed),
            order: Vec::new(),
            next: 0,
            seq: 0,
            probe: None,
            next_round_at: 0,
            last_round: None,
            last_now: 0,
            actions: Vec::new(),
        }
    }

    pub fn introduce(&mut self, peer: PeerId) {
        assert_ne!(peer, self.members.local(), "a node is not its own peer");
        self.members.apply(
            Update {
                member: peer.get(),
                incarnation: 0,
                status: Status::Alive,
            },
            self.last_now,
        );
    }

    pub fn cluster_size(&self) -> usize {
        self.members.cluster_size()
    }

    pub fn members(&self) -> &MemberTable {
        &self.members
    }

    pub fn awaiting_ack(&self) -> Option<PeerId> {
        self.probe.map(|probe| probe.target)
    }

    pub fn last_round(&self) -> Option<Nanos> {
        self.last_round
    }

    pub fn limiter(&self) -> &Limiter {
        &self.limiter
    }

    pub fn check(&mut self, key: u64, now: Nanos) -> Decision {
        self.advance(now);
        self.limiter.check(key, now, self.members.cluster_size())
    }

    // A tick may come more often than the protocol period: the round runs
    // once per period, the timeouts are checked on every tick. For rounds to
    // keep their period exactly, the tick interval should divide it.
    pub fn handle(&mut self, event: Event<'_>, now: Nanos) -> &[Action] {
        self.advance(now);
        self.actions.clear();

        match event {
            Event::Tick => {
                for dead in self
                    .members
                    .expire_suspects(now, self.timing.suspicion_timeout)
                {
                    self.gossip.push(dead);
                }
                if now >= self.next_round_at {
                    self.round(now);
                }
            }
            Event::MessageReceived { from, bytes } => self.receive(from, bytes, now),
        }

        &self.actions
    }

    fn round(&mut self, now: Nanos) {
        self.next_round_at = now + self.timing.protocol_period;
        self.last_round = Some(now);

        if let Some(missed) = self.probe.take()
            && let Some(suspicion) = self.members.suspect(missed.target, now)
        {
            self.gossip.push(suspicion);
        }
        self.limiter.tick(now, self.members.cluster_size());
        self.probe_next();
    }

    fn probe_next(&mut self) {
        self.follow_changes();

        let Some(target) = self.next_target() else {
            return;
        };
        self.seq = self.seq.wrapping_add(1);
        self.probe = Some(Probe {
            target,
            seq: self.seq,
        });
        let updates = self.outgoing(target);
        self.send(
            target,
            &Message::Ping {
                seq: self.seq,
                updates,
            },
        );
    }

    // SWIM inserts a newcomer at a random place among the peers still to be
    // probed this round, so it is probed within the round it joined.
    fn follow_changes(&mut self) {
        for change in self.members.drain_changes() {
            if let Change::Joined(peer) = change
                && !self.order[self.next..].contains(&peer)
            {
                let remaining = (self.order.len() - self.next) as u64;
                let at = self.next + self.rng.up_to(remaining) as usize;
                self.order.insert(at, peer);
            }
        }
    }

    fn next_target(&mut self) -> Option<PeerId> {
        if let Some(target) = self.take_live() {
            return Some(target);
        }
        self.order.clear();
        self.order.extend_from_slice(self.members.peers());
        self.rng.shuffle(&mut self.order);
        self.next = 0;
        self.take_live()
    }

    fn take_live(&mut self) -> Option<PeerId> {
        while let Some(&peer) = self.order.get(self.next) {
            self.next += 1;
            if self.members.peers().binary_search(&peer).is_ok() {
                return Some(peer);
            }
        }
        None
    }

    // News is applied before the reply is built: a suspect answers the PING
    // that told it of the suspicion with an ACK that already refutes it.
    fn receive(&mut self, from: PeerId, bytes: &[u8], now: Nanos) {
        let Ok(message) = proto::decode(bytes) else {
            return;
        };
        for &update in message.updates() {
            self.learn(update, now);
        }
        match message {
            Message::Ping { seq, .. } => {
                let updates = self.outgoing(from);
                self.send(from, &Message::Ack { seq, updates });
            }
            Message::Ack { seq, .. } => {
                if self.probe == Some(Probe { target: from, seq }) {
                    self.probe = None;
                }
            }
            Message::PingReq { .. } => {}
        }
    }

    fn learn(&mut self, update: Update, now: Nanos) {
        match self.members.apply(update, now) {
            Applied::Ignored => {}
            Applied::Accepted { .. } => {
                self.gossip.push(update);
            }
            Applied::Refuted(alive) => {
                self.gossip.push(alive);
            }
        }
    }

    // Whoever we hold as suspect or dead hears it from us on every message,
    // not only while the news is in the buffer: it is the one node that can
    // refute it, and a death nobody repeats to the dead is never undone.
    fn outgoing(&mut self, to: PeerId) -> Vec<Update> {
        let accusation = self
            .members
            .update_about(to)
            .filter(|update| update.status != Status::Alive);
        let room = proto::MAX_UPDATES - usize::from(accusation.is_some());
        let limit = gossip::retransmit_limit(self.members.cluster_size());

        let mut updates = self.gossip.take(room, limit);
        if let Some(accusation) = accusation
            && !updates.contains(&accusation)
        {
            updates.push(accusation);
        }
        updates
    }

    fn send(&mut self, peer: PeerId, message: &Message) {
        let bytes = proto::encode(message).expect("MAX_UPDATES updates always fit a datagram");
        self.actions.push(Action::SendTo { peer, bytes });
    }

    fn advance(&mut self, now: Nanos) {
        assert!(
            now >= self.last_now,
            "time must not run backwards: {now} < {}",
            self.last_now
        );
        self.last_now = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Status::{Alive, Dead, Suspect};
    use std::collections::BTreeSet;

    const ONE_SEC: Nanos = 1_000_000_000;
    const KEY: u64 = 42;
    const LOCAL: PeerId = PeerId::new(0);
    const PERIOD: Nanos = 200 * ONE_MS;
    const TICK: Nanos = 50 * ONE_MS;
    const TIMING: Timing = Timing {
        protocol_period: PERIOD,
        suspicion_timeout: 5 * PERIOD,
    };

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

    fn node() -> Node {
        Node::new(config(), TIMING, LOCAL, 1)
    }

    fn node_with(peers: impl IntoIterator<Item = u64>) -> Node {
        let mut n = node();
        for peer in peers {
            n.introduce(PeerId::new(peer));
        }
        n
    }

    fn measured_interval(node: &mut Node, now: Nanos) -> Nanos {
        for _ in 0..config().burst {
            assert_eq!(node.check(KEY, now), Decision::Allow);
        }
        let Decision::Deny { retry_at } = node.check(KEY, now) else {
            panic!("burst must be exhausted");
        };
        retry_at - now
    }

    fn sent(actions: &[Action]) -> Vec<(PeerId, Message)> {
        actions
            .iter()
            .map(|Action::SendTo { peer, bytes }| (*peer, proto::decode(bytes).unwrap()))
            .collect()
    }

    fn deliver(node: &mut Node, from: u64, message: Message, now: Nanos) -> Vec<(PeerId, Message)> {
        let bytes = proto::encode(&message).unwrap();
        sent(node.handle(
            Event::MessageReceived {
                from: PeerId::new(from),
                bytes: &bytes,
            },
            now,
        ))
    }

    fn ack(seq: u32) -> Message {
        Message::Ack {
            seq,
            updates: Vec::new(),
        }
    }

    // Runs round `k` of the protocol, which starts at k * PERIOD.
    fn round(node: &mut Node, k: u64) -> (PeerId, u32) {
        let (target, seq, _) = round_with_news(node, k);
        (target, seq)
    }

    fn round_with_news(node: &mut Node, k: u64) -> (PeerId, u32, Vec<Update>) {
        match sent(node.handle(Event::Tick, k * PERIOD)).as_slice() {
            [(peer, Message::Ping { seq, updates })] => (*peer, *seq, updates.clone()),
            other => panic!("a round must send exactly one PING, sent {other:?}"),
        }
    }

    fn news(member: u64, incarnation: u32, status: Status) -> Update {
        Update {
            member,
            incarnation,
            status,
        }
    }

    fn answered_round(node: &mut Node, k: u64) -> PeerId {
        let (target, seq) = round(node, k);
        deliver(node, target.get(), ack(seq), k * PERIOD + ONE_MS);
        target
    }

    #[test]
    fn a_freash_node_is_a_cluster_of_one() {
        assert_eq!(node().cluster_size(), 1);
    }

    #[test]
    fn check_delegates_to_the_limiter_at_the_cold_share() {
        assert_eq!(measured_interval(&mut node(), 0), 2_000_000);
    }

    #[test]
    fn the_cluster_size_comes_from_the_members() {
        let mut n = node_with([1]);
        assert_eq!(n.cluster_size(), 2);
        assert_eq!(measured_interval(&mut n, 0), 4_000_000);
    }

    #[test]
    #[should_panic(expected = "not its own peer")]
    fn a_node_cannot_introduce_itself() {
        node().introduce(LOCAL);
    }

    #[test]
    #[should_panic(expected = "expires before anyone can refute it")]
    fn a_suspicion_shorter_than_a_period_is_rejected() {
        let timing = Timing {
            protocol_period: PERIOD,
            suspicion_timeout: PERIOD - 1,
        };
        Node::new(config(), timing, LOCAL, 1);
    }

    #[test]
    fn a_tick_drives_promotion() {
        let mut n = node();
        for _ in 0..5000 {
            n.check(KEY, 0);
        }

        assert_eq!(
            measured_interval(&mut n, 100_000_000),
            2_000_000,
            "promotion belongs to the tick, not the check()"
        );

        n.handle(Event::Tick, ONE_SEC);
        assert_eq!(
            measured_interval(&mut n, 2 * ONE_SEC),
            1_000_000,
            "a hot key gets the full share, not the alphs one"
        );
    }

    #[test]
    fn a_lone_node_pings_nobody() {
        let mut n = node();
        assert!(n.handle(Event::Tick, 0).is_empty());
        assert_eq!(n.awaiting_ack(), None);
    }

    #[test]
    fn a_round_runs_once_per_period_however_often_it_ticks() {
        let mut n = node_with([1]);
        let mut pings_at = Vec::new();
        for k in 0..=8 {
            let now = k * TICK;
            if !n.handle(Event::Tick, now).is_empty() {
                pings_at.push(now);
            }
        }
        assert_eq!(pings_at, [0, PERIOD, 2 * PERIOD]);
        assert_eq!(n.last_round(), Some(2 * PERIOD));
    }

    #[test]
    fn a_round_pings_one_peer_and_waits_for_it() {
        let mut n = node_with([1, 2]);
        let (target, _) = round(&mut n, 0);
        assert!([PeerId::new(1), PeerId::new(2)].contains(&target));
        assert_eq!(n.awaiting_ack(), Some(target));
    }

    #[test]
    fn every_peer_is_probed_once_per_round() {
        let peers = [1, 2, 3, 4, 5];
        let mut n = node_with(peers);
        let expected: BTreeSet<PeerId> = peers.into_iter().map(PeerId::new).collect();

        let mut circuits = BTreeSet::new();
        for circuit in 0..20 {
            let order: Vec<PeerId> = (0..peers.len() as u64)
                .map(|i| answered_round(&mut n, circuit * peers.len() as u64 + i))
                .collect();
            assert_eq!(
                order.iter().copied().collect::<BTreeSet<_>>(),
                expected,
                "circuit {circuit} went {order:?}"
            );
            circuits.insert(order);
        }
        assert!(
            circuits.len() > 1,
            "the order is reshuffled between circuits, or failures correlate"
        );
    }

    #[test]
    fn a_newcomer_is_probed_within_the_current_circuit() {
        let mut n = node_with([1, 2, 3]);
        let first = answered_round(&mut n, 0);
        n.introduce(PeerId::new(9));

        let rest: Vec<PeerId> = (1..4).map(|k| answered_round(&mut n, k)).collect();
        assert!(rest.contains(&PeerId::new(9)), "{rest:?}");
        assert!(
            !rest.contains(&first),
            "the circuit must not restart: {rest:?}"
        );
    }

    #[test]
    fn a_ping_is_answered_with_an_ack_of_the_same_seq() {
        let mut n = node();
        let ping = Message::Ping {
            seq: 77,
            updates: Vec::new(),
        };
        assert_eq!(
            deliver(&mut n, 5, ping, 0),
            [(PeerId::new(5), ack(77))],
            "liveness is answered even to a stranger"
        );
    }

    #[test]
    fn the_matching_ack_ends_the_wait() {
        let mut n = node_with([1]);
        let (target, seq) = round(&mut n, 0);

        assert!(deliver(&mut n, target.get(), ack(seq), 1).is_empty());
        assert_eq!(n.awaiting_ack(), None);
    }

    #[test]
    fn a_stray_ack_does_not_end_the_wait() {
        let mut n = node_with([1, 2]);
        let (target, seq) = round(&mut n, 0);
        let other = if target == PeerId::new(1) { 2 } else { 1 };

        deliver(&mut n, other, ack(seq), 1);
        assert_eq!(n.awaiting_ack(), Some(target), "an ACK from someone else");
        deliver(&mut n, target.get(), ack(seq.wrapping_sub(1)), 1);
        assert_eq!(n.awaiting_ack(), Some(target), "an ACK to an older PING");
    }

    #[test]
    fn a_late_ack_does_not_end_the_next_wait() {
        let mut n = node_with([1]);
        let (_, old) = round(&mut n, 0);
        let (target, new) = round(&mut n, 1);
        assert_ne!(old, new, "every PING has its own seq");

        deliver(&mut n, target.get(), ack(old), PERIOD + 1);
        assert_eq!(n.awaiting_ack(), Some(target));
    }

    #[test]
    fn an_answered_probe_raises_no_suspicion() {
        let mut n = node_with([1]);
        for k in 0..10 {
            answered_round(&mut n, k);
        }
        assert_eq!(n.members().status(PeerId::new(1)), Some(Alive));
    }

    #[test]
    fn a_probe_unanswered_by_the_next_round_makes_a_suspect() {
        let mut n = node_with([1, 2]);
        let (target, _) = round(&mut n, 0);

        n.handle(Event::Tick, TICK);
        assert_eq!(
            n.members().status(target),
            Some(Alive),
            "the ACK still has until the end of the period"
        );

        round(&mut n, 1);
        assert_eq!(n.members().status(target), Some(Suspect));
        assert_eq!(n.cluster_size(), 3, "a suspect still counts");
    }

    #[test]
    fn an_unrefuted_suspect_is_declared_dead_and_leaves() {
        let mut n = node_with([1]);
        let peer = PeerId::new(1);
        round(&mut n, 0);
        round(&mut n, 1);
        assert_eq!(n.members().status(peer), Some(Suspect));
        let suspected_at = PERIOD;

        let mut now = suspected_at;
        while now < suspected_at + TIMING.suspicion_timeout - TICK {
            now += TICK;
            n.handle(Event::Tick, now);
            assert_eq!(n.members().status(peer), Some(Suspect), "at {now}");
        }

        n.handle(Event::Tick, suspected_at + TIMING.suspicion_timeout);
        assert_eq!(n.members().status(peer), Some(Dead));
        assert_eq!(n.cluster_size(), 1);
    }

    #[test]
    fn missing_again_does_not_restart_the_suspicion() {
        let mut n = node_with([1]);
        round(&mut n, 0);
        round(&mut n, 1);
        for k in 2..=5 {
            round(&mut n, k);
        }
        n.handle(Event::Tick, PERIOD + TIMING.suspicion_timeout);
        assert_eq!(
            n.members().status(PeerId::new(1)),
            Some(Dead),
            "the timer runs from the first suspicion, not the latest miss"
        );
    }

    #[test]
    fn a_suspect_is_still_probed() {
        let mut n = node_with([1]);
        round(&mut n, 0);
        let (target, _) = round(&mut n, 1);
        assert_eq!(n.members().status(target), Some(Suspect));
        assert_eq!(target, PeerId::new(1), "only an answer can save it");
    }

    #[test]
    fn the_dead_are_no_longer_probed() {
        let mut n = node_with([1, 2]);
        let mut k = 0;
        while n.members().status(PeerId::new(1)) != Some(Dead) {
            let (target, seq) = round(&mut n, k);
            if target == PeerId::new(2) {
                deliver(&mut n, 2, ack(seq), k * PERIOD + ONE_MS);
            }
            k += 1;
            assert!(k < 100, "peer 1 never died");
        }

        for _ in 0..10 {
            assert_eq!(
                answered_round(&mut n, k),
                PeerId::new(2),
                "a dead peer was probed"
            );
            k += 1;
        }
    }

    #[test]
    fn a_suspicion_rides_on_the_next_ping() {
        let mut n = node_with([1]);
        round(&mut n, 0);
        let (target, _, updates) = round_with_news(&mut n, 1);
        assert_eq!(target, PeerId::new(1));
        assert_eq!(
            updates,
            [news(1, 0, Suspect)],
            "the suspect itself must hear of it to refute"
        );
    }

    #[test]
    fn a_suspect_refutes_in_the_ack_to_the_ping_that_accused_it() {
        let mut n = node();
        let ping = Message::Ping {
            seq: 5,
            updates: vec![news(0, 0, Suspect)],
        };
        assert_eq!(
            deliver(&mut n, 3, ping, 0),
            [(
                PeerId::new(3),
                Message::Ack {
                    seq: 5,
                    updates: vec![news(0, 1, Alive)]
                }
            )]
        );
        assert_eq!(n.members().incarnation(), 1);
    }

    #[test]
    fn a_refutation_lifts_the_suspicion() {
        let mut n = node_with([1]);
        round(&mut n, 0);
        let (_, seq) = round(&mut n, 1);
        assert_eq!(n.members().status(PeerId::new(1)), Some(Suspect));

        let refuting = Message::Ack {
            seq,
            updates: vec![news(1, 1, Alive)],
        };
        deliver(&mut n, 1, refuting, PERIOD + ONE_MS);
        assert_eq!(n.members().status(PeerId::new(1)), Some(Alive));
        assert_eq!(n.awaiting_ack(), None);
        n.handle(Event::Tick, PERIOD + TIMING.suspicion_timeout);
        assert_eq!(
            n.members().status(PeerId::new(1)),
            Some(Alive),
            "a lifted suspicion does not expire"
        );
    }

    #[test]
    fn news_heard_from_anyone_is_learned_and_passed_on() {
        let mut n = node_with([1]);
        let stray = Message::Ack {
            seq: 999,
            updates: vec![news(7, 0, Alive)],
        };
        deliver(&mut n, 1, stray, 0);
        assert_eq!(n.cluster_size(), 3, "a peer learned by gossip joins");

        let (_, _, updates) = round_with_news(&mut n, 0);
        assert_eq!(updates, [news(7, 0, Alive)]);
    }

    #[test]
    fn news_is_passed_on_a_limited_number_of_times() {
        let mut n = node_with([1]);
        let death = news(7, 0, Dead);
        let carrying = |updates: Vec<Update>| Message::Ack { seq: 999, updates };
        deliver(&mut n, 1, carrying(vec![death]), 0);
        deliver(&mut n, 1, carrying(vec![death]), 0);

        let mut times = 0;
        for k in 0..20 {
            let (target, seq, updates) = round_with_news(&mut n, k);
            times += updates.iter().filter(|&&u| u == death).count();
            deliver(&mut n, target.get(), ack(seq), k * PERIOD + ONE_MS);
        }
        assert_eq!(
            times,
            gossip::retransmit_limit(2) as usize,
            "an echo of known news must not restart the count"
        );
    }

    #[test]
    fn a_death_by_timeout_is_announced() {
        let mut n = node_with([1, 2]);
        let mut k = 0;
        loop {
            let (target, seq, updates) = round_with_news(&mut n, k);
            if updates.contains(&news(1, 0, Dead)) {
                break;
            }
            if target == PeerId::new(2) {
                deliver(&mut n, 2, ack(seq), k * PERIOD + ONE_MS);
            }
            k += 1;
            assert!(k < 100, "the death of peer 1 was never announced");
        }
    }

    #[test]
    fn garbage_is_dropped_without_a_reply() {
        let mut n = node_with([7]);
        let bytes = [0xde, 0xad, 0xbe, 0xef];

        assert!(
            n.handle(
                Event::MessageReceived {
                    from: PeerId::new(7),
                    bytes: &bytes
                },
                0
            )
            .is_empty()
        );
    }

    #[test]
    fn the_same_seed_probes_in_the_same_order() {
        let order = |seed| {
            let mut n = Node::new(config(), TIMING, LOCAL, seed);
            for peer in 1..=6 {
                n.introduce(PeerId::new(peer));
            }
            (0..12)
                .map(|k| answered_round(&mut n, k))
                .collect::<Vec<_>>()
        };
        assert_eq!(order(5), order(5));
        assert_ne!(order(5), order(6));
    }

    #[test]
    #[should_panic(expected = "time must not run backwards")]
    fn an_event_may_not_arrive_in_the_past() {
        let mut n = node();
        n.handle(Event::Tick, ONE_SEC);
        n.handle(Event::Tick, 0);
    }

    #[test]
    #[should_panic(expected = "time must not run backwards")]
    fn a_request_may_not_arrive_in_the_past() {
        let mut n = node();
        n.handle(Event::Tick, ONE_SEC);
        n.check(KEY, 0);
    }
}
