//! One cluster member: the limiter and the membership protocol behind one
//! sans-I/O interface.
//!
//! [`Node::check`] answers requests directly. [`Node::handle`] takes
//! everything that comes from outside, a tick or a datagram, and returns the
//! datagrams to send.
//!
//! The membership protocol is SWIM (Das, Gupta, Motivala, 2002). Every
//! protocol period a node probes one peer, going round-robin over a shuffled
//! list. With no ACK within `ack_timeout` it asks `indirect_probes` other
//! peers to probe it (PING-REQ). With no answer by the end of the period the
//! peer becomes a suspect, and a suspicion nobody refutes becomes death
//! after `suspicion_timeout`. News rides on every message.
//!
//! On top of the paper:
//!
//! - Every message opens with the sender's own record, so whoever hears from
//!   a node knows it. That is the whole join protocol: a newcomer knocks on a
//!   seed and learns the others as they probe it.
//! - A peer held as suspect or dead hears it on every message sent to it,
//!   not only while the news is in the gossip buffer: it is the one node
//!   that can refute it.
//! - Every `reconnect_interval` the probe goes to a random dead member or
//!   unknown seed instead. Without it, two sides that buried each other
//!   would never speak again.

use rateguard_proto::{self as proto, Address, DemandReport, KeyDemand, Message, Status, Update};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::{
    allocation::{self, View},
    boundary::{Action, Event, PeerId},
    gcra::{Decision, Nanos, Quota},
    gossip::{self, Gossip},
    limiter::{Config, Limiter},
    member::{Applied, MemberTable},
    membership::{Change, Membership},
    partition::{self, PartitionPolicy, SizeHistory},
    peer_demand::{self, PeerDemand},
    rng::Rng,
};

const ONE_MS: Nanos = 1_000_000;

// How many rounds a share has to stay up before it is applied. A peer
// recomputes its shares once a round, so up to a round after it has heard
// this node's demand; until then it still holds the share it had. Were this
// node to take more at once, the two would overlap: falling at once and
// rising two rounds late, the peers give up first.
const RISE_DELAY_ROUNDS: usize = 2;

/// The parameters of the membership protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwimConfig {
    /// T: one probe per period, and the period of the allocation round.
    pub protocol_period: Nanos,
    /// How long to wait for a direct ACK before asking helpers. Shorter
    /// than the period, which must leave room for the indirect round trip.
    pub ack_timeout: Nanos,
    /// k: how many helpers a PING-REQ goes to.
    pub indirect_probes: usize,
    /// How long a suspect has to refute before it is declared dead.
    pub suspicion_timeout: Nanos,
    /// How often a probe goes to a dead member or an unknown seed instead.
    pub reconnect_interval: Nanos,
    /// How long a dead member is remembered.
    pub tombstone_ttl: Nanos,
}
impl Default for SwimConfig {
    // The ACK gets half the period; the other half is for the PING-REQ round
    // trip through a helper. The suspicion outlasts the 5 s GC pause of the
    // chaos scenarios, with room left for the paused node to hear of it and
    // refute. A reconnect every 2 s takes one round in ten from failure
    // detection.
    fn default() -> Self {
        Self {
            protocol_period: 200 * ONE_MS,
            ack_timeout: 100 * ONE_MS,
            indirect_probes: 3,
            suspicion_timeout: 6_000 * ONE_MS,
            reconnect_interval: 2_000 * ONE_MS,
            tombstone_ttl: 600_000 * ONE_MS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Probe {
    target: PeerId,
    seq: u32,
    sent_at: Nanos,
    helpers: Option<Vec<PeerId>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Relay {
    requester: PeerId,
    seq: u32,
    target: PeerId,
    expires_at: Nanos,
}

/// One cluster member.
pub struct Node {
    limiter: Limiter,
    members: MemberTable,
    gossip: Gossip,
    swim: SwimConfig,
    rng: Rng,
    seeds: Vec<(PeerId, Address)>,
    order: Vec<PeerId>,
    next: usize,
    seq: u32,
    probe: Option<Probe>,
    relays: BTreeMap<u32, Relay>,
    next_round_at: Nanos,
    next_reconnect_at: Nanos,
    last_round: Option<Nanos>,
    last_now: Nanos,
    actions: Vec<Action>,
    peer_demand: PeerDemand,
    round_number: u16,
    epoch: u32,
    own_demand: Vec<KeyDemand>,
    policy: PartitionPolicy,
    sizes: SizeHistory,
    held_size: usize,
    quorum_size: usize,
    floored_until: Nanos,
    joined_at: Option<Nanos>,
    expects_peers: bool,
    last_heard: BTreeMap<PeerId, Nanos>,
    reported: VecDeque<(Nanos, Vec<KeyDemand>)>,
    recent_shares: BTreeMap<u64, [f64; RISE_DELAY_ROUNDS]>,
}
/// The sizes of a node's collections; see [`Node::footprint`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footprint {
    /// Keys with limiter state, hot or cold.
    pub tracked_keys: usize,
    /// Keys in the hot set.
    pub hot_keys: usize,
    /// Keys the peers reported, over all peers.
    pub peer_demand_keys: usize,
    /// Members known, the dead not yet forgotten and this node included.
    pub members: usize,
    /// Peers with a time of last message.
    pub last_heard: usize,
    /// Membership news waiting to be spread.
    pub gossip: usize,
    /// This node's own past reports, kept to date its demand.
    pub reported_rounds: usize,
    /// Keys over those reports.
    pub reported_keys: usize,
    /// Keys with recent shares, for the delay on a rising share.
    pub recent_shares: usize,
    /// PING-REQs being relayed.
    pub relays: usize,
    /// Peers in the probe order.
    pub probe_order: usize,
}

impl Node {
    /// A node alone in its cluster. `seed` drives its random choices, so the
    /// same seed replays the same run.
    ///
    /// # Panics
    ///
    /// If the timeouts do not fit together, see the messages for each.
    pub fn new(config: Config, swim: SwimConfig, local: PeerId, addr: Address, seed: u64) -> Self {
        assert!(swim.protocol_period > 0, "protocol_period must be > 0");
        assert!(
            swim.ack_timeout > 0 && swim.ack_timeout < swim.protocol_period,
            "the ACK timeout must leave part of the period for the indirect probe"
        );
        assert!(
            swim.suspicion_timeout >= swim.protocol_period,
            "a suspicion shorter than a protocol period expires before anyone can refute it"
        );
        assert!(
            swim.reconnect_interval >= swim.protocol_period,
            "a reconnect more often than once a period would replace every probe"
        );
        assert!(
            swim.tombstone_ttl >= swim.suspicion_timeout,
            "a tombstone must outlive the gossip of the death, or a late echo resurrects it"
        );

        Self {
            limiter: Limiter::new(config),
            members: MemberTable::new(local, addr),
            gossip: Gossip::new(),
            swim,
            rng: Rng::new(seed),
            seeds: Vec::new(),
            order: Vec::new(),
            next: 0,
            seq: 0,
            probe: None,
            relays: BTreeMap::new(),
            next_round_at: 0,
            next_reconnect_at: 0,
            last_round: None,
            last_now: 0,
            actions: Vec::new(),
            peer_demand: PeerDemand::new(),
            round_number: 0,
            // Drawn apart from `rng`, so as not to shift the random choices
            // a seed replays.
            epoch: Rng::new(!seed).next_u64() as u32,
            own_demand: Vec::new(),
            policy: PartitionPolicy::default(),
            sizes: SizeHistory::new(hold_of(PartitionPolicy::default())),
            held_size: 0,
            quorum_size: 1,
            floored_until: 0,
            joined_at: None,
            expects_peers: false,
            last_heard: BTreeMap::new(),
            reported: VecDeque::new(),
            recent_shares: BTreeMap::new(),
        }
    }

    /// Adds a peer as alive without asking it: static membership, for tests
    /// and fixed fleets. A node that does not know the cluster joins it
    /// through [`add_seed`](Node::add_seed) instead.
    pub fn introduce(&mut self, peer: PeerId, addr: Address) {
        assert_ne!(peer, self.members.local(), "a node is not its own peer");
        self.members.apply(
            Update {
                member: peer.get(),
                addr,
                incarnation: 0,
                status: Status::Alive,
            },
            self.last_now,
        );
    }

    /// Adds an address to join through. A seed is not a member until it
    /// answers, carrying its own record like every message does. While the
    /// node has no peers it knocks on a seed every round; later the seeds it
    /// has lost touch with join the reconnect rotation, which heals a split
    /// even after both sides have forgotten each other.
    pub fn add_seed(&mut self, seed: PeerId, addr: Address) {
        assert_ne!(
            seed,
            self.members.local(),
            "a node cannot join through itself"
        );
        if !self.seeds.iter().any(|&(known, _)| known == seed) {
            self.seeds.push((seed, addr));
        }
        self.refresh_cap(self.last_now);
    }

    /// Waits to join even with no seed to join through yet: for seeds still
    /// to be found, such as a DNS name nobody is behind yet. Until the node
    /// joins it holds every key at the floor, as it does with seeds, rather
    /// than take itself for a cluster of one.
    pub fn expect_peers(&mut self) {
        self.expects_peers = true;
        self.refresh_cap(self.last_now);
    }

    /// Forgets an address to join through, for seeds that come and go, such
    /// as the addresses a DNS name resolves to. A member that was a seed
    /// stays a member.
    pub fn remove_seed(&mut self, seed: PeerId) {
        self.seeds.retain(|&(known, _)| known != seed);
        self.refresh_cap(self.last_now);
    }

    /// Counts attempts collected by a runtime that enforces in its own table;
    /// [`quota`](Node::quota) is the way back.
    ///
    /// # Panics
    ///
    /// If `now` is earlier than a time already seen.
    pub fn record_attempts(&mut self, key: u64, n: u64, now: Nanos) {
        self.advance(now);
        let cluster_size = self.allocation_size();
        self.limiter.record_attempts(key, n, now, cluster_size);
    }

    /// The quota `key` is held to after the last round, every cap included.
    pub fn quota(&self, key: u64) -> Option<Quota> {
        self.limiter.quota(key)
    }

    /// The quota a key not seen before starts with.
    pub fn new_key_quota(&self) -> Quota {
        self.limiter.new_key_quota(self.allocation_size())
    }

    /// Whether the key has state on this node.
    pub fn is_tracked(&self, key: u64) -> bool {
        self.limiter.tracked(key)
    }

    /// Where to send to `peer`: a member's own address, or a seed's.
    pub fn address(&self, peer: PeerId) -> Option<Address> {
        self.members.address(peer).or_else(|| {
            self.seeds
                .iter()
                .find(|&&(seed, _)| seed == peer)
                .map(|&(_, addr)| addr)
        })
    }

    /// How many members are live, alive or suspect, this node included.
    pub fn cluster_size(&self) -> usize {
        self.members.cluster_size()
    }

    /// What the node does when its cluster shrinks; see [`PartitionPolicy`].
    pub fn set_partition_policy(&mut self, policy: PartitionPolicy) {
        self.policy = policy;
        self.sizes = SizeHistory::new(hold_of(policy));
        self.held_size = 0;
        self.refresh_cap(self.last_now);
    }

    /// The policy set with [`set_partition_policy`](Node::set_partition_policy).
    pub fn partition_policy(&self) -> PartitionPolicy {
        self.policy
    }

    // N as allocation sees it: the membership's, or under HoldDown the
    // largest of the hold. Cold and hot keys use the same one (spec §4.1).
    fn allocation_size(&self) -> usize {
        self.members.cluster_size().max(self.held_size)
    }

    /// The member table, read only.
    pub fn members(&self) -> &MemberTable {
        &self.members
    }

    /// The peer probed this period that has not answered yet.
    pub fn awaiting_ack(&self) -> Option<PeerId> {
        self.probe.as_ref().map(|probe| probe.target)
    }

    /// When the last protocol round ran.
    pub fn last_round(&self) -> Option<Nanos> {
        self.last_round
    }

    /// How much the node holds, collection by collection: what guarantee 4
    /// of the spec bounds by the config and the cluster size.
    pub fn footprint(&self) -> Footprint {
        Footprint {
            tracked_keys: self.limiter.tracked_keys(),
            hot_keys: self.limiter.hot_keys(),
            peer_demand_keys: self.peer_demand.len(),
            members: self.members.known(),
            last_heard: self.last_heard.len(),
            gossip: self.gossip.len(),
            reported_rounds: self.reported.len(),
            reported_keys: self.reported.iter().map(|(_, keys)| keys.len()).sum(),
            recent_shares: self.recent_shares.len(),
            relays: self.relays.len(),
            probe_order: self.order.len(),
        }
    }

    /// The limiter, read only.
    pub fn limiter(&self) -> &Limiter {
        &self.limiter
    }

    /// What the peers told this node about their demand.
    pub fn peer_demand(&self) -> &PeerDemand {
        &self.peer_demand
    }

    /// The hot path: admits or denies a request for `key`. No I/O, no
    /// locks, and the same latency whatever the state of the cluster.
    ///
    /// # Panics
    ///
    /// If `now` is earlier than a time already seen: time running backwards
    /// would break GCRA silently.
    pub fn check(&mut self, key: u64, now: Nanos) -> Decision {
        self.advance(now);
        self.limiter.check(key, now, self.allocation_size())
    }

    /// Handles an event and returns the datagrams to send.
    ///
    /// A tick may, and should, come more often than the protocol period: the
    /// round runs once per period, the timeouts are checked on every tick.
    /// For the ACK timeout to mean anything the tick must come at least that
    /// often, and for rounds to keep their period exactly its interval
    /// should divide the period.
    ///
    /// # Panics
    ///
    /// If `now` is earlier than a time already seen.
    pub fn handle(&mut self, event: Event<'_>, now: Nanos) -> &[Action] {
        self.advance(now);
        self.actions.clear();

        match event {
            Event::Tick => {
                for dead in self
                    .members
                    .expire_suspects(now, self.swim.suspicion_timeout)
                {
                    self.gossip.push(dead);
                }
                if self.members.forget_dead(now, self.swim.tombstone_ttl) > 0 {
                    let members = &self.members;
                    self.last_heard
                        .retain(|&peer, _| members.status(peer).is_some());
                }
                self.relays.retain(|_, relay| relay.expires_at > now);
                if now >= self.next_round_at {
                    self.round(now);
                }
                self.escalate(now);
            }
            Event::MessageReceived { from, bytes } => self.receive(from, bytes, now),
        }

        &self.actions
    }

    fn round(&mut self, now: Nanos) {
        self.next_round_at = now + self.swim.protocol_period;
        self.last_round = Some(now);

        if let Some(missed) = self.probe.take()
            && let Some(suspicion) = self.members.suspect(missed.target, now)
        {
            self.gossip.push(suspicion);
        }
        self.apply_policy(now);
        self.apply_shares(now);
        self.report_demand(now);
        self.follow_changes(now);
        let stale = peer_demand::stale_after_rounds(self.allocation_size());
        self.peer_demand.expire(
            now,
            (stale * self.swim.protocol_period).saturating_add(hold_of(self.policy)),
        );

        // Reconnect: once in a while the probe goes to someone we have lost
        // instead. Nobody probes the dead, so after a mutual burial nothing
        // would ever be said that one side could refute. A node with no one
        // to probe spends every round on it: that is how it joins.
        let target = if now >= self.next_reconnect_at
            && let Some(lost) = self.random_lost()
        {
            self.next_reconnect_at = now + self.swim.reconnect_interval;
            Some(lost)
        } else if let Some(target) = self.next_target() {
            Some(target)
        } else {
            self.random_lost()
        };
        if let Some(target) = target {
            self.ping(target, now);
        }
    }

    fn random_lost(&mut self) -> Option<PeerId> {
        let lost: Vec<PeerId> = self
            .members
            .dead()
            .chain(
                self.seeds
                    .iter()
                    .map(|&(seed, _)| seed)
                    .filter(|&seed| self.members.status(seed).is_none()),
            )
            .collect();
        if lost.is_empty() {
            return None;
        }
        let pick = self.rng.up_to(lost.len() as u64 - 1) as usize;
        Some(lost[pick])
    }

    fn ping(&mut self, target: PeerId, now: Nanos) {
        let seq = self.next_seq();
        self.probe = Some(Probe {
            target,
            seq,
            sent_at: now,
            helpers: None,
        });
        let updates = self.outgoing(target);
        let demand = self.outgoing_demand();
        self.send(
            target,
            &Message::Ping {
                seq,
                updates,
                demand,
            },
        );
    }

    // SWIM's indirect probe: a target that did not answer us may still answer
    // others, and then the fault is in our link, not in the target.
    fn escalate(&mut self, now: Nanos) {
        let Some(probe) = &self.probe else {
            return;
        };
        if probe.helpers.is_some() || now < probe.sent_at + self.swim.ack_timeout {
            return;
        }
        let (target, seq) = (probe.target, probe.seq);

        let mut helpers: Vec<PeerId> = self
            .members
            .peers()
            .iter()
            .copied()
            .filter(|&peer| peer != target && self.members.status(peer) == Some(Status::Alive))
            .collect();
        self.rng.shuffle(&mut helpers);
        helpers.truncate(self.swim.indirect_probes);

        for &helper in &helpers {
            let updates = self.outgoing(helper);
            let demand = self.outgoing_demand();
            self.send(
                helper,
                &Message::PingReq {
                    seq,
                    target: target.get(),
                    updates,
                    demand,
                },
            );
        }
        if let Some(probe) = &mut self.probe {
            probe.helpers = Some(helpers);
        }
    }

    // SWIM inserts a newcomer at a random place among the peers still to be
    // probed this round, so it is probed within the round it joined.
    fn follow_changes(&mut self, now: Nanos) {
        for change in self.members.drain_changes() {
            match change {
                Change::Joined(peer) if !self.order[self.next..].contains(&peer) => {
                    let remaining = (self.order.len() - self.next) as u64;
                    let at = self.next + self.rng.up_to(remaining) as usize;
                    self.order.insert(at, peer);
                }
                Change::Joined(_) => {}
                Change::Left(peer) => self.peer_demand.leave(peer, now, hold_of(self.policy)),
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
        // A node may hear itself, through a seed list that names it under
        // another address or a transport that loops back; it is not its
        // own peer.
        if from == self.members.local() {
            return;
        }
        let Ok(message) = proto::decode(bytes) else {
            return;
        };
        for &update in message.updates() {
            self.learn(update, now);
        }
        // Only members are heard of: a stranger that does not introduce
        // itself would grow the table without bound.
        if self.members.status(from).is_some() {
            self.last_heard.insert(from, now);
        }
        // Demand comes first hand and whole, in every message; an empty
        // report says the peer has no hot key. A message with no report
        // has no round to order it by, and changes nothing.
        if let Some(report) = message
            .demand()
            .iter()
            .find(|report| report.origin == from.get())
        {
            self.peer_demand.apply(from, report, now);
        }
        match message {
            Message::Ping { seq, .. } => self.ack(from, seq),
            Message::Ack {
                seq, ref demand, ..
            } => self.acknowledged(from, seq, demand, now),
            Message::PingReq { seq, target, .. } => self.relay(from, seq, PeerId::new(target), now),
        }
    }

    // An ACK relayed by a helper carries the target's report: the one way
    // its demand reaches a requester that cannot hear it directly, on a
    // link that fails one way only.
    fn acknowledged(&mut self, from: PeerId, seq: u32, demand: &[DemandReport], now: Nanos) {
        if let Some(probe) = &self.probe
            && probe.seq == seq
        {
            let target = probe.target;
            let by_helper = probe
                .helpers
                .as_ref()
                .is_some_and(|helpers| helpers.contains(&from));
            if from == target || by_helper {
                if by_helper && let Some(report) = demand.iter().find(|r| r.origin == target.get())
                {
                    self.peer_demand.apply(target, report, now);
                }
                self.probe = None;
                return;
            }
        }
        if self
            .relays
            .get(&seq)
            .is_some_and(|relay| relay.target == from)
            && let Some(relay) = self.relays.remove(&seq)
        {
            // The target's report in place of ours: two full reports would
            // not fit one datagram, and a message without ours changes
            // nothing at the requester.
            let theirs = demand
                .iter()
                .filter(|report| report.origin == from.get())
                .cloned()
                .collect();
            let updates = self.outgoing(relay.requester);
            self.send(
                relay.requester,
                &Message::Ack {
                    seq: relay.seq,
                    updates,
                    demand: theirs,
                },
            );
        }
    }

    fn relay(&mut self, requester: PeerId, seq: u32, target: PeerId, now: Nanos) {
        if target == self.members.local() {
            self.ack(requester, seq);
            return;
        }
        let own = self.next_seq();
        self.relays.insert(
            own,
            Relay {
                requester,
                seq,
                target,
                expires_at: now + self.swim.protocol_period,
            },
        );
        let updates = self.outgoing(target);
        let demand = self.outgoing_demand();
        self.send(
            target,
            &Message::Ping {
                seq: own,
                updates,
                demand,
            },
        );
    }

    fn ack(&mut self, to: PeerId, seq: u32) {
        let updates = self.outgoing(to);
        let demand = self.outgoing_demand();
        self.send(
            to,
            &Message::Ack {
                seq,
                updates,
                demand,
            },
        );
    }

    fn next_seq(&mut self) -> u32 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
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

    // Every message opens with the sender's own record: whoever hears from us
    // learns that we exist and in which incarnation. That is how a newcomer
    // joins and how a forgotten member comes back.
    //
    // Whoever we hold as suspect or dead hears it from us on every message,
    // not only while the news is in the buffer: it is the one node that can
    // refute it, and a death nobody repeats to the dead is never undone.
    fn outgoing(&mut self, to: PeerId) -> Vec<Update> {
        let own = self
            .members
            .update_about(self.members.local())
            .expect("a node always knows itself");
        let accusation = self
            .members
            .update_about(to)
            .filter(|update| update.status != Status::Alive);
        let room = proto::MAX_UPDATES - 1 - usize::from(accusation.is_some());
        let limit = gossip::retransmit_limit(self.members.cluster_size());

        let mut updates = vec![own];
        updates.extend(
            self.gossip
                .take(room, limit)
                .into_iter()
                .filter(|&update| update != own),
        );
        if let Some(accusation) = accusation
            && !updates.contains(&accusation)
        {
            updates.push(accusation);
        }
        updates
    }

    // A key some peer holds hot by its own demand is hot here too (spec
    // §4.1), and every hot key gets its share of the limit from the demand
    // the peers reported (spec §4.3). Until the node has been up long enough to hear
    // from every peer, what it has not heard may be demand: it learns, and
    // takes no more than an even split.
    // Spec §5.1. HoldDown holds the cluster size here, and the demand of
    // the peers that left in follow_changes. Quorum caps every key at the
    // floor while this node sees no majority of the cluster it knows: the
    // live peers, suspects not counted, against all of them, the dead too,
    // and those dead long enough to be forgotten as well.
    //
    // A node that regains its quorum stays at the floor until every peer
    // has had time to hear it: the majority forgot its demand during the
    // split and, until it hears it again, still hands all of R out among
    // itself.
    fn apply_policy(&mut self, now: Nanos) {
        let size = self.members.cluster_size();
        self.held_size = self.sizes.record(now, size);
        if size > 1 && self.joined_at.is_none() {
            self.joined_at = Some(now);
        }

        // The dead are forgotten after tombstone_ttl, and a minority that
        // forgot them would take itself for the whole cluster. So the size a
        // majority is counted against grows at once but shrinks only while
        // this node has a majority of it.
        let alive = 1 + self
            .members
            .peers()
            .iter()
            .filter(|&&peer| self.members.status(peer) == Some(Status::Alive))
            .count();
        let known = size + self.members.dead().count();
        let counted = known.max(self.quorum_size);
        let quorum = partition::has_quorum(alive, counted);
        self.quorum_size = if quorum { known } else { counted };
        if self.policy == PartitionPolicy::Quorum && !quorum {
            let heard_within = peer_demand::stale_after_rounds(counted) * self.swim.protocol_period;
            self.floored_until = now + heard_within;
        }
        self.refresh_cap(now);
    }

    // The cap every key is held to, if any: the floor `R·β/N`, the one part
    // of the limit the rest of the cluster keeps for this node whatever it
    // knows of it.
    //
    // - Learning (spec §10.5): the peers have not heard this node's demand
    //   yet, so they hand all but its floor out among themselves.
    // - Waiting to join: a node with seeds, or told to expect peers, and no
    //   peer yet does not even know N. Taking itself for a cluster of one,
    //   it would hand out up to R on top of what the cluster it is about to
    //   join already shares. N is taken as itself and its seeds.
    //   Optimistic serves anyway, by definition.
    // - Quorum without a majority, see apply_policy.
    fn refresh_cap(&mut self, now: Nanos) {
        let config = self.limiter.config();
        let floor = |known: usize| config.limit_per_sec as f64 * config.floor_factor / known as f64;

        let waiting = self.joined_at.is_none() && (self.expects_peers || !self.seeds.is_empty());
        let mut caps = Vec::with_capacity(3);
        if waiting && self.policy != PartitionPolicy::Optimistic {
            caps.push(floor(1 + self.seeds.len()));
        }
        if !waiting && !self.informed_since(self.joined_at.unwrap_or(0), now) {
            caps.push(floor(self.allocation_size()));
        }
        if self.policy == PartitionPolicy::Quorum && now < self.floored_until {
            caps.push(floor(self.quorum_size));
        }
        self.limiter.set_cap(caps.into_iter().reduce(f64::min));
    }

    // Learning (spec §10.5): whether every live peer has been heard since
    // `since`, and so, a message going each way, has most likely heard this
    // node's latest demand too. After stale_after_rounds it is taken as
    // done in any case, lest a peer that never gets through keep the node
    // learning forever.
    fn informed_since(&self, since: Nanos, now: Nanos) -> bool {
        let stale = peer_demand::stale_after_rounds(self.allocation_size());
        now >= since + stale * self.swim.protocol_period
            || self
                .members
                .peers()
                .iter()
                .all(|peer| self.last_heard.get(peer).is_some_and(|&at| at > since))
    }

    // A key that has just turned hot here learns too: the peers have not
    // heard its demand here, and if it turned hot there at the same time,
    // this node has not heard theirs. Until every peer has been heard since
    // it turned hot, it gets the floor; a key that turns hot in this very
    // tick is not in the list yet and gets the floor as well, unless there
    // is nobody to wait for.
    fn apply_shares(&mut self, now: Nanos) {
        let cluster_size = self.allocation_size();
        let config = *self.limiter.config();
        let joined_at = self.joined_at.unwrap_or(0);
        let informed: BTreeSet<u64> = self
            .limiter
            .hot_demand()
            .into_iter()
            .map(|(key, ..)| key)
            .filter(|&key| {
                let since = self.limiter.hot_since(key).unwrap_or(now).max(joined_at);
                self.informed_since(since, now)
            })
            .collect();
        let alone = self.informed_since(now, now);
        let as_of = self.as_of();
        let reported = &self.reported;
        let peers = &self.peer_demand;
        let floor = config.limit_per_sec as f64 * config.floor_factor / cluster_size as f64;
        let previous = std::mem::take(&mut self.recent_shares);
        let mut recent = BTreeMap::new();
        self.limiter.tick_with_shares(
            now,
            cluster_size,
            |key| peers.hot_elsewhere(key),
            |key, own| {
                let share = if !alone && !informed.contains(&key) {
                    floor
                } else {
                    let then = as_of.and_then(|at| reported_demand(reported, key, at));
                    allocation::share(View {
                        limit: config.limit_per_sec as f64,
                        cluster_size,
                        floor_factor: config.floor_factor,
                        own: then.map_or(own, |then| own.min(then)),
                        others: peers.total(key),
                    })
                };
                // A share falls at once and rises only once it has been
                // computed as high for RISE_DELAY_ROUNDS more rounds.
                let past = previous
                    .get(&key)
                    .copied()
                    .unwrap_or([share; RISE_DELAY_ROUNDS]);
                let applied = past.iter().copied().fold(share, f64::min);
                let mut kept = [share; RISE_DELAY_ROUNDS];
                kept[1..].copy_from_slice(&past[..RISE_DELAY_ROUNDS - 1]);
                recent.insert(key, kept);
                applied
            },
        );
        self.recent_shares = recent;
    }

    // What the peers' demand dates from: the last message from the peer
    // heard from longest ago. None with no peer, or one never heard at all.
    //
    // A share compares this node's demand with the peers'. Theirs is as old
    // as the last message from each; this node's is fresh. While demand
    // climbs everywhere, fresh against stale makes every node overrate its
    // part, and the shares add up to more than R. So the node takes its own
    // demand as it was then, or as it is now if that is less.
    fn as_of(&self) -> Option<Nanos> {
        self.members
            .peers()
            .iter()
            .map(|peer| self.last_heard.get(peer).copied())
            .collect::<Option<Vec<Nanos>>>()?
            .into_iter()
            .min()
    }

    // Once a round, right after the limiter has decided which keys are hot.
    // The whole hot set fits one report: hot_set_size is capped at
    // MAX_DEMAND_KEYS. Past reports are kept as far back as a peer's
    // demand may date from, for as_of.
    fn report_demand(&mut self, now: Nanos) {
        self.round_number = self.round_number.wrapping_add(1);
        self.own_demand = self
            .limiter
            .hot_demand()
            .into_iter()
            .map(|(key_hash, rate, primary)| KeyDemand {
                key_hash,
                demand: rate as f32,
                primary,
            })
            .collect();

        let kept =
            peer_demand::stale_after_rounds(self.allocation_size()) * self.swim.protocol_period;
        self.reported.push_back((now, self.own_demand.clone()));
        while self
            .reported
            .front()
            .is_some_and(|&(at, _)| at + kept < now)
        {
            self.reported.pop_front();
        }
    }

    // Every message carries our own demand, and only ours: demand is
    // exchanged first hand (spec §10.8). With no hot key the report is
    // empty, not left out: its round orders it against the older ones.
    fn outgoing_demand(&self) -> Vec<DemandReport> {
        vec![DemandReport {
            origin: self.members.local().get(),
            epoch: self.epoch,
            round: self.round_number,
            keys: self.own_demand.clone(),
        }]
    }

    fn send(&mut self, peer: PeerId, message: &Message) {
        let bytes = proto::encode(message)
            .expect("a message within the proto limits always fits a datagram");
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

// This node's demand for `key` in its last report made at or before `at`,
// if the key was in it.
fn reported_demand(
    reported: &VecDeque<(Nanos, Vec<KeyDemand>)>,
    key: u64,
    at: Nanos,
) -> Option<f64> {
    let (_, keys) = reported.iter().rev().find(|&&(made, _)| made <= at)?;
    keys.iter()
        .find(|entry| entry.key_hash == key)
        .map(|entry| entry.demand as f64)
}

// How long HoldDown holds; nothing for the other policies.
fn hold_of(policy: PartitionPolicy) -> Nanos {
    match policy {
        PartitionPolicy::HoldDown(hold) => hold,
        PartitionPolicy::Optimistic | PartitionPolicy::Quorum => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionPolicy;
    use crate::peer_demand::stale_after_rounds;
    use Status::{Alive, Dead, Suspect};
    use proto::{DemandReport, KeyDemand};
    use std::collections::BTreeSet;

    const ONE_SEC: Nanos = 1_000_000_000;
    const KEY: u64 = 42;
    const LOCAL: PeerId = PeerId::new(0);
    const PERIOD: Nanos = 200 * ONE_MS;
    const TICK: Nanos = 50 * ONE_MS;
    const SWIM: SwimConfig = SwimConfig {
        protocol_period: PERIOD,
        ack_timeout: PERIOD / 2,
        indirect_probes: 3,
        suspicion_timeout: 5 * PERIOD,
        reconnect_interval: 5 * PERIOD,
        tombstone_ttl: 50 * PERIOD,
    };

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

    fn node() -> Node {
        Node::new(config(), SWIM, LOCAL, addr(LOCAL.get()), 1)
    }

    fn node_with(peers: impl IntoIterator<Item = u64>) -> Node {
        let mut n = node();
        for peer in peers {
            n.introduce(PeerId::new(peer), addr(peer));
        }
        n
    }

    fn measured_interval(node: &mut Node, now: Nanos) -> Nanos {
        measured_interval_of(node, KEY, now)
    }

    fn measured_interval_of(node: &mut Node, key: u64, now: Nanos) -> Nanos {
        for _ in 0..config().burst {
            assert_eq!(node.check(key, now), Decision::Allow);
        }
        let Decision::Deny { retry_at } = node.check(key, now) else {
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
            demand: Vec::new(),
        }
    }

    // 500 attempts at once: the key is hot from the next round on, the EWMA
    // well over the hot threshold of a two-node cluster, 250. Returns a time
    // safely past that.
    fn heat(node: &mut Node) -> Nanos {
        node.handle(Event::Tick, 0);
        for _ in 0..500 {
            node.check(KEY, 0);
        }
        5 * PERIOD
    }

    fn empty_report(n: &Node) -> Vec<DemandReport> {
        vec![DemandReport {
            origin: LOCAL.get(),
            epoch: n.epoch,
            round: 0,
            keys: Vec::new(),
        }]
    }

    fn own_report(message: &Message) -> &DemandReport {
        match message.demand() {
            [report] if report.origin == LOCAL.get() => report,
            other => panic!("expected the node's own report alone, got {other:?}"),
        }
    }

    // Runs round `k` of the protocol, which starts at k * PERIOD.
    fn round(node: &mut Node, k: u64) -> (PeerId, u32) {
        let (target, seq, _) = round_with_news(node, k);
        (target, seq)
    }

    fn round_with_news(node: &mut Node, k: u64) -> (PeerId, u32, Vec<Update>) {
        match sent(node.handle(Event::Tick, k * PERIOD)).as_slice() {
            [(peer, Message::Ping { seq, updates, .. })] => (*peer, *seq, updates.clone()),
            other => panic!("a round must send exactly one PING, sent {other:?}"),
        }
    }

    fn addr(member: u64) -> Address {
        Address::V4([10, 0, 0, member as u8], 7946)
    }

    fn news(member: u64, incarnation: u32, status: Status) -> Update {
        Update {
            member,
            addr: addr(member),
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
        node().introduce(LOCAL, addr(LOCAL.get()));
    }

    #[test]
    #[should_panic(expected = "expires before anyone can refute it")]
    fn a_suspicion_shorter_than_a_period_is_rejected() {
        let swim = SwimConfig {
            suspicion_timeout: PERIOD - 1,
            ..SWIM
        };
        Node::new(config(), swim, LOCAL, addr(LOCAL.get()), 1);
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
        n.introduce(PeerId::new(9), addr(9));

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
        let no_hot_key = empty_report(&n);
        let ping = Message::Ping {
            seq: 77,
            updates: Vec::new(),
            demand: Vec::new(),
        };
        assert_eq!(
            deliver(&mut n, 5, ping, 0),
            [(
                PeerId::new(5),
                Message::Ack {
                    seq: 77,
                    updates: vec![news(0, 0, Alive)],
                    demand: no_hot_key,
                }
            )],
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
        while now < suspected_at + SWIM.suspicion_timeout - TICK {
            now += TICK;
            n.handle(Event::Tick, now);
            assert_eq!(n.members().status(peer), Some(Suspect), "at {now}");
        }

        n.handle(Event::Tick, suspected_at + SWIM.suspicion_timeout);
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
        n.handle(Event::Tick, PERIOD + SWIM.suspicion_timeout);
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

    fn bury_peer_one(n: &mut Node) -> u64 {
        let mut k = 0;
        while n.members().status(PeerId::new(1)) != Some(Dead) {
            let (target, seq) = round(n, k);
            if target == PeerId::new(2) {
                deliver(n, 2, ack(seq), k * PERIOD + ONE_MS);
            }
            k += 1;
            assert!(k < 100, "peer 1 never died");
        }
        k
    }

    #[test]
    fn the_dead_are_probed_only_to_reconnect() {
        let mut n = node_with([1, 2]);
        let buried = bury_peer_one(&mut n);

        let mut reconnects = Vec::new();
        for k in buried..buried + 20 {
            let (target, seq, updates) = round_with_news(&mut n, k);
            if target == PeerId::new(1) {
                assert!(
                    updates.contains(&news(1, 0, Dead)),
                    "the dead must hear of its death to refute it"
                );
                reconnects.push(k);
            } else {
                deliver(&mut n, 2, ack(seq), k * PERIOD + ONE_MS);
            }
        }
        assert_eq!(
            reconnects.len(),
            (20 * PERIOD / SWIM.reconnect_interval) as usize,
            "one round per reconnect_interval: {reconnects:?}"
        );
        assert_eq!(n.members().status(PeerId::new(1)), Some(Dead));
    }

    #[test]
    fn a_dead_peer_that_answers_a_reconnect_comes_back() {
        let mut n = node_with([1]);
        let obituary = Message::Ack {
            seq: 999,
            updates: vec![news(1, 0, Dead)],
            demand: Vec::new(),
        };
        deliver(&mut n, 5, obituary, 0);
        assert_eq!(n.cluster_size(), 1);

        let (target, seq, updates) = round_with_news(&mut n, 0);
        assert_eq!(target, PeerId::new(1));
        assert!(updates.contains(&news(1, 0, Dead)));

        let refuting = Message::Ack {
            seq,
            updates: vec![news(1, 1, Alive)],
            demand: Vec::new(),
        };
        deliver(&mut n, 1, refuting, ONE_MS);
        assert_eq!(n.members().status(PeerId::new(1)), Some(Alive));
        assert_eq!(n.cluster_size(), 2);
    }

    #[test]
    fn a_forgotten_tombstone_is_no_longer_reconnected() {
        let mut n = node_with([2]);
        let obituary = Message::Ack {
            seq: 999,
            updates: vec![news(1, 0, Dead)],
            demand: Vec::new(),
        };
        deliver(&mut n, 2, obituary, 0);

        let first = SWIM.tombstone_ttl / PERIOD;
        for k in first..first + 20 {
            assert_eq!(
                answered_round(&mut n, k),
                PeerId::new(2),
                "round {k} probed a forgotten member"
            );
        }
        assert_eq!(n.members().status(PeerId::new(1)), None);
    }

    #[test]
    fn a_suspicion_rides_on_the_next_ping() {
        let mut n = node_with([1]);
        round(&mut n, 0);
        let (target, _, updates) = round_with_news(&mut n, 1);
        assert_eq!(target, PeerId::new(1));
        assert_eq!(
            updates,
            [news(0, 0, Alive), news(1, 0, Suspect)],
            "the suspect itself must hear of it to refute"
        );
    }

    #[test]
    fn a_suspect_refutes_in_the_ack_to_the_ping_that_accused_it() {
        let mut n = node();
        let no_hot_key = empty_report(&n);
        let ping = Message::Ping {
            seq: 5,
            updates: vec![news(0, 0, Suspect)],
            demand: Vec::new(),
        };
        assert_eq!(
            deliver(&mut n, 3, ping, 0),
            [(
                PeerId::new(3),
                Message::Ack {
                    seq: 5,
                    updates: vec![news(0, 1, Alive)],
                    demand: no_hot_key,
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
            demand: Vec::new(),
        };
        deliver(&mut n, 1, refuting, PERIOD + ONE_MS);
        assert_eq!(n.members().status(PeerId::new(1)), Some(Alive));
        assert_eq!(n.awaiting_ack(), None);
        n.handle(Event::Tick, PERIOD + SWIM.suspicion_timeout);
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
            demand: Vec::new(),
        };
        deliver(&mut n, 1, stray, 0);
        assert_eq!(n.cluster_size(), 3, "a peer learned by gossip joins");

        let (_, _, updates) = round_with_news(&mut n, 0);
        assert_eq!(updates, [news(0, 0, Alive), news(7, 0, Alive)]);
    }

    #[test]
    fn news_is_passed_on_a_limited_number_of_times() {
        let quiet = SwimConfig {
            reconnect_interval: 1000 * PERIOD,
            tombstone_ttl: 1000 * PERIOD,
            ..SWIM
        };
        let mut n = Node::new(config(), quiet, LOCAL, addr(LOCAL.get()), 1);
        n.introduce(PeerId::new(1), addr(1));
        let death = news(7, 0, Dead);
        let carrying = |updates: Vec<Update>| Message::Ack {
            seq: 999,
            updates,
            demand: Vec::new(),
        };
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
            "an echo of known news must not restart the count, \
             and the one reconnect to the dead carries the buffered copy"
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

    fn ping_req(seq: u32, target: u64) -> Message {
        Message::PingReq {
            seq,
            target,
            updates: Vec::new(),
            demand: Vec::new(),
        }
    }

    fn tick(node: &mut Node, now: Nanos) -> Vec<(PeerId, Message)> {
        sent(node.handle(Event::Tick, now))
    }

    #[test]
    fn an_unanswered_ping_turns_to_k_helpers_after_the_ack_timeout() {
        let mut n = node_with(1..=5);
        let (target, seq) = round(&mut n, 0);

        assert!(tick(&mut n, SWIM.ack_timeout - 1).is_empty());
        let asked = tick(&mut n, SWIM.ack_timeout);
        assert_eq!(asked.len(), SWIM.indirect_probes);

        let helpers: BTreeSet<PeerId> = asked
            .iter()
            .map(|(helper, message)| {
                assert!(
                    matches!(message, Message::PingReq { seq: s, target: t, .. }
                        if *s == seq && *t == target.get()),
                    "{message:?}"
                );
                *helper
            })
            .collect();
        assert_eq!(helpers.len(), SWIM.indirect_probes, "distinct helpers");
        assert!(
            !helpers.contains(&target),
            "the target cannot vouch for itself"
        );

        assert!(
            tick(&mut n, SWIM.ack_timeout + TICK).is_empty(),
            "helpers are asked once per probe"
        );
    }

    #[test]
    fn with_fewer_peers_than_k_every_other_peer_helps() {
        let mut n = node_with([1, 2]);
        let (target, _) = round(&mut n, 0);
        let asked = tick(&mut n, SWIM.ack_timeout);
        assert_eq!(asked.len(), 1);
        assert_ne!(asked[0].0, target);
    }

    #[test]
    fn a_suspect_is_not_asked_to_help() {
        let mut n = node_with([1, 2, 3]);
        let rumor = Message::Ack {
            seq: 999,
            updates: vec![news(2, 0, Suspect)],
            demand: Vec::new(),
        };
        deliver(&mut n, 3, rumor, 0);

        for k in 0..6 {
            let (target, _) = round(&mut n, k);
            let asked = tick(&mut n, k * PERIOD + SWIM.ack_timeout);
            assert!(
                asked.iter().all(|(helper, _)| *helper != PeerId::new(2)),
                "round {k}: probing {target:?} asked a suspect {asked:?}"
            );
        }
    }

    #[test]
    fn an_ack_relayed_by_a_helper_ends_the_wait() {
        let mut n = node_with([1, 2, 3]);
        let (target, seq) = round(&mut n, 0);
        let (helper, _) = tick(&mut n, SWIM.ack_timeout)[0].clone();

        deliver(&mut n, helper.get(), ack(seq), SWIM.ack_timeout + ONE_MS);
        assert_eq!(n.awaiting_ack(), None);

        round(&mut n, 1);
        assert_eq!(
            n.members().status(target),
            Some(Alive),
            "reachable through a helper is alive"
        );
    }

    #[test]
    fn the_right_seq_from_a_bystander_does_not_end_the_wait() {
        let mut n = node_with([1, 2]);
        let (target, seq) = round(&mut n, 0);
        let other = if target == PeerId::new(1) { 2 } else { 1 };

        deliver(&mut n, other, ack(seq), ONE_MS);
        assert_eq!(n.awaiting_ack(), Some(target), "not yet asked to help");
    }

    #[test]
    fn a_helper_pings_the_target_and_relays_its_ack() {
        let mut n = node();
        let forwarded = deliver(&mut n, 3, ping_req(40, 9), 0);
        let [(to, Message::Ping { seq: own, .. })] = forwarded.as_slice() else {
            panic!("a PING-REQ must turn into one PING, got {forwarded:?}");
        };
        assert_eq!(*to, PeerId::new(9));

        let relayed = deliver(&mut n, 9, ack(*own), ONE_MS);
        let [(to, Message::Ack { seq, .. })] = relayed.as_slice() else {
            panic!("the target's ACK must be relayed, got {relayed:?}");
        };
        assert_eq!(
            (*to, *seq),
            (PeerId::new(3), 40),
            "with the requester's seq"
        );

        assert!(
            deliver(&mut n, 9, ack(*own), 2 * ONE_MS).is_empty(),
            "a duplicate ACK is relayed once"
        );
    }

    // A report of `origin`'s, on an ACK of `seq`.
    fn ack_reporting(seq: u32, origin: u64) -> Message {
        let Message::Ping { demand, .. } = reporting(origin, 3) else {
            unreachable!()
        };
        Message::Ack {
            seq,
            updates: Vec::new(),
            demand,
        }
    }

    // Demand goes first hand, but a requester that cannot hear the target
    // directly hears it through the helper: the relayed ACK carries the
    // target's report, in place of the helper's own.
    #[test]
    fn a_helper_relays_the_targets_report() {
        let mut n = node();
        let forwarded = deliver(&mut n, 3, ping_req(40, 9), 0);
        let own = forwarded[0].1.seq();
        let relayed = deliver(&mut n, 9, ack_reporting(own, 9), ONE_MS);
        let [(_, answer)] = relayed.as_slice() else {
            panic!("the target's ACK must be relayed, got {relayed:?}");
        };
        let origins: Vec<u64> = answer.demand().iter().map(|r| r.origin).collect();
        assert_eq!(origins, [9], "the target's report, not the helper's");
    }

    #[test]
    fn a_requester_takes_the_targets_report_from_its_helper() {
        let mut n = node_with([1, 2, 3]);
        let (target, seq) = round(&mut n, 0);
        let (helper, _) = tick(&mut n, SWIM.ack_timeout)[0].clone();
        deliver(
            &mut n,
            helper.get(),
            ack_reporting(seq, target.get()),
            SWIM.ack_timeout + ONE_MS,
        );
        assert!(n.peer_demand().get(target, KEY).is_some());
    }

    // Anyone else's word about a third node's demand is still ignored.
    #[test]
    fn a_report_about_a_third_node_is_ignored_off_the_relay_path() {
        let mut n = node_with([1, 2, 3]);
        let (target, seq) = round(&mut n, 0);
        let other = (1..=3).map(PeerId::new).find(|&p| p != target).unwrap();
        deliver(
            &mut n,
            other.get(),
            ack_reporting(seq, target.get()),
            ONE_MS,
        );
        assert!(
            n.peer_demand().get(target, KEY).is_none(),
            "not a helper yet"
        );
    }

    #[test]
    fn a_relay_is_answered_only_by_its_target() {
        let mut n = node();
        let forwarded = deliver(&mut n, 3, ping_req(40, 9), 0);
        let own = forwarded[0].1.seq();
        assert!(deliver(&mut n, 8, ack(own), ONE_MS).is_empty());
    }

    #[test]
    fn an_unanswered_relay_expires_after_a_period() {
        let mut n = node();
        let forwarded = deliver(&mut n, 3, ping_req(40, 9), 0);
        let own = forwarded[0].1.seq();

        n.handle(Event::Tick, PERIOD);
        assert!(
            deliver(&mut n, 9, ack(own), PERIOD + ONE_MS).is_empty(),
            "the requester gave up at its next round"
        );
    }

    #[test]
    fn every_message_opens_with_the_senders_own_record() {
        let mut n = node_with([1]);
        let (_, _, updates) = round_with_news(&mut n, 0);
        assert_eq!(updates.first(), Some(&news(0, 0, Alive)));
    }

    #[test]
    fn anyone_who_writes_to_us_becomes_known() {
        let mut n = node();
        let hello = Message::Ping {
            seq: 1,
            updates: vec![news(8, 2, Alive)],
            demand: Vec::new(),
        };
        deliver(&mut n, 8, hello, 0);
        assert_eq!(
            n.members().update_about(PeerId::new(8)),
            Some(news(8, 2, Alive))
        );
        assert_eq!(n.cluster_size(), 2);
    }

    #[test]
    fn a_lone_node_knocks_on_its_seed_every_round() {
        let mut n = node();
        n.add_seed(PeerId::new(5), addr(5));
        for k in 0..4 {
            assert_eq!(round(&mut n, k).0, PeerId::new(5), "round {k}");
        }
        assert_eq!(n.cluster_size(), 1, "an unanswered seed is no member");
    }

    #[test]
    fn a_seed_that_answers_becomes_a_member() {
        let mut n = node();
        n.add_seed(PeerId::new(5), addr(5));
        let (_, seq) = round(&mut n, 0);

        let answer = Message::Ack {
            seq,
            updates: vec![news(5, 3, Alive)],
            demand: Vec::new(),
        };
        deliver(&mut n, 5, answer, ONE_MS);
        assert_eq!(
            n.members().update_about(PeerId::new(5)),
            Some(news(5, 3, Alive))
        );
        assert_eq!(n.cluster_size(), 2);
    }

    #[test]
    fn an_unknown_seed_joins_the_reconnect_rotation() {
        let mut n = node_with([1]);
        n.add_seed(PeerId::new(5), addr(5));

        let mut knocks = 0;
        for k in 0..20 {
            let (target, seq) = round(&mut n, k);
            if target == PeerId::new(5) {
                knocks += 1;
            } else {
                deliver(&mut n, 1, ack(seq), k * PERIOD + ONE_MS);
            }
        }
        assert_eq!(knocks, (20 * PERIOD / SWIM.reconnect_interval) as usize);
    }

    #[test]
    #[should_panic(expected = "cannot join through itself")]
    fn a_node_is_not_its_own_seed() {
        node().add_seed(LOCAL, addr(LOCAL.get()));
    }

    #[test]
    #[should_panic(expected = "leave part of the period")]
    fn an_ack_timeout_as_long_as_the_period_is_rejected() {
        let swim = SwimConfig {
            ack_timeout: PERIOD,
            ..SWIM
        };
        Node::new(config(), swim, LOCAL, addr(LOCAL.get()), 1);
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
            let mut n = Node::new(config(), SWIM, LOCAL, addr(LOCAL.get()), seed);
            for peer in 1..=6 {
                n.introduce(PeerId::new(peer), addr(peer));
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

    #[test]
    fn a_round_reports_the_demand_of_hot_keys() {
        let mut n = node_with([1]);
        let now = heat(&mut n);
        let out = sent(n.handle(Event::Tick, now));
        let [(_, ping @ Message::Ping { .. })] = out.as_slice() else {
            panic!("a round must send exactly one PING, sent {out:?}");
        };
        let [key] = own_report(ping).keys.as_slice() else {
            panic!("one hot key, one entry");
        };
        assert_eq!(key.key_hash, KEY);
        assert!(key.primary, "hot by its own demand");
        assert!(
            (250.0..=500.0).contains(&key.demand),
            "the EWMA of 500 attempts a second, a second in: {}",
            key.demand
        );
    }

    #[test]
    fn each_round_reports_under_a_new_round_number() {
        let mut n = node_with([1]);
        let now = heat(&mut n);
        let first = own_report(&sent(n.handle(Event::Tick, now))[0].1).round;
        let second = own_report(&sent(n.handle(Event::Tick, now + PERIOD))[0].1).round;
        assert_eq!(second, first.wrapping_add(1));
    }

    #[test]
    fn answers_and_relayed_probes_carry_the_report_too() {
        let mut n = node_with([1, 2]);
        let now = heat(&mut n);
        n.handle(Event::Tick, now);

        let ping = Message::Ping {
            seq: 7,
            updates: Vec::new(),
            demand: Vec::new(),
        };
        let [(_, answer)] = deliver(&mut n, 1, ping, now).try_into().unwrap();
        own_report(&answer);

        let [(_, relayed)] = deliver(&mut n, 1, ping_req(8, 2), now).try_into().unwrap();
        own_report(&relayed);
    }

    // An empty report, not none: it carries a round, so a late message
    // cannot undo a newer report (spec §5.2).
    #[test]
    fn without_hot_keys_an_empty_report_is_sent() {
        let mut n = node_with([1]);
        n.check(KEY, 0);
        let out = sent(n.handle(Event::Tick, 0));
        assert!(own_report(&out[0].1).keys.is_empty(), "{out:?}");
    }

    #[test]
    fn each_start_reports_under_an_epoch_of_its_own() {
        let epoch = |seed: u64| {
            let mut n = Node::new(config(), SWIM, LOCAL, addr(LOCAL.get()), seed);
            n.introduce(PeerId::new(1), addr(1));
            let out = sent(n.handle(Event::Tick, 0));
            own_report(&out[0].1).epoch
        };
        assert_ne!(epoch(1), epoch(2));
    }

    fn reporting(from: u64, round: u16) -> Message {
        Message::Ping {
            seq: 7,
            updates: Vec::new(),
            demand: vec![DemandReport {
                origin: from,
                epoch: 0,
                round,
                keys: vec![KeyDemand {
                    key_hash: KEY,
                    demand: 75.0,
                    primary: true,
                }],
            }],
        }
    }

    #[test]
    fn an_empty_report_means_the_sender_has_no_hot_key() {
        let mut n = node_with([1]);
        deliver(&mut n, 1, reporting(1, 3), 0);
        assert!(!n.peer_demand().is_empty());

        let mut cooled = reporting(1, 4);
        if let Message::Ping { demand, .. } = &mut cooled {
            demand[0].keys.clear();
        }
        deliver(&mut n, 1, cooled, 1);
        assert!(n.peer_demand().is_empty());
    }

    // A message with no report has no round to tell how old it is.
    #[test]
    fn a_message_without_a_report_changes_nothing() {
        let mut n = node_with([1]);
        deliver(&mut n, 1, reporting(1, 3), 0);

        let silent = Message::Ping {
            seq: 8,
            updates: Vec::new(),
            demand: Vec::new(),
        };
        deliver(&mut n, 1, silent, 1);
        assert!(!n.peer_demand().is_empty());
    }

    // Pods come and go, each under a new address and so a new ID: what the
    // node remembers of them must go once the member table forgets them.
    #[test]
    fn members_come_and_go_and_leave_no_trace() {
        let mut n = node();
        let mut now = 0;
        for peer in 1..=20 {
            let hello = Message::Ping {
                seq: 1,
                updates: vec![news(peer, 0, Alive)],
                demand: Vec::new(),
            };
            deliver(&mut n, peer, hello, now);
            while n.members().status(PeerId::new(peer)).is_some() {
                now += TICK;
                n.handle(Event::Tick, now);
            }
        }
        assert!(n.last_heard.is_empty(), "{:?}", n.last_heard);
    }

    #[test]
    fn a_stranger_that_does_not_introduce_itself_leaves_no_trace() {
        let mut n = node_with([1]);
        let anonymous = Message::Ping {
            seq: 1,
            updates: Vec::new(),
            demand: Vec::new(),
        };
        deliver(&mut n, 9, anonymous, 0);
        assert!(!n.last_heard.contains_key(&PeerId::new(9)));
    }

    #[test]
    fn a_peer_that_leaves_the_cluster_is_forgotten() {
        let mut n = node_with([1]);
        // HoldDown would keep counting it; see the partition tests.
        n.set_partition_policy(PartitionPolicy::Optimistic);
        deliver(&mut n, 1, reporting(1, 3), 0);
        // Peer 1 never answers: suspected after a round, dead once the
        // suspicion times out, well before its demand would go stale.
        let mut now = 0;
        while n.members().status(PeerId::new(1)) != Some(Dead) {
            now += TICK;
            n.handle(Event::Tick, now);
        }
        assert!(now < stale_after_rounds(2) * PERIOD, "{now}");
        assert!(n.peer_demand().is_empty());
    }

    #[test]
    fn under_hold_down_a_peer_that_left_still_counts_until_the_hold_ends() {
        let hold = 30 * PERIOD;
        let mut n = node_with([1]);
        n.set_partition_policy(PartitionPolicy::HoldDown(hold));
        deliver(&mut n, 1, reporting(1, 3), 0);
        let mut now = 0;
        while n.members().status(PeerId::new(1)) != Some(Dead) {
            now += TICK;
            n.handle(Event::Tick, now);
        }
        n.handle(Event::Tick, now + hold - PERIOD);
        assert!(!n.peer_demand().is_empty(), "held");
        n.handle(Event::Tick, now + hold + PERIOD);
        assert!(n.peer_demand().is_empty(), "released");
    }

    #[test]
    fn a_peer_not_heard_from_for_too_long_goes_stale() {
        let patient = SwimConfig {
            suspicion_timeout: 1000 * PERIOD,
            tombstone_ttl: 1000 * PERIOD,
            ..SWIM
        };
        let mut n = Node::new(config(), patient, LOCAL, addr(LOCAL.get()), 1);
        n.set_partition_policy(PartitionPolicy::Optimistic);
        n.introduce(PeerId::new(1), addr(1));
        deliver(&mut n, 1, reporting(1, 3), 0);

        let stale = stale_after_rounds(2);
        for k in 0..stale {
            n.handle(Event::Tick, k * PERIOD);
        }
        assert!(!n.peer_demand().is_empty(), "still within the threshold");
        n.handle(Event::Tick, stale * PERIOD);
        assert!(n.peer_demand().is_empty());
    }

    // A two-node cluster where peer 1 stays alive however silent it is,
    // and KEY is hot here from round 5 on.
    fn hot_pair() -> Node {
        let patient = SwimConfig {
            suspicion_timeout: 1000 * PERIOD,
            tombstone_ttl: 1000 * PERIOD,
            ..SWIM
        };
        let mut n = Node::new(config(), patient, LOCAL, addr(LOCAL.get()), 1);
        n.introduce(PeerId::new(1), addr(1));
        heat(&mut n);
        n
    }

    // Runs rounds `from..=to`, peer 1 reporting `demand` for KEY before
    // each, or nothing at all.
    fn rounds_with_peer(n: &mut Node, from: u64, to: u64, demand: Option<f32>) {
        for k in from..=to {
            if let Some(demand) = demand {
                let mut report = reporting(1, k as u16);
                if let Message::Ping { demand: d, .. } = &mut report {
                    d[0].keys[0].demand = demand;
                }
                deliver(n, 1, report, k * PERIOD);
            }
            n.handle(Event::Tick, k * PERIOD);
        }
    }

    fn rate_of(n: &mut Node, now: Nanos) -> f64 {
        ONE_SEC as f64 / measured_interval(n, now) as f64
    }

    #[test]
    fn a_new_node_stays_at_the_floor_until_it_has_heard_its_peers() {
        let mut n = hot_pair();
        rounds_with_peer(&mut n, 1, 6, None);
        assert!(n.limiter().is_hot(KEY));
        // Its peers have not heard its demand either: they keep for it no
        // more than the floor, R × β / 2.
        assert_eq!(rate_of(&mut n, 6 * PERIOD + 1), 50.0);
    }

    #[test]
    fn hearing_every_peer_ends_the_learning() {
        let mut n = hot_pair();
        rounds_with_peer(&mut n, 1, 2, Some(100.0));
        // Learned well before stale_after_rounds: a cold key gets α × R / 2.
        assert_eq!(cold_rate_after_round(&mut n, 2 * PERIOD + 1), 250.0);
    }

    #[test]
    fn a_key_that_turns_hot_stays_at_the_floor_until_every_peer_is_heard_again() {
        let mut n = hot_pair();
        // The key turns hot in round 1, after that round's report.
        rounds_with_peer(&mut n, 1, 1, Some(100.0));
        assert_eq!(n.limiter().hot_since(KEY), Some(PERIOD));
        assert_eq!(
            rate_of(&mut n, PERIOD + 1),
            50.0,
            "the peer may not know yet"
        );

        // Heard since in round 2; the higher share is applied after
        // RISE_DELAY_ROUNDS more.
        let up = 2 + RISE_DELAY_ROUNDS as u64;
        rounds_with_peer(&mut n, 2, up - 1, Some(100.0));
        assert_eq!(rate_of(&mut n, (up - 1) * PERIOD + 1), 50.0, "not yet");
        rounds_with_peer(&mut n, up, up, Some(100.0));
        assert!(rate_of(&mut n, up * PERIOD + 1) > 100.0, "heard since");
    }

    #[test]
    fn a_peer_never_heard_from_does_not_keep_a_node_learning_forever() {
        let mut n = hot_pair();
        let up = stale_after_rounds(2) + 1 + RISE_DELAY_ROUNDS as u64;
        rounds_with_peer(&mut n, 1, up, None);
        assert!(rate_of(&mut n, up * PERIOD + 1) > 100.0);
    }

    #[test]
    fn alone_with_demand_a_node_takes_all_but_its_peers_floor() {
        let mut n = hot_pair();
        let up = stale_after_rounds(2) + 1 + RISE_DELAY_ROUNDS as u64;
        rounds_with_peer(&mut n, 1, up, None);
        // 1000 × 0.1 / 2 = 50 left as the peer's floor.
        let rate = rate_of(&mut n, up * PERIOD + 1);
        assert!((rate - 950.0).abs() < 1.0, "{rate}");
    }

    #[test]
    fn a_busy_peer_leaves_a_node_little_more_than_its_floor() {
        let mut n = hot_pair();
        let learned = stale_after_rounds(2);
        rounds_with_peer(&mut n, 1, learned + 1, Some(1e6));
        let rate = rate_of(&mut n, (learned + 1) * PERIOD + 1);
        assert!((rate - 50.0).abs() < 1.0, "{rate}");
    }

    // Peer 1 holds KEY primary, or only secondary; this node sees 50
    // attempts a second, under its hot threshold of 250.
    fn lukewarm_with_peer(primary: bool) -> Node {
        let mut n = node_with([1]);
        n.handle(Event::Tick, 0);
        for k in 1..=5u64 {
            for _ in 0..10 {
                n.check(KEY, k * PERIOD - 1);
            }
            let mut report = reporting(1, k as u16);
            if let Message::Ping { demand, .. } = &mut report {
                demand[0].keys[0].primary = primary;
            }
            deliver(&mut n, 1, report, k * PERIOD - 1);
            n.handle(Event::Tick, k * PERIOD);
        }
        n
    }

    #[test]
    fn a_key_a_peer_holds_hot_is_hot_here_and_reported_secondary() {
        let mut n = lukewarm_with_peer(true);
        assert!(n.limiter().is_hot(KEY));
        let out = sent(n.handle(Event::Tick, 6 * PERIOD));
        let [key] = own_report(&out[0].1).keys.as_slice() else {
            panic!("one hot key, one entry");
        };
        assert_eq!(key.key_hash, KEY);
        assert!(!key.primary, "hot here only because of the peer");
    }

    #[test]
    fn a_key_a_peer_holds_only_secondary_stays_cold_here() {
        let n = lukewarm_with_peer(false);
        assert!(!n.limiter().is_hot(KEY));
    }

    // Four peers that never answer: all of them dead once the suspicion
    // times out. Returns the time they are all buried.
    fn abandoned(policy: PartitionPolicy) -> (Node, Nanos) {
        let mut n = node_with([1, 2, 3, 4]);
        n.set_partition_policy(policy);
        let mut now = 0;
        while n.cluster_size() > 1 {
            now += TICK;
            n.handle(Event::Tick, now);
        }
        (n, now)
    }

    // The rate a key new at `now` gets: after the next round, to let the
    // policy see the latest membership.
    fn cold_rate_after_round(n: &mut Node, now: Nanos) -> f64 {
        let round = now.next_multiple_of(PERIOD);
        n.handle(Event::Tick, round);
        ONE_SEC as f64 / measured_interval_of(n, KEY + 7, round) as f64
    }

    #[test]
    fn a_node_that_regains_its_quorum_stays_at_the_floor_until_it_is_heard() {
        let (mut n, buried) = abandoned(PartitionPolicy::Quorum);
        // The peers come back: every one of them says so.
        let back = buried.next_multiple_of(PERIOD) + PERIOD;
        for peer in 1..=4 {
            let hello = Message::Ping {
                seq: 1,
                updates: vec![news(peer, 1, Alive)],
                demand: Vec::new(),
            };
            deliver(&mut n, peer, hello, back);
        }
        assert_eq!(cold_rate_after_round(&mut n, back), 20.0, "just regained");

        // From now on the peers answer every probe, refuting any suspicion
        // the unanswered rounds above raised.
        let mut now = back.next_multiple_of(PERIOD);
        while now < back + stale_after_rounds(5) * PERIOD {
            now += PERIOD;
            for (peer, message) in sent(n.handle(Event::Tick, now)) {
                if let Message::Ping { seq, .. } = message {
                    let answer = Message::Ack {
                        seq,
                        updates: vec![news(peer.get(), 5, Alive)],
                        demand: Vec::new(),
                    };
                    deliver(&mut n, peer.get(), answer, now + 1);
                }
            }
        }
        assert_eq!(n.cluster_size(), 5);
        assert_eq!(cold_rate_after_round(&mut n, now + 1), 100.0, "α × R / 5");
    }

    fn waiting_for(policy: PartitionPolicy) -> Node {
        let mut n = node();
        n.set_partition_policy(policy);
        n.add_seed(PeerId::new(1), addr(1));
        n
    }

    #[test]
    fn a_node_that_has_not_joined_yet_keeps_every_key_at_the_floor() {
        let mut n = waiting_for(PartitionPolicy::default());
        // R × β over itself and its one seed, not α × R alone.
        assert_eq!(rate_of(&mut n, 0), 50.0, "before any round");
        assert_eq!(cold_rate_after_round(&mut n, 3 * PERIOD), 50.0);
    }

    #[test]
    fn an_optimistic_node_serves_before_it_joins() {
        let mut n = waiting_for(PartitionPolicy::Optimistic);
        assert_eq!(cold_rate_after_round(&mut n, 0), 500.0, "α × R alone");
    }

    #[test]
    fn joining_lifts_the_floor() {
        let mut n = waiting_for(PartitionPolicy::default());
        n.handle(Event::Tick, 0);
        let hello = Message::Ack {
            seq: 1,
            updates: vec![news(1, 0, Alive)],
            demand: Vec::new(),
        };
        deliver(&mut n, 1, hello, 1);
        assert_eq!(n.cluster_size(), 2);
        assert_eq!(
            cold_rate_after_round(&mut n, 1),
            50.0,
            "joined, but the peer not heard since: R × β / 2"
        );

        deliver(&mut n, 1, ack(9), PERIOD + 1);
        assert_eq!(
            cold_rate_after_round(&mut n, PERIOD + 1),
            250.0,
            "α × R / 2"
        );
    }

    #[test]
    fn a_node_alone_without_seeds_is_a_cluster_of_one() {
        let mut n = node();
        assert_eq!(cold_rate_after_round(&mut n, 0), 500.0);
    }

    #[test]
    fn the_default_policy_is_a_ten_second_hold_down() {
        assert_eq!(
            node().partition_policy(),
            PartitionPolicy::HoldDown(10 * ONE_SEC)
        );
    }

    #[test]
    fn optimistic_follows_the_surviving_cluster_at_once() {
        let (mut n, buried) = abandoned(PartitionPolicy::Optimistic);
        // Alone: α × R / 1.
        assert_eq!(cold_rate_after_round(&mut n, buried), 500.0);
    }

    #[test]
    fn hold_down_keeps_the_old_size_for_its_duration() {
        let hold = 20 * PERIOD;
        let (mut n, buried) = abandoned(PartitionPolicy::HoldDown(hold));
        assert_eq!(cold_rate_after_round(&mut n, buried), 100.0, "α × R / 5");
        assert_eq!(
            cold_rate_after_round(&mut n, buried + hold + PERIOD),
            500.0,
            "α × R / 1"
        );
    }

    #[test]
    fn without_a_quorum_every_key_drops_to_the_floor() {
        let (mut n, buried) = abandoned(PartitionPolicy::Quorum);
        // R × β / 5, the five the node knows of, the dead included.
        assert_eq!(cold_rate_after_round(&mut n, buried), 20.0);
    }

    // Forgetting the dead must not hand the minority a majority of itself:
    // it cannot tell a split from a death, so it keeps counting them.
    #[test]
    fn a_node_without_a_quorum_stays_at_the_floor_after_the_dead_are_forgotten() {
        let (mut n, buried) = abandoned(PartitionPolicy::Quorum);
        let mut now = buried;
        while n.members().dead().count() > 0 {
            now += TICK;
            n.handle(Event::Tick, now);
        }
        assert_eq!(cold_rate_after_round(&mut n, now), 20.0, "R × β / 5");
    }

    // Ticks until `until`, answering the probes to the `answering` peers;
    // the others are gone.
    fn run_answering(n: &mut Node, answering: &[u64], from: Nanos, until: Nanos) {
        let mut now = from;
        while now < until {
            now += TICK;
            for (peer, message) in sent(n.handle(Event::Tick, now)) {
                if let Message::Ping { seq, .. } = message
                    && answering.contains(&peer.get())
                {
                    let answer = Message::Ack {
                        seq,
                        updates: vec![news(peer.get(), 0, Alive)],
                        demand: Vec::new(),
                    };
                    deliver(n, peer.get(), answer, now + 1);
                }
            }
        }
    }

    // A majority may shrink the cluster it counts: a cluster scaled down
    // less than half at a time keeps its quorum all the way.
    #[test]
    fn a_cluster_shrunk_a_minority_at_a_time_keeps_its_quorum() {
        let mut n = node_with([1, 2, 3, 4]);
        n.set_partition_policy(PartitionPolicy::Quorum);
        // 3 and 4 go for good: three of five, then, once they are
        // forgotten, a cluster of three.
        let forgotten = SWIM.suspicion_timeout + SWIM.tombstone_ttl + 10 * PERIOD;
        run_answering(&mut n, &[1, 2], 0, forgotten);
        assert_eq!(n.members().dead().count(), 0);

        // Then 2 goes as well: two of three.
        let end = forgotten + SWIM.suspicion_timeout + 10 * PERIOD;
        run_answering(&mut n, &[1], forgotten, end);
        assert_eq!(n.cluster_size(), 2);
        assert_eq!(cold_rate_after_round(&mut n, end), 250.0, "α × R / 2");
    }

    #[test]
    fn a_member_heard_of_through_gossip_is_reachable() {
        let mut n = node_with([1]);
        let rumor = Message::Ack {
            seq: 9,
            updates: vec![news(1, 0, Alive), news(5, 0, Alive)],
            demand: Vec::new(),
        };
        deliver(&mut n, 1, rumor, 0);
        assert_eq!(n.address(PeerId::new(5)), Some(addr(5)));
    }

    #[test]
    fn a_seed_is_reachable_before_it_answers() {
        let mut n = node();
        n.add_seed(PeerId::new(3), addr(3));
        assert_eq!(n.address(PeerId::new(3)), Some(addr(3)));
    }

    #[test]
    fn a_node_fed_attempts_hands_out_quotas() {
        let mut n = node();
        n.handle(Event::Tick, 0);
        n.record_attempts(KEY, 2000, 0);
        assert!(n.is_tracked(KEY));
        n.handle(Event::Tick, ONE_SEC);
        assert!(n.limiter().is_hot(KEY));
        // Alone, nobody to learn from: the share is the whole limit.
        assert_eq!(n.quota(KEY), Some(Quota::new(1000, config().burst)));
    }

    #[test]
    fn a_waiting_node_starts_new_keys_at_the_floor() {
        let mut n = node();
        n.add_seed(PeerId::new(1), addr(1));
        // R × β over itself and its seed; the test config has β = 0.1.
        assert_eq!(n.new_key_quota(), Quota::new(50, config().burst));
    }

    #[test]
    fn a_node_expecting_peers_waits_with_no_seed_at_all() {
        let mut n = node();
        n.expect_peers();
        // R × β over itself alone; the test config has β = 0.1.
        assert_eq!(n.new_key_quota(), Quota::new(100, config().burst));
        n.add_seed(PeerId::new(1), addr(1));
        n.remove_seed(PeerId::new(1));
        assert_eq!(n.new_key_quota(), Quota::new(100, config().burst), "still");
    }

    #[test]
    fn a_node_expecting_peers_stops_waiting_once_it_joins() {
        let mut n = node();
        n.expect_peers();
        n.add_seed(PeerId::new(5), addr(5));
        let (_, seq) = round(&mut n, 0);
        let answer = Message::Ack {
            seq,
            updates: vec![news(5, 0, Alive)],
            demand: Vec::new(),
        };
        deliver(&mut n, 5, answer, ONE_MS);
        n.handle(Event::Tick, PERIOD);
        assert_ne!(n.new_key_quota(), Quota::new(100, config().burst));
    }

    #[test]
    fn a_removed_seed_is_neither_reached_nor_waited_for() {
        let mut n = node();
        n.add_seed(PeerId::new(1), addr(1));
        n.add_seed(PeerId::new(2), addr(2));
        n.remove_seed(PeerId::new(1));
        assert_eq!(n.address(PeerId::new(1)), None);
        // R × β over itself and the seed left.
        assert_eq!(n.new_key_quota(), Quota::new(50, config().burst));
        n.remove_seed(PeerId::new(2));
        assert_eq!(cold_rate_after_round(&mut n, 0), 500.0, "alone again");
    }

    #[test]
    fn a_removed_seed_that_answered_stays_a_member() {
        let mut n = node();
        n.add_seed(PeerId::new(5), addr(5));
        let (_, seq) = round(&mut n, 0);
        let answer = Message::Ack {
            seq,
            updates: vec![news(5, 0, Alive)],
            demand: Vec::new(),
        };
        deliver(&mut n, 5, answer, ONE_MS);
        n.remove_seed(PeerId::new(5));
        assert_eq!(n.cluster_size(), 2);
        assert_eq!(n.address(PeerId::new(5)), Some(addr(5)));
    }

    // A node can hear itself: a seed list naming it under another address,
    // or a transport that loops back. It is not its own peer.
    // A hold-down as long as time itself: nothing may overflow.
    #[test]
    fn an_endless_hold_down_overflows_nothing() {
        let (mut n, buried) = abandoned(PartitionPolicy::HoldDown(u64::MAX));
        assert_eq!(
            cold_rate_after_round(&mut n, buried),
            100.0,
            "α × R / 5, held"
        );
    }

    #[test]
    fn a_message_from_the_node_itself_is_ignored() {
        let mut n = node_with([1]);
        let echo = reporting(LOCAL.get(), 3);
        assert!(
            deliver(&mut n, LOCAL.get(), echo, 0).is_empty(),
            "no answer to itself"
        );
        assert!(n.peer_demand().is_empty(), "its own demand is not a peer's");
        assert_eq!(n.cluster_size(), 2);
    }

    #[test]
    fn a_peer_reporting_its_own_demand_is_heard() {
        let mut n = node_with([1]);
        let report = |origin: u64| DemandReport {
            origin,
            epoch: 0,
            round: 3,
            keys: vec![KeyDemand {
                key_hash: KEY,
                demand: 75.0,
                primary: true,
            }],
        };
        let ping = |origin| Message::Ping {
            seq: 7,
            updates: Vec::new(),
            demand: vec![report(origin)],
        };
        deliver(&mut n, 1, ping(1), 0);
        let heard = n.peer_demand().get(PeerId::new(1), KEY).unwrap();
        assert_eq!((heard.demand, heard.round), (75.0, 3));

        deliver(&mut n, 1, ping(2), 0);
        assert_eq!(
            n.peer_demand().get(PeerId::new(2), KEY),
            None,
            "demand is taken first hand only"
        );
    }
}
