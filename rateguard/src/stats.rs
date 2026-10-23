//! What a node tells about itself, for dashboards and health checks.

use std::time::Duration;

use rateguard_proto::auth::GossipAuth;

/// A node's state, as of the last tick of its background task: a quarter
/// of the protocol period ago at most, 50 ms by default. See
/// [`Guard::stats`](crate::Guard::stats).
///
/// The counts are this instance's alone, since it started. A check that
/// lands on a slot just as the background task hands it to another key may
/// go uncounted.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Stats {
    /// Requests admitted. In a dry run, every request but those counted in
    /// `denied`.
    pub admitted: u64,
    /// Requests denied; in a dry run, those that would have been.
    pub denied: u64,
    /// Checks on keys that found no place of their own in the key table and
    /// shared one allowance at the cold rate: if this grows, raise
    /// `tracked_keys`.
    pub overflowed: u64,
    /// Members this node sees, itself included, as
    /// [`Guard::cluster_size`](crate::Guard::cluster_size).
    pub cluster_size: usize,
    /// The N the limit is shared out over: `cluster_size`, or more under
    /// `HoldDown` while a member lost is still held, which keeps the fleet
    /// under the limit, not over it.
    pub counted_size: usize,
    /// Members under suspicion: not heard from, not yet declared dead.
    pub suspects: usize,
    /// Keys this node keeps state for.
    pub tracked_keys: usize,
    /// Keys coordinated with the peers.
    pub hot_keys: usize,
    /// Every key is at the floor `R × β / N` because the node has seeds and
    /// has joined nobody yet.
    pub joining: bool,
    /// Every key is at the floor because the node has joined, but its peers
    /// may not have heard its demand yet.
    pub learning: bool,
    /// Every key is at the floor because, under `Quorum`, the node has no
    /// majority, or regained it too recently to have been heard.
    pub without_quorum: bool,
    /// Every key is at the floor `R × β / max_members` because the member
    /// table, full, refused a live newcomer within the last
    /// `tombstone_ttl`: there are nodes this one cannot count. See
    /// [`members_refused`](Stats::members_refused) and
    /// [`Builder::max_members`](crate::Builder::max_members).
    pub members_full: bool,
    /// How long ago the last protocol round ran; `None` before the first.
    /// Far over the protocol period, the background task is stuck.
    pub since_last_round: Option<Duration>,
    /// Whether the background task runs: false once the node has left, or
    /// if the task died, a bug. The counts stop then.
    pub running: bool,
    /// Whether this is a dry run: see
    /// [`Guard::set_dry_run`](crate::Guard::set_dry_run).
    pub dry_run: bool,
    /// The keys with the most denials here lately, most denied first: who
    /// is being limited. As many as
    /// [`Builder::most_denied`](crate::Builder::most_denied) says, ten
    /// unless set. Keys go by their hash; match them with
    /// [`Guard::key_hash_in`](crate::Guard::key_hash_in), which knows this
    /// node's [`Builder::key_salt`](crate::Builder::key_salt).
    ///
    /// Ranked by denials over the last ten seconds or so, the time before
    /// a key was first seen counted as silent: a key denied once just now
    /// ranks below one denied as often for ten seconds. The rates listed
    /// are a new key's so far.
    pub most_denied: Vec<DeniedKey>,
    /// The keys whose shadow limit would have denied the most here lately,
    /// most first, as many as `most_denied`: see
    /// [`Guard::set_shadow`](crate::Guard::set_shadow).
    pub most_shadow_denied: Vec<DeniedKey>,
    /// Checks in a class this node does not know, held to the default limit
    /// instead: a typo, or a class removed while still in use.
    pub unknown_class: u64,
    /// Requests a shadow limit would have denied, whatever the real limit
    /// decided: see [`Guard::set_shadow`](crate::Guard::set_shadow). Not
    /// part of `admitted` or `denied`, which count what was decided.
    pub shadow_denied: u64,
    /// How this node signs its gossip and checks its peers': see
    /// [`Builder::gossip_auth`](crate::Builder::gossip_auth).
    pub gossip_auth: GossipAuth,
    /// Datagrams refused before the protocol saw them, by reason. A node
    /// whose secrets do not match its peers' shows here: it hears nobody,
    /// and nobody hears it.
    pub rejected_datagrams: RejectedDatagrams,
    /// Times this node has had to outbid an `Alive` record of itself that
    /// it did not make: a newer incarnation of its own ID, spread by
    /// another. A restart quicker than the peers notice adds one, as they
    /// tell the new instance where the old one stood. Steady growth means
    /// two instances advertise one address, and each keeps outbidding the
    /// other.
    pub identity_conflicts: u64,
    /// Times news of a member not seen before was refused because the
    /// member table was full; see
    /// [`Builder::max_members`](crate::Builder::max_members). Steady growth
    /// means a cluster bigger than the cap, or a stranger flooding the
    /// gossip with made-up members.
    pub members_refused: u64,
}

/// Datagrams refused since the node started; see
/// [`Stats::rejected_datagrams`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RejectedDatagrams {
    /// Not signed, under [`GossipAuth::Required`]: a peer not signing yet,
    /// or a stranger.
    pub unsigned: u64,
    /// Signed with no secret this node holds, or altered on the way: a peer
    /// with other secrets, mid-rotation or misconfigured, or a forgery.
    /// A datagram longer than `MAX_DATAGRAM`, 1400 bytes, which a
    /// conforming peer never sends, arrives cut short and is counted here
    /// if signed.
    pub bad_tag: u64,
    /// No message at all: a frame cut short or of an unknown version,
    /// bytes that do not decode, a message that names no sender, or one
    /// whose sender's own record, the first, names it by an ID its address
    /// does not give. Counted in every mode, `Off` included.
    pub malformed: u64,
    /// Not datagrams but records: news after the sender's own record that
    /// names a member by an ID its address does not give, dropped while
    /// the rest of the datagram is taken. A made-up member, or a borrowed
    /// name, that a peer passed on as it heard it; a 0.7.0 peer does that
    /// with a forger's news. Counted in every mode, `Off` included.
    pub mismatched_records: u64,
}

/// A key denied lately; see [`Stats::most_denied`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DeniedKey {
    /// The key, as [`Guard::key_hash_in`](crate::Guard::key_hash_in)
    /// hashes it with its class.
    pub key_hash: u64,
    /// Denials a second here, averaged over the last ten seconds or so.
    pub denied_per_sec: f64,
    /// Requests a second here, admitted or not, over the same window: the
    /// denials are this much of them.
    pub requests_per_sec: f64,
    /// Requests a second the key's shadow limit would have denied, over the
    /// same window.
    pub shadow_denied_per_sec: f64,
}

/// What this node knows of one key; see
/// [`Guard::key_stats`](crate::Guard::key_stats).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct KeyStats {
    /// Requests a second here, admitted or not, averaged over the last ten
    /// seconds or so.
    pub requests_per_sec: f64,
    /// Of those, the ones denied a second, over the same window.
    pub denied_per_sec: f64,
    /// The ones the key's shadow limit would have denied a second, over the
    /// same window.
    pub shadow_denied_per_sec: f64,
    /// The rate, in requests per second, this instance admits for the key
    /// now: see [`Guard::share_in`](crate::Guard::share_in).
    pub share: f64,
    /// The limit the key is held to, as given: its own, or its class's.
    pub limit: crate::Limit,
    /// Its shadow limit, if it has one.
    pub shadow: Option<crate::Limit>,
}

/// What the background task keeps for [`Stats`], under a lock only it and
/// `Guard::stats` take.
#[derive(Debug, Default)]
pub(crate) struct Report {
    /// Everything but the fields filled in when asked.
    pub stats: Stats,
    /// Every check counted, admitted or denied.
    pub attempts: u64,
    /// When the last round ran, in the node's nanoseconds.
    pub last_round: Option<u64>,
}
