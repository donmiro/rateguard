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
    /// How long ago the last protocol round ran; `None` before the first.
    /// Far over the protocol period, the background task is stuck.
    pub since_last_round: Option<Duration>,
    /// Whether the background task runs: false once the node has left, or
    /// if the task died, a bug. The counts stop then.
    pub running: bool,
    /// Whether this is a dry run: see
    /// [`Guard::set_dry_run`](crate::Guard::set_dry_run).
    pub dry_run: bool,
    /// Up to ten keys with the most denials here lately, most denied first:
    /// who is being limited. Keys go by their hash; match them with
    /// [`Guard::key_hash_in`](crate::Guard::key_hash_in), which knows this
    /// node's [`Builder::key_salt`](crate::Builder::key_salt).
    pub most_denied: Vec<DeniedKey>,
    /// Checks in a class this node does not know, held to the default limit
    /// instead: a typo, or a class removed while still in use.
    pub unknown_class: u64,
    /// How this node signs its gossip and checks its peers': see
    /// [`Builder::gossip_auth`](crate::Builder::gossip_auth).
    pub gossip_auth: GossipAuth,
    /// Datagrams refused before the protocol saw them, by reason. A node
    /// whose secrets do not match its peers' shows here: it hears nobody,
    /// and nobody hears it.
    pub rejected_datagrams: RejectedDatagrams,
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
    /// bytes that do not decode, or a message that names no sender.
    /// Counted in every mode, `Off` included.
    pub malformed: u64,
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
