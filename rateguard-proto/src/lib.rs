//! The rateguard wire format: SWIM messages, encoded with `postcard`.
//!
//! Every message fits one UDP datagram of [`MAX_DATAGRAM`] bytes. The
//! membership part is capped at [`MAX_UPDATES`] updates, under 460 bytes in
//! the worst case, IPv6 addresses included; the rest is left to the demand
//! of the allocation layer, capped at [`MAX_REPORTS`] report of at most
//! [`MAX_DEMAND_KEYS`] keys.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The largest datagram, in bytes: under a typical 1500-byte MTU with room
/// for IP and UDP headers, so nothing is ever fragmented.
pub const MAX_DATAGRAM: usize = 1400;
/// The most membership updates one message may carry.
pub const MAX_UPDATES: usize = 12;
/// The most demand reports one message may carry. Demand is exchanged first
/// hand (spec §10.8): a message carries the sender's report only. The format
/// still groups by origin, for aggregation later.
pub const MAX_REPORTS: usize = 1;
/// The most keys one message may carry demand for, over all its reports.
pub const MAX_DEMAND_KEYS: usize = 64;

/// A member's state. The declaration order is the order of precedence
/// within one incarnation, and it is relied on: do not reorder.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Status {
    Alive,
    Suspect,
    Dead,
}

/// Where a member listens. Its own type rather than `SocketAddr`: the wire
/// format is this crate's to keep stable, the serde form of `SocketAddr` is
/// not. IPv6 flow info and scope are not carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Address {
    V4([u8; 4], u16),
    V6([u8; 16], u16),
}
impl From<SocketAddr> for Address {
    fn from(socket: SocketAddr) -> Self {
        match socket.ip() {
            IpAddr::V4(ip) => Address::V4(ip.octets(), socket.port()),
            IpAddr::V6(ip) => Address::V6(ip.octets(), socket.port()),
        }
    }
}
impl From<Address> for SocketAddr {
    fn from(address: Address) -> Self {
        match address {
            Address::V4(ip, port) => SocketAddr::new(Ipv4Addr::from(ip).into(), port),
            Address::V6(ip, port) => SocketAddr::new(Ipv6Addr::from(ip).into(), port),
        }
    }
}

/// A piece of news about one member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Update {
    pub member: u64,
    /// Where the member listens; gossiped with it so that whoever hears of a
    /// member can reach it.
    pub addr: Address,
    pub incarnation: u32,
    pub status: Status,
}
impl Update {
    /// Whether this news is newer than `other`: a higher incarnation wins,
    /// and within one incarnation `Dead` beats `Suspect` beats `Alive`.
    /// News never supersedes itself, so an echo is never mistaken for news.
    ///
    /// # Panics
    ///
    /// If the two are about different members.
    pub fn supersedes(&self, other: &Update) -> bool {
        assert_eq!(
            self.member, other.member,
            "only updates about the same member are comparable"
        );
        (self.incarnation, self.status) > (other.incarnation, other.status)
    }
}

/// The demand for one key, as one node observed it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct KeyDemand {
    /// The key, hashed: the string never leaves the node that saw it.
    /// Fixed width, because a uniform hash would take 9 or 10 bytes as a
    /// varint.
    #[serde(with = "postcard::fixint::le")]
    pub key_hash: u64,
    /// Attempts per second, admitted or not. Finite and non-negative.
    pub demand: f32,
    /// The key is hot at `origin` by its own demand. Only such news makes
    /// the key hot elsewhere; without the flag two nodes would keep it hot
    /// for each other forever.
    pub primary: bool,
}

/// What one node, `origin`, observed in one of its rounds. Absolute values,
/// never deltas: a newer round of the same origin replaces the older one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DemandReport {
    pub origin: u64,
    /// Drawn at random when `origin` starts. Rounds order the reports of one
    /// epoch only: a new epoch is the origin restarted, counting from 0.
    pub epoch: u32,
    pub round: u16,
    pub keys: Vec<KeyDemand>,
}

/// A SWIM message. Every one carries membership news and demand. The
/// variant order is the wire format: do not reorder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// Are you alive? The answer is an `Ack` with the same `seq`.
    Ping {
        seq: u32,
        updates: Vec<Update>,
        demand: Vec<DemandReport>,
    },
    /// The answer to a `Ping`, or to a `PingReq` relayed by a helper.
    Ack {
        seq: u32,
        updates: Vec<Update>,
        demand: Vec<DemandReport>,
    },
    /// Probe `target` for me: I could not reach it myself. If it answers,
    /// the helper sends me an `Ack` with this `seq`.
    PingReq {
        seq: u32,
        target: u64,
        updates: Vec<Update>,
        demand: Vec<DemandReport>,
    },
}
impl Message {
    pub fn seq(&self) -> u32 {
        match self {
            Message::Ping { seq, .. } | Message::Ack { seq, .. } | Message::PingReq { seq, .. } => {
                *seq
            }
        }
    }

    pub fn updates(&self) -> &[Update] {
        match self {
            Message::Ping { updates, .. }
            | Message::Ack { updates, .. }
            | Message::PingReq { updates, .. } => updates,
        }
    }

    pub fn demand(&self) -> &[DemandReport] {
        match self {
            Message::Ping { demand, .. }
            | Message::Ack { demand, .. }
            | Message::PingReq { demand, .. } => demand,
        }
    }
}

/// Why a message could not be encoded or decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// More than [`MAX_UPDATES`] updates.
    TooManyUpdates(usize),
    /// More than [`MAX_REPORTS`] demand reports.
    TooManyReports(usize),
    /// More than [`MAX_DEMAND_KEYS`] keys over all demand reports.
    TooManyDemandKeys(usize),
    /// A demand that is negative, infinite or NaN.
    InvalidDemand,
    /// Larger than [`MAX_DATAGRAM`] bytes.
    TooLarge(usize),
    /// Not a rateguard message.
    Malformed,
    /// A valid message followed by extra bytes.
    TrailingBytes(usize),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::TooManyUpdates(n) => write!(f, "{n} updates, at most {MAX_UPDATES} fit"),
            Error::TooManyReports(n) => {
                write!(f, "{n} demand reports, at most {MAX_REPORTS} fit")
            }
            Error::TooManyDemandKeys(n) => {
                write!(f, "demand for {n} keys, at most {MAX_DEMAND_KEYS} fit")
            }
            Error::InvalidDemand => write!(f, "a demand is negative, infinite or NaN"),
            Error::TooLarge(n) => write!(f, "{n} bytes, the datagram limit is {MAX_DATAGRAM}"),
            Error::Malformed => write!(f, "not a rateguard message"),
            Error::TrailingBytes(n) => write!(f, "{n} bytes left after the message"),
        }
    }
}
impl std::error::Error for Error {}

/// Encodes a message into one datagram.
pub fn encode(message: &Message) -> Result<Vec<u8>, Error> {
    check_limits(message)?;
    let bytes = postcard::to_allocvec(message).expect("a Message always serialized");
    if bytes.len() > MAX_DATAGRAM {
        return Err(Error::TooLarge(bytes.len()));
    }
    Ok(bytes)
}

/// Decodes one datagram. Anything that is not exactly one valid message is
/// refused: the bytes come from the network.
pub fn decode(bytes: &[u8]) -> Result<Message, Error> {
    if bytes.len() > MAX_DATAGRAM {
        return Err(Error::TooLarge(bytes.len()));
    }
    let (message, rest) =
        postcard::take_from_bytes::<Message>(bytes).map_err(|_| Error::Malformed)?;
    if !rest.is_empty() {
        return Err(Error::TrailingBytes(rest.len()));
    }
    check_limits(&message)?;
    Ok(message)
}

fn check_limits(message: &Message) -> Result<(), Error> {
    let n = message.updates().len();
    if n > MAX_UPDATES {
        return Err(Error::TooManyUpdates(n));
    }
    let reports = message.demand();
    if reports.len() > MAX_REPORTS {
        return Err(Error::TooManyReports(reports.len()));
    }
    let keys: usize = reports.iter().map(|r| r.keys.len()).sum();
    if keys > MAX_DEMAND_KEYS {
        return Err(Error::TooManyDemandKeys(keys));
    }
    let is_rate = |d: f32| d.is_finite() && d >= 0.0;
    if !reports
        .iter()
        .flat_map(|r| &r.keys)
        .all(|k| is_rate(k.demand))
    {
        return Err(Error::InvalidDemand);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u8) -> Address {
        Address::V4([10, 0, 0, n], 7946)
    }

    fn update(incarnation: u32, status: Status) -> Update {
        Update {
            member: 7,
            addr: addr(7),
            incarnation,
            status,
        }
    }

    fn key(key_hash: u64, demand: f32) -> KeyDemand {
        KeyDemand {
            key_hash,
            demand,
            primary: true,
        }
    }

    fn report(keys: Vec<KeyDemand>) -> DemandReport {
        DemandReport {
            origin: 3,
            epoch: 5,
            round: 17,
            keys,
        }
    }

    // The most demand one message may carry, every field at its widest.
    fn worst_demand() -> Vec<DemandReport> {
        let per_report = MAX_DEMAND_KEYS / MAX_REPORTS;
        (0..MAX_REPORTS)
            .map(|_| DemandReport {
                origin: u64::MAX,
                epoch: u32::MAX,
                round: u16::MAX,
                keys: vec![key(u64::MAX, f32::MAX); per_report],
            })
            .collect()
    }

    fn ping_with_demand(demand: Vec<DemandReport>) -> Message {
        Message::Ping {
            seq: 0,
            updates: Vec::new(),
            demand,
        }
    }

    fn worst_updates(n: usize) -> Vec<Update> {
        vec![
            Update {
                member: u64::MAX,
                addr: Address::V6([0xff; 16], u16::MAX),
                incarnation: u32::MAX,
                status: Status::Dead,
            };
            n
        ]
    }

    #[test]
    fn an_address_survives_the_wire_both_ways() {
        use std::net::SocketAddr;
        for text in ["10.0.0.7:7946", "[2001:db8::1]:65535"] {
            let socket: SocketAddr = text.parse().unwrap();
            let address = Address::from(socket);
            assert_eq!(SocketAddr::from(address), socket);

            let message = Message::Ping {
                seq: 1,
                updates: vec![Update {
                    addr: address,
                    ..update(0, Status::Alive)
                }],
                demand: Vec::new(),
            };
            assert_eq!(decode(&encode(&message).unwrap()).unwrap(), message);
        }
    }

    #[test]
    fn every_message_survives_the_wire() {
        let updates = vec![update(3, Status::Suspect), update(0, Status::Alive)];
        for message in [
            Message::Ping {
                seq: 1,
                updates: updates.clone(),
                demand: vec![report(vec![key(11, 2.5), key(12, 0.0)])],
            },
            Message::Ack {
                seq: 2,
                updates: Vec::new(),
                demand: Vec::new(),
            },
            Message::PingReq {
                seq: 3,
                target: 9,
                updates,
                demand: vec![report(vec![key(13, 40.0)])],
            },
        ] {
            let bytes = encode(&message).unwrap();
            assert_eq!(decode(&bytes).unwrap(), message);
        }
    }

    #[test]
    fn membership_leaves_most_of_the_datagram_to_demand() {
        let message = Message::PingReq {
            seq: u32::MAX,
            target: u64::MAX,
            updates: worst_updates(MAX_UPDATES),
            demand: Vec::new(),
        };
        let len = encode(&message).unwrap().len();
        // Twelve IPv6 records leave room for a full report.
        assert!(len <= 460, "{len} bytes");
    }

    #[test]
    fn the_worst_case_datagram_fits_the_mtu() {
        let message = Message::PingReq {
            seq: u32::MAX,
            target: u64::MAX,
            updates: worst_updates(MAX_UPDATES),
            demand: worst_demand(),
        };
        let len = encode(&message).unwrap().len();
        assert!(len <= MAX_DATAGRAM, "{len} bytes");
    }

    #[test]
    fn a_key_hash_takes_eight_bytes_whatever_its_value() {
        // Hashes are uniform, so a varint would take 9 or 10 bytes for
        // almost every key: fixed width is the smaller encoding here.
        let len = |key_hash| {
            encode(&ping_with_demand(vec![report(vec![key(key_hash, 1.0)])]))
                .unwrap()
                .len()
        };
        assert_eq!(len(u64::MAX), len(1));
        assert_eq!(
            len(1)
                - encode(&ping_with_demand(vec![report(Vec::new())]))
                    .unwrap()
                    .len(),
            8 + 4 + 1,
            "key_hash, demand and primary"
        );
    }

    #[test]
    fn one_demand_key_too_many_is_refused_both_ways() {
        let mut demand = worst_demand();
        demand[0].keys.push(key(1, 1.0));
        let message = ping_with_demand(demand);
        let n = MAX_DEMAND_KEYS + 1;
        assert_eq!(encode(&message), Err(Error::TooManyDemandKeys(n)));

        let smuggled = postcard::to_allocvec(&message).unwrap();
        assert_eq!(decode(&smuggled), Err(Error::TooManyDemandKeys(n)));
    }

    #[test]
    fn one_report_too_many_is_refused_both_ways() {
        let message = ping_with_demand(vec![report(Vec::new()); MAX_REPORTS + 1]);
        let n = MAX_REPORTS + 1;
        assert_eq!(encode(&message), Err(Error::TooManyReports(n)));

        let smuggled = postcard::to_allocvec(&message).unwrap();
        assert_eq!(decode(&smuggled), Err(Error::TooManyReports(n)));
    }

    #[test]
    fn a_demand_that_is_not_a_rate_is_refused_both_ways() {
        // One NaN or infinity in a sum of demands poisons every share
        // computed from it, so it is stopped at the wire.
        for bad in [f32::NAN, f32::INFINITY, -1.0] {
            let message = ping_with_demand(vec![report(vec![key(1, 1.0), key(2, bad)])]);
            assert_eq!(encode(&message), Err(Error::InvalidDemand), "{bad}");

            let smuggled = postcard::to_allocvec(&message).unwrap();
            assert_eq!(decode(&smuggled), Err(Error::InvalidDemand), "{bad}");
        }
    }

    #[test]
    fn one_update_too_many_is_refused_both_ways() {
        let message = Message::Ping {
            seq: 0,
            updates: worst_updates(MAX_UPDATES + 1),
            demand: Vec::new(),
        };
        assert_eq!(
            encode(&message),
            Err(Error::TooManyUpdates(MAX_UPDATES + 1))
        );

        let smuggled = postcard::to_allocvec(&message).unwrap();
        assert_eq!(
            decode(&smuggled),
            Err(Error::TooManyUpdates(MAX_UPDATES + 1))
        );
    }

    #[test]
    fn the_variant_order_is_the_wire_format() {
        let ping = Message::Ping {
            seq: 1,
            updates: Vec::new(),
            demand: Vec::new(),
        };
        assert_eq!(encode(&ping).unwrap(), [0, 1, 0, 0]);
    }

    #[test]
    fn garbage_is_rejected_not_guessed() {
        assert_eq!(decode(&[]), Err(Error::Malformed));
        assert_eq!(decode(&[0xde, 0xad, 0xbe, 0xef]), Err(Error::Malformed));
        assert_eq!(decode(&[0, 1, 0, 0, 0xff]), Err(Error::TrailingBytes(1)));
        assert_eq!(
            decode(&vec![0; MAX_DATAGRAM + 1]),
            Err(Error::TooLarge(MAX_DATAGRAM + 1))
        );

        let bytes = encode(&Message::Ack {
            seq: 5,
            updates: vec![update(1, Status::Alive)],
            demand: Vec::new(),
        })
        .unwrap();
        assert_eq!(decode(&bytes[..bytes.len() - 1]), Err(Error::Malformed));
    }

    #[test]
    fn within_one_incarnation_dead_beats_suspect_beats_alive() {
        use Status::*;
        assert!(update(4, Suspect).supersedes(&update(4, Alive)));
        assert!(update(4, Dead).supersedes(&update(4, Suspect)));
        assert!(update(4, Dead).supersedes(&update(4, Alive)));
        assert!(!update(4, Alive).supersedes(&update(4, Suspect)));
    }

    #[test]
    fn a_higher_incarnation_beats_any_status() {
        use Status::*;
        assert!(update(5, Alive).supersedes(&update(4, Suspect)));
        assert!(update(5, Alive).supersedes(&update(4, Dead)));
        assert!(!update(3, Dead).supersedes(&update(4, Alive)));
    }

    #[test]
    fn an_update_does_not_supersede_itself() {
        let u = update(4, Status::Suspect);
        assert!(!u.supersedes(&u), "re-gossiped news must not count as new");
    }

    #[test]
    #[should_panic(expected = "same member")]
    fn updates_about_different_members_are_not_comparable() {
        let other = Update {
            member: 8,
            ..update(0, Status::Alive)
        };
        update(1, Status::Alive).supersedes(&other);
    }

    mod properties {
        use super::super::*;
        use proptest::prelude::*;

        fn update() -> impl Strategy<Value = Update> {
            let address = prop_oneof![
                (any::<[u8; 4]>(), any::<u16>()).prop_map(|(ip, port)| Address::V4(ip, port)),
                (any::<[u8; 16]>(), any::<u16>()).prop_map(|(ip, port)| Address::V6(ip, port)),
            ];
            (
                any::<u64>(),
                address,
                any::<u32>(),
                prop_oneof![
                    Just(Status::Alive),
                    Just(Status::Suspect),
                    Just(Status::Dead)
                ],
            )
                .prop_map(|(member, addr, incarnation, status)| Update {
                    member,
                    addr,
                    incarnation,
                    status,
                })
        }

        fn report() -> impl Strategy<Value = DemandReport> {
            let key = (any::<u64>(), 0.0..=f32::MAX, any::<bool>()).prop_map(
                |(key_hash, demand, primary)| KeyDemand {
                    key_hash,
                    demand,
                    primary,
                },
            );
            let keys = prop::collection::vec(key, 0..=MAX_DEMAND_KEYS / MAX_REPORTS);
            (any::<u64>(), any::<u32>(), any::<u16>(), keys).prop_map(
                |(origin, epoch, round, keys)| DemandReport {
                    origin,
                    epoch,
                    round,
                    keys,
                },
            )
        }

        fn message() -> impl Strategy<Value = Message> {
            let updates = || prop::collection::vec(update(), 0..=MAX_UPDATES);
            let demand = || prop::collection::vec(report(), 0..=MAX_REPORTS);
            prop_oneof![
                (any::<u32>(), updates(), demand()).prop_map(|(seq, updates, demand)| {
                    Message::Ping {
                        seq,
                        updates,
                        demand,
                    }
                }),
                (any::<u32>(), updates(), demand()).prop_map(|(seq, updates, demand)| {
                    Message::Ack {
                        seq,
                        updates,
                        demand,
                    }
                }),
                (any::<u32>(), any::<u64>(), updates(), demand()).prop_map(
                    |(seq, target, updates, demand)| Message::PingReq {
                        seq,
                        target,
                        updates,
                        demand,
                    }
                ),
            ]
        }

        proptest! {
            #[test]
            fn any_bytes_from_the_network_are_decoded_or_refused_never_a_panic(
                bytes in prop::collection::vec(any::<u8>(), 0..2 * MAX_DATAGRAM)
            ) {
                let _ = decode(&bytes);
            }

            #[test]
            fn every_valid_message_survives_the_wire(message in message()) {
                let bytes = encode(&message).unwrap();
                prop_assert!(bytes.len() <= MAX_DATAGRAM);
                prop_assert_eq!(decode(&bytes).unwrap(), message);
            }
        }
    }
}
