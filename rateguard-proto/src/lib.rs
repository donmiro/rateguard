//! The rateguard wire format: SWIM messages, encoded with `postcard`.
//!
//! Every message fits one UDP datagram of [`MAX_DATAGRAM`] bytes. The
//! membership part is capped at [`MAX_UPDATES`] updates, under 300 bytes in
//! the worst case, which leaves the rest of the datagram to the demand
//! entries of the allocation layer.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The largest datagram, in bytes: under a typical 1500-byte MTU with room
/// for IP and UDP headers, so nothing is ever fragmented.
pub const MAX_DATAGRAM: usize = 1400;
/// The most membership updates one message may carry.
pub const MAX_UPDATES: usize = 16;

/// A member's state. The declaration order is the order of precedence
/// within one incarnation, and it is relied on: do not reorder.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Status {
    Alive,
    Suspect,
    Dead,
}

/// A piece of news about one member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Update {
    pub member: u64,
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

/// A SWIM message. Every one carries membership news. The variant order is
/// the wire format: do not reorder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// Are you alive? The answer is an `Ack` with the same `seq`.
    Ping { seq: u32, updates: Vec<Update> },
    /// The answer to a `Ping`, or to a `PingReq` relayed by a helper.
    Ack { seq: u32, updates: Vec<Update> },
    /// Probe `target` for me: I could not reach it myself. If it answers,
    /// the helper sends me an `Ack` with this `seq`.
    PingReq {
        seq: u32,
        target: u64,
        updates: Vec<Update>,
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
}

/// Why a message could not be encoded or decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// More than [`MAX_UPDATES`] updates.
    TooManyUpdates(usize),
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
            Error::TooLarge(n) => write!(f, "{n} bytes, the datagram limit is {MAX_DATAGRAM}"),
            Error::Malformed => write!(f, "not a rateguard message"),
            Error::TrailingBytes(n) => write!(f, "{n} bytes left after the message"),
        }
    }
}
impl std::error::Error for Error {}

/// Encodes a message into one datagram.
pub fn encode(message: &Message) -> Result<Vec<u8>, Error> {
    check_updates(message)?;
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
    check_updates(&message)?;
    Ok(message)
}

fn check_updates(message: &Message) -> Result<(), Error> {
    let n = message.updates().len();
    if n > MAX_UPDATES {
        return Err(Error::TooManyUpdates(n));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(incarnation: u32, status: Status) -> Update {
        Update {
            member: 7,
            incarnation,
            status,
        }
    }

    fn worst_updates(n: usize) -> Vec<Update> {
        vec![
            Update {
                member: u64::MAX,
                incarnation: u32::MAX,
                status: Status::Dead,
            };
            n
        ]
    }

    #[test]
    fn every_message_survives_the_wire() {
        let updates = vec![update(3, Status::Suspect), update(0, Status::Alive)];
        for message in [
            Message::Ping {
                seq: 1,
                updates: updates.clone(),
            },
            Message::Ack {
                seq: 2,
                updates: Vec::new(),
            },
            Message::PingReq {
                seq: 3,
                target: 9,
                updates,
            },
        ] {
            let bytes = encode(&message).unwrap();
            assert_eq!(decode(&bytes).unwrap(), message);
        }
    }

    #[test]
    fn the_worst_case_datagram_fits_the_mtu() {
        let message = Message::PingReq {
            seq: u32::MAX,
            target: u64::MAX,
            updates: worst_updates(MAX_UPDATES),
        };
        let len = encode(&message).unwrap().len();
        assert!(len <= MAX_DATAGRAM, "{len} bytes");
        assert!(
            len <= 300,
            "membership must leave the datagram to demand[]: {len} bytes"
        );
    }

    #[test]
    fn one_update_too_many_is_refused_both_ways() {
        let message = Message::Ping {
            seq: 0,
            updates: worst_updates(MAX_UPDATES + 1),
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
        };
        assert_eq!(encode(&ping).unwrap(), [0, 1, 0]);
    }

    #[test]
    fn garbage_is_rejected_not_guessed() {
        assert_eq!(decode(&[]), Err(Error::Malformed));
        assert_eq!(decode(&[0xde, 0xad, 0xbe, 0xef]), Err(Error::Malformed));
        assert_eq!(decode(&[0, 1, 0, 0xff]), Err(Error::TrailingBytes(1)));
        assert_eq!(
            decode(&vec![0; MAX_DATAGRAM + 1]),
            Err(Error::TooLarge(MAX_DATAGRAM + 1))
        );

        let bytes = encode(&Message::Ack {
            seq: 5,
            updates: vec![update(1, Status::Alive)],
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
            (
                any::<u64>(),
                any::<u32>(),
                prop_oneof![
                    Just(Status::Alive),
                    Just(Status::Suspect),
                    Just(Status::Dead)
                ],
            )
                .prop_map(|(member, incarnation, status)| Update {
                    member,
                    incarnation,
                    status,
                })
        }

        fn message() -> impl Strategy<Value = Message> {
            let updates = || prop::collection::vec(update(), 0..=MAX_UPDATES);
            prop_oneof![
                (any::<u32>(), updates()).prop_map(|(seq, updates)| Message::Ping { seq, updates }),
                (any::<u32>(), updates()).prop_map(|(seq, updates)| Message::Ack { seq, updates }),
                (any::<u32>(), any::<u64>(), updates()).prop_map(|(seq, target, updates)| {
                    Message::PingReq {
                        seq,
                        target,
                        updates,
                    }
                }),
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
