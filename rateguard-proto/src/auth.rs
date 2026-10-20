//! Signed frames for gossip datagrams.
//!
//! A signed datagram is a frame around the bytes of one encoded message:
//!
//! ```text
//! [0xA6] [version = 1] [the message, as encode makes it] [tag: 16 bytes]
//! ```
//!
//! The tag is HMAC-SHA256 of everything before it, truncated to 16 bytes.
//! An unsigned message starts with its variant index, a small varint, so the
//! marker tells the two apart. A node that predates frames refuses one as
//! malformed: signing starts only once no such node is left.
//!
//! [`Auth`] seals what a node sends and opens what it receives, according to
//! its [`GossipAuth`] mode; decoding the message stays with
//! [`decode`](crate::decode).
//!
//! The module needs the `auth` feature of this crate.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;

/// The first byte of a signed frame. No message starts with it.
const MARKER: u8 = 0xA6;
/// The frame version this crate writes and reads.
const VERSION: u8 = 1;
/// The bytes of the truncated tag. 128 bits are enough against forgery
/// (RFC 2104 §5 allows truncation to half the output).
const TAG_LEN: usize = 16;
/// The shortest secret accepted, in bytes: the output size of SHA-256.
const MIN_SECRET: usize = 32;
/// The most secrets one node holds: the one it signs with and, during a
/// rotation, one more it still accepts.
const MAX_SECRETS: usize = 2;

/// The bytes a frame adds to a message: marker, version and tag.
pub const FRAME_OVERHEAD: usize = 2 + TAG_LEN;

/// How a node signs what it sends and checks what it receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum GossipAuth {
    /// Sends unsigned, byte for byte as 0.5. Receives unsigned datagrams,
    /// and signed ones with the frame stripped unchecked: a node not yet
    /// signing must keep hearing the nodes that already do, or the cluster
    /// splits for the length of a rollout. There is no security in this
    /// mode to lose.
    #[default]
    Off,
    /// Sends signed. Receives signed datagrams with a valid tag and unsigned
    /// ones: for the minutes of a rollout, not for good, since a forgery
    /// passes simply by going unsigned.
    Transitional,
    /// Sends signed. Receives signed datagrams with a valid tag only.
    Required,
}

/// A secret shared by every node of a cluster, at least 32 bytes. Never
/// shown: its `Debug` prints `Secret(..)`.
#[derive(Clone)]
pub struct Secret(Vec<u8>);
impl Secret {
    /// Takes the bytes as they are, any trailing newline included: whoever
    /// reads a secret from a file decides what belongs to it.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Secret, AuthError> {
        let bytes = bytes.into();
        if bytes.len() < MIN_SECRET {
            return Err(AuthError::ShortSecret(bytes.len()));
        }
        Ok(Secret(bytes))
    }
}
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(..)")
    }
}

/// Why a datagram was refused before decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Rejected {
    /// Not signed, and the mode requires a signature.
    Unsigned,
    /// Signed, but by no secret this node holds, or altered on the way.
    BadTag,
    /// The marker of a frame, but no frame: too short, or of a version this
    /// crate does not know.
    Malformed,
}

/// Why secrets or a mode were refused. Never carries secret bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthError {
    /// A secret of this many bytes, fewer than 32.
    ShortSecret(usize),
    /// This many secrets, more than 2.
    TooManySecrets(usize),
    /// A mode that signs, with no secret to sign with.
    NoSecrets,
}
impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::ShortSecret(n) => {
                write!(f, "a secret of {n} bytes, at least {MIN_SECRET} are needed")
            }
            AuthError::TooManySecrets(n) => {
                write!(f, "{n} secrets, at most {MAX_SECRETS} are held")
            }
            AuthError::NoSecrets => write!(f, "gossip signing is on, but no secret is given"),
        }
    }
}
impl std::error::Error for AuthError {}

/// Seals outgoing datagrams and opens incoming ones, for one mode and an
/// ordered list of secrets: the first signs, any verifies.
#[derive(Clone)]
pub struct Auth {
    mode: GossipAuth,
    // One keyed HMAC per secret, in list order, cloned for each datagram so
    // the key schedule is not redone every time.
    macs: Vec<Hmac<Sha256>>,
}
impl Auth {
    /// An `Auth` for `mode` with `secrets`, at most two. Every mode but
    /// `Off` needs at least one; `Off` keeps none it is given.
    pub fn new(mode: GossipAuth, secrets: Vec<Secret>) -> Result<Auth, AuthError> {
        if secrets.len() > MAX_SECRETS {
            return Err(AuthError::TooManySecrets(secrets.len()));
        }
        if mode == GossipAuth::Off {
            return Ok(Auth::off());
        }
        if secrets.is_empty() {
            return Err(AuthError::NoSecrets);
        }
        let macs = secrets
            .iter()
            .map(|s| Hmac::new_from_slice(&s.0).expect("HMAC takes a key of any length"))
            .collect();
        Ok(Auth { mode, macs })
    }

    /// No signing and no checking: the wire of 0.5.
    pub fn off() -> Auth {
        Auth {
            mode: GossipAuth::Off,
            macs: Vec::new(),
        }
    }

    /// The mode.
    pub fn mode(&self) -> GossipAuth {
        self.mode
    }

    /// The datagram to send for the encoded `message`: a frame signed with
    /// the first secret, or in `Off` the message itself.
    pub fn seal(&self, message: &[u8]) -> Vec<u8> {
        let Some(mac) = self.macs.first() else {
            return message.to_vec();
        };
        let mut frame = Vec::with_capacity(message.len() + FRAME_OVERHEAD);
        frame.extend_from_slice(&[MARKER, VERSION]);
        frame.extend_from_slice(message);
        let mut mac = mac.clone();
        mac.update(&frame);
        frame.extend_from_slice(&mac.finalize().into_bytes()[..TAG_LEN]);
        frame
    }

    /// The encoded message inside a received datagram, for
    /// [`decode`](crate::decode), or why the datagram is refused. The tag is
    /// compared in constant time, against each secret in list order.
    pub fn open<'a>(&self, datagram: &'a [u8]) -> Result<&'a [u8], Rejected> {
        if datagram.first() != Some(&MARKER) {
            return match self.mode {
                GossipAuth::Required => Err(Rejected::Unsigned),
                GossipAuth::Off | GossipAuth::Transitional => Ok(datagram),
            };
        }
        if datagram.len() < FRAME_OVERHEAD || datagram[1] != VERSION {
            return Err(Rejected::Malformed);
        }
        let (signed, tag) = datagram.split_at(datagram.len() - TAG_LEN);
        let message = &signed[2..];
        if self.mode == GossipAuth::Off {
            return Ok(message);
        }
        let valid = self.macs.iter().any(|mac| {
            let mut mac = mac.clone();
            mac.update(signed);
            mac.verify_truncated_left(tag).is_ok()
        });
        if valid {
            Ok(message)
        } else {
            Err(Rejected::BadTag)
        }
    }
}
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth")
            .field("mode", &self.mode)
            .field("secrets", &self.macs.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Address, DemandReport, KeyDemand, MAX_DATAGRAM, MAX_DEMAND_KEYS, MAX_UPDATES, Message,
        Status, Update, encode,
    };

    const A: [u8; 32] = [0xA1; 32];
    const B: [u8; 32] = [0xB2; 32];

    fn auth(mode: GossipAuth, secrets: &[&[u8]]) -> Auth {
        let secrets = secrets
            .iter()
            .map(|s| Secret::new(s.to_vec()).unwrap())
            .collect();
        Auth::new(mode, secrets).unwrap()
    }

    fn message() -> Vec<u8> {
        encode(&Message::Ping {
            seq: 7,
            updates: vec![Update {
                member: 3,
                addr: Address::V4([10, 0, 0, 3], 7946),
                incarnation: 1,
                status: Status::Alive,
            }],
            demand: Vec::new(),
        })
        .unwrap()
    }

    #[test]
    fn a_sealed_message_opens_to_itself() {
        let m = message();
        for mode in [GossipAuth::Transitional, GossipAuth::Required] {
            let auth = auth(mode, &[&A]);
            let frame = auth.seal(&m);
            assert_eq!(frame.len(), m.len() + 18, "{mode:?}");
            assert_eq!(frame.len(), m.len() + FRAME_OVERHEAD, "{mode:?}");
            assert_eq!(frame[0], 0xA6, "{mode:?}");
            assert_eq!(frame[1], 1, "{mode:?}");
            assert_eq!(auth.open(&frame), Ok(&m[..]), "{mode:?}");
            assert_eq!(auth.mode(), mode);
        }
    }

    #[test]
    fn off_sends_the_bytes_of_0_5() {
        let m = message();
        assert_eq!(Auth::off().seal(&m), m);
        assert_eq!(Auth::off().mode(), GossipAuth::Off);
        assert_eq!(GossipAuth::default(), GossipAuth::Off);
    }

    // During a rollout a node still in Off must keep hearing the nodes
    // already signing, or the cluster splits for the length of the rollout.
    #[test]
    fn off_opens_a_signed_frame_without_checking() {
        let m = message();
        let mut frame = auth(GossipAuth::Required, &[&A]).seal(&m);
        let last = frame.len() - 1;
        frame[last] ^= 0xff;
        assert_eq!(Auth::off().open(&frame), Ok(&m[..]));
        assert_eq!(Auth::off().open(&m), Ok(&m[..]));
    }

    #[test]
    fn transitional_takes_unsigned_and_refuses_a_bad_tag() {
        let m = message();
        let transitional = auth(GossipAuth::Transitional, &[&A]);
        assert_eq!(transitional.open(&m), Ok(&m[..]));

        let foreign = auth(GossipAuth::Transitional, &[&B]).seal(&m);
        assert_eq!(transitional.open(&foreign), Err(Rejected::BadTag));

        let mut flipped = transitional.seal(&m);
        flipped[2] ^= 1;
        assert_eq!(transitional.open(&flipped), Err(Rejected::BadTag));
    }

    #[test]
    fn required_refuses_unsigned() {
        let m = message();
        let required = auth(GossipAuth::Required, &[&A]);
        assert_eq!(required.open(&m), Err(Rejected::Unsigned));
        assert_eq!(required.open(&[]), Err(Rejected::Unsigned));
    }

    #[test]
    fn either_listed_secret_verifies_the_first_signs() {
        let m = message();
        let mode = GossipAuth::Required;
        let frame = auth(mode, &[&A, &B]).seal(&m);
        assert_eq!(auth(mode, &[&A]).open(&frame), Ok(&m[..]));
        assert_eq!(auth(mode, &[&B]).open(&frame), Err(Rejected::BadTag));

        let rotated = auth(mode, &[&B, &A]);
        assert_eq!(rotated.open(&auth(mode, &[&A]).seal(&m)), Ok(&m[..]));
        assert_eq!(rotated.open(&auth(mode, &[&B]).seal(&m)), Ok(&m[..]));
    }

    #[test]
    fn a_truncated_frame_is_malformed() {
        let mut seventeen = vec![0xA6, 1];
        seventeen.extend([0; 15]);
        let mut unknown_version = auth(GossipAuth::Required, &[&A]).seal(&message());
        unknown_version[1] = 2;
        let inputs: [&[u8]; 4] = [&[0xA6], &[0xA6, 1], &seventeen, &unknown_version];
        for auth in [
            Auth::off(),
            auth(GossipAuth::Transitional, &[&A]),
            auth(GossipAuth::Required, &[&A]),
        ] {
            for input in inputs {
                assert_eq!(
                    auth.open(input),
                    Err(Rejected::Malformed),
                    "{:?} {input:?}",
                    auth.mode()
                );
            }
        }
    }

    #[test]
    fn secrets_are_checked() {
        assert_eq!(
            Secret::new(vec![1; 31]).unwrap_err(),
            AuthError::ShortSecret(31)
        );
        let three = vec![
            Secret::new(A).unwrap(),
            Secret::new(B).unwrap(),
            Secret::new(A).unwrap(),
        ];
        assert_eq!(
            Auth::new(GossipAuth::Required, three).unwrap_err(),
            AuthError::TooManySecrets(3)
        );
        assert_eq!(
            Auth::new(GossipAuth::Transitional, Vec::new()).unwrap_err(),
            AuthError::NoSecrets
        );
        assert_eq!(
            Auth::new(GossipAuth::Required, Vec::new()).unwrap_err(),
            AuthError::NoSecrets
        );
        assert!(Auth::new(GossipAuth::Off, Vec::new()).is_ok());

        // Bytes that read as text, so that a leak shows in every form.
        let bytes = b"hunter2-hunter2-hunter2-hunter2-!".to_vec();
        let secret = Secret::new(bytes.clone()).unwrap();
        let debug = format!("{secret:?} {:?}", auth(GossipAuth::Required, &[&bytes]));
        assert!(debug.starts_with("Secret(..)"), "{debug}");
        assert!(!debug.contains("hunter"), "{debug}");
        assert!(!debug.contains("68756e746572"), "{debug}");
        assert!(!debug.contains("104, 117"), "{debug}");
        for error in [
            AuthError::ShortSecret(31),
            AuthError::TooManySecrets(3),
            AuthError::NoSecrets,
        ] {
            assert!(!error.to_string().is_empty());
        }
    }

    // The worst case of spec §1, measured on 2026-10-19: every field at its
    // widest, twelve IPv6 records and a full report of demand.
    #[test]
    fn the_largest_message_fits_framed() {
        let message = Message::PingReq {
            seq: u32::MAX,
            target: u64::MAX,
            updates: vec![
                Update {
                    member: u64::MAX,
                    addr: Address::V6([0xff; 16], u16::MAX),
                    incarnation: u32::MAX,
                    status: Status::Dead,
                };
                MAX_UPDATES
            ],
            demand: vec![DemandReport {
                origin: u64::MAX,
                epoch: u32::MAX,
                round: u16::MAX,
                keys: vec![
                    KeyDemand {
                        key_hash: u64::MAX,
                        demand: f32::MAX,
                        primary: true,
                    };
                    MAX_DEMAND_KEYS
                ],
            }],
        };
        let bytes = encode(&message).unwrap();
        assert_eq!(bytes.len(), 1301);
        assert!(bytes.len() + FRAME_OVERHEAD <= MAX_DATAGRAM);
        let auth = auth(GossipAuth::Required, &[&A]);
        let frame = auth.seal(&bytes);
        assert!(frame.len() <= MAX_DATAGRAM, "{} bytes", frame.len());
        assert_eq!(auth.open(&frame), Ok(&bytes[..]));
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn any_bytes_from_the_network_are_opened_or_refused_never_a_panic(
                bytes in prop::collection::vec(any::<u8>(), 0..2 * MAX_DATAGRAM),
                signed in any::<bool>(),
            ) {
                let mut bytes = bytes;
                if signed && !bytes.is_empty() {
                    bytes[0] = 0xA6;
                }
                for auth in [
                    Auth::off(),
                    auth(GossipAuth::Transitional, &[&A, &B]),
                    auth(GossipAuth::Required, &[&A]),
                ] {
                    let _ = auth.open(&bytes);
                }
            }
        }
    }
}
