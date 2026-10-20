//! How keys and nodes are named on the wire.

use std::fmt;
use std::hash::Hasher;

use rateguard_core::boundary::PeerId;
use rateguard_proto::Address;
use sha2::{Digest, Sha256};
use siphasher::sip::SipHasher13;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

use crate::config::Error;

/// The 64-bit hash a key travels and is stored under. xxh3: fixed by its
/// specification, the same on every node and every Rust version. The key
/// table reserves 0 (an empty slot) and `u64::MAX` (a slot being reset), so
/// a key hashing to either is moved next door.
///
/// A node given a [`Builder::key_salt`](crate::Builder::key_salt) hashes
/// otherwise: its hashes come from
/// [`Guard::key_hash_in`](crate::Guard::key_hash_in).
pub fn key_hash(key: &str) -> u64 {
    key_hash_in("", key)
}

/// The hash of `key` in the class `class`: xxh3 with the class's hash as
/// its seed, so that one key in two classes is two keys. The default class,
/// `""`, has seed 0, and its hashes are [`key_hash`]'s. Public so that the
/// hashes of [`Stats::most_denied`](crate::Stats::most_denied) can be
/// matched to keys. Without a salt only: a node given a
/// [`Builder::key_salt`](crate::Builder::key_salt) hashes as
/// [`Guard::key_hash_in`](crate::Guard::key_hash_in) does.
pub fn key_hash_in(class: &str, key: &str) -> u64 {
    hash_in(class_hash(class), key)
}

/// [`key_hash_in`] for a class already hashed.
pub(crate) fn hash_in(class: u64, key: &str) -> u64 {
    off_reserved(xxh3_64_with_seed(key.as_bytes(), class))
}

/// The hash of `key` in the class with the hash `class`, salted if `salt`:
/// SipHash-1-3 keyed by the salt over the class, then the key. xxh3 is not
/// a PRF, so its seed would not keep a client who picks the keys from
/// finding two that collide; SipHash is made for that. Unsalted, it is
/// [`hash_in`], the hashes of 0.5.
pub(crate) fn salted_hash_in(salt: Option<&KeySalt>, class: u64, key: &str) -> u64 {
    let Some(salt) = salt else {
        return hash_in(class, key);
    };
    let mut hasher = SipHasher13::new_with_key(&salt.0);
    hasher.write(&class.to_le_bytes());
    hasher.write(key.as_bytes());
    off_reserved(hasher.finish())
}

// The two hashes the key table reserves, moved next door.
fn off_reserved(hash: u64) -> u64 {
    match hash {
        0 => 1,
        u64::MAX => u64::MAX - 1,
        hash => hash,
    }
}

/// The 128 bits a salted hash is keyed with. Its `Debug` shows none of
/// them.
#[derive(Clone)]
pub(crate) struct KeySalt([u8; 16]);
impl KeySalt {
    /// The shortest salt taken: 128 bits, SipHash's whole key.
    pub const MIN_LEN: usize = 16;

    /// The salt from `bytes`: 16 of them as they are, more cut down to the
    /// first 16 bytes of their SHA-256, so that no byte of a long salt is
    /// lost.
    pub fn new(bytes: &[u8]) -> Result<KeySalt, Error> {
        let mut salt = [0; 16];
        match bytes.len() {
            n if n < Self::MIN_LEN => return Err(Error::ShortSalt(n)),
            16 => salt.copy_from_slice(bytes),
            _ => salt.copy_from_slice(&Sha256::digest(bytes)[..16]),
        }
        Ok(KeySalt(salt))
    }
}
impl fmt::Debug for KeySalt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeySalt(..)")
    }
}

/// A class's name as the key table stores it: 0 for the default class,
/// never 0 for another.
pub(crate) fn class_hash(class: &str) -> u64 {
    if class.is_empty() {
        0
    } else {
        xxh3_64(class.as_bytes()).max(1)
    }
}

/// A node's ID, from the address it advertises: every node derives the same
/// one, and a restart on the same address keeps it.
pub(crate) fn peer_id(address: Address) -> PeerId {
    let bytes = postcard::to_allocvec(&address).expect("an Address always serializes");
    PeerId::new(xxh3_64(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_hash_is_xxh3_and_never_zero() {
        assert_eq!(key_hash("api:tenant-42"), xxh3_64(b"api:tenant-42"));
        assert_ne!(key_hash(""), 0, "the empty key is an ordinary key");
        assert_ne!(key_hash(""), key_hash("a"));
        let long = "k".repeat(1 << 20);
        assert_ne!(key_hash(&long), key_hash(&long[1..]));
    }

    #[test]
    fn a_peer_id_follows_from_the_address() {
        let a = Address::V4([10, 0, 0, 1], 7946);
        assert_eq!(peer_id(a), peer_id(a));
        assert_ne!(peer_id(a), peer_id(Address::V4([10, 0, 0, 1], 7947)));
    }

    fn salt(bytes: &[u8]) -> KeySalt {
        KeySalt::new(bytes).unwrap()
    }

    #[test]
    fn no_salt_is_xxh3_as_before() {
        for class in [0, class_hash("pro"), class_hash("free")] {
            for key in ["", "k", "api:tenant-42", &"k".repeat(300)] {
                assert_eq!(salted_hash_in(None, class, key), hash_in(class, key));
            }
        }
    }

    #[test]
    fn two_salts_give_two_hashes() {
        let one = salted_hash_in(Some(&salt(&[1; 16])), 0, "api:tenant-42");
        let two = salted_hash_in(Some(&salt(&[2; 16])), 0, "api:tenant-42");
        assert_ne!(one, two);
        assert_ne!(one, hash_in(0, "api:tenant-42"));
        assert_ne!(two, hash_in(0, "api:tenant-42"));
    }

    #[test]
    fn a_salted_hash_keeps_classes_apart() {
        let s = salt(&[1; 16]);
        assert_ne!(
            salted_hash_in(Some(&s), class_hash("pro"), "k"),
            salted_hash_in(Some(&s), 0, "k")
        );
    }

    #[test]
    fn a_salted_hash_is_siphash_1_3_keyed_by_the_salt() {
        use std::hash::Hasher;
        let bytes: Vec<u8> = (0..16).collect();
        let mut hasher =
            siphasher::sip::SipHasher13::new_with_key(&bytes.clone().try_into().unwrap());
        hasher.write(&7u64.to_le_bytes());
        hasher.write(b"k");
        assert_eq!(salted_hash_in(Some(&salt(&bytes)), 7, "k"), hasher.finish());
    }

    #[test]
    fn a_long_salt_goes_through_sha256() {
        use sha2::Digest;
        let long = [0x5A; 40];
        let digest = sha2::Sha256::digest(long);
        assert_eq!(salt(&long).0, salt(&digest[..16]).0);
        assert_eq!(salt(&long).0, digest[..16]);
        assert_eq!(salt(&[3; 16]).0, [3; 16], "16 bytes are taken as they are");
    }

    #[test]
    fn a_short_salt_is_refused() {
        assert!(matches!(KeySalt::new(&[0; 15]), Err(Error::ShortSalt(15))));
        assert!(matches!(KeySalt::new(&[]), Err(Error::ShortSalt(0))));
    }

    #[test]
    fn a_salt_shows_none_of_its_bytes() {
        let debug = format!(
            "{:?} {:#?}",
            salt(b"hunter2-hunter2-"),
            salt(b"hunter2-hunter2-")
        );
        assert!(!debug.contains("hunter"), "{debug}");
        assert!(!debug.contains("104"), "{debug}");
    }
}
