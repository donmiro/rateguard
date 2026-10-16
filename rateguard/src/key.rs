//! How keys and nodes are named on the wire.

use rateguard_core::boundary::PeerId;
use rateguard_proto::Address;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

/// The 64-bit hash a key travels and is stored under. xxh3: fixed by its
/// specification, the same on every node and every Rust version. The key
/// table reserves 0 (an empty slot) and `u64::MAX` (a slot being reset), so
/// a key hashing to either is moved next door.
pub fn key_hash(key: &str) -> u64 {
    key_hash_in("", key)
}

/// The hash of `key` in the class `class`: xxh3 with the class's hash as
/// its seed, so that one key in two classes is two keys. The default class,
/// `""`, has seed 0, and its hashes are [`key_hash`]'s. Public so that the
/// hashes of [`Stats::most_denied`](crate::Stats::most_denied) can be
/// matched to keys.
pub fn key_hash_in(class: &str, key: &str) -> u64 {
    hash_in(class_hash(class), key)
}

/// [`key_hash_in`] for a class already hashed.
pub(crate) fn hash_in(class: u64, key: &str) -> u64 {
    match xxh3_64_with_seed(key.as_bytes(), class) {
        0 => 1,
        u64::MAX => u64::MAX - 1,
        hash => hash,
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
}
