//! How keys and nodes are named on the wire.

use rateguard_core::boundary::PeerId;
use rateguard_proto::Address;
use xxhash_rust::xxh3::xxh3_64;

/// The 64-bit hash a key travels and is stored under. xxh3: fixed by its
/// specification, the same on every node and every Rust version. The key
/// table reserves 0 (an empty slot) and `u64::MAX` (a slot being reset), so
/// a key hashing to either is moved next door.
pub fn key_hash(key: &str) -> u64 {
    match xxh3_64(key.as_bytes()) {
        0 => 1,
        u64::MAX => u64::MAX - 1,
        hash => hash,
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
