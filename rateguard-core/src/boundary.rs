//! The contract between the core and whoever drives it.
//!
//! The driver feeds [`Event`]s into
//! [`Node::handle`](crate::node::Node::handle) and carries out the
//! [`Action`]s it gets back. Requests do not come through here:
//! [`Node::check`](crate::node::Node::check) answers them directly and
//! without allocating, because the network takes no part in the decision.

/// A cluster member's identity. Opaque to the core: the runtime maps it to an
/// address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(u64);
impl PeerId {
    /// Wraps a raw ID.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw ID.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Something that happened outside the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event<'a> {
    /// Time passed. See [`Node::handle`](crate::node::Node::handle) for how
    /// often it should come.
    Tick,
    /// A datagram arrived. The bytes are borrowed: the core copies what it
    /// keeps.
    MessageReceived {
        /// The member the datagram is from, by its first record.
        from: PeerId,
        /// The datagram.
        bytes: &'a [u8],
    },
}

/// Something the core asks the caller to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Send the bytes to `peer` as one datagram. Losing it is fine: the
    /// protocol is built for loss.
    SendTo {
        /// The member to send to; its address is the caller's to look up.
        peer: PeerId,
        /// The datagram.
        bytes: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_action_can_outlive_the_call_that_produced_it() {
        fn send_and_static<T: Send + 'static>() {}
        send_and_static::<Action>();
        send_and_static::<PeerId>();
    }

    #[test]
    fn an_action_is_a_cheap_borrowed_view() {
        fn copy<T: Copy>() {}
        copy::<Event<'_>>();
        assert!(size_of::<Event<'_>>() <= 32, "{}", size_of::<Event<'_>>());
    }

    #[test]
    fn a_peer_id_is_an_opaque_key() {
        use std::collections::HashSet;

        let ids: HashSet<PeerId> = [PeerId::new(7), PeerId::new(7), PeerId::new(9)]
            .into_iter()
            .collect();
        assert_eq!(ids.len(), 2);
        assert_eq!(PeerId::new(7).get(), 7);
    }
}
