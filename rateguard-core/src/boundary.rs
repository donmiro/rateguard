#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(u64);
impl PeerId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event<'a> {
    Tick,
    MessageReceived { from: PeerId, bytes: &'a [u8] },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    SendTo { peer: PeerId, bytes: Vec<u8> },
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
