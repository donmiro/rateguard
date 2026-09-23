use rateguard_core::gcra::Nanos;

pub type NodeIndex = usize;

pub trait Link {
    fn deliver_at(
        &mut self,
        from: NodeIndex,
        to: NodeIndex,
        now: Nanos,
        len: usize,
    ) -> Option<Nanos>;
}

pub struct PerfectLink {
    latency: Nanos,
}
impl PerfectLink {
    pub fn new(latency: Nanos) -> Self {
        assert!(
            latency > 0,
            "latency must be > 0: a dataram that arrives in the same nanosecond it was sent is not a network, and it hides every ordering bug"
        );

        Self { latency }
    }
}
impl Link for PerfectLink {
    fn deliver_at(
        &mut self,
        _from: NodeIndex,
        _to: NodeIndex,
        now: Nanos,
        _len: usize,
    ) -> Option<Nanos> {
        Some(now + self.latency)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn an_instant_wire_is_rejected() {
        PerfectLink::new(0);
    }

    #[test]
    fn a_perfect_wire_always_delivers_one_latency_later() {
        let mut link = PerfectLink::new(1_000_000);
        assert_eq!(link.deliver_at(0, 1, 5_000_000, 64), Some(6_000_000));
    }
}
