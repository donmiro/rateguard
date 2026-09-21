use crate::{
    boundary::{Action, Event},
    gcra::{Decision, Nanos},
    limiter::{Config, Limiter},
};

pub struct Node {
    limiter: Limiter,
    cluster_size: usize,
    last_now: Nanos,
    actions: Vec<Action>,
}
impl Node {
    pub fn new(config: Config) -> Self {
        Self {
            limiter: Limiter::new(config),
            cluster_size: 1,
            last_now: 0,
            actions: Vec::new(),
        }
    }

    pub fn set_cluster_size(&mut self, n: usize) {
        assert!(n > 0, "cluster_size must be > 0");
        self.cluster_size = n;
    }

    pub fn cluster_size(&self) -> usize {
        self.cluster_size
    }

    pub fn check(&mut self, key: u64, now: Nanos) -> Decision {
        self.advance(now);
        self.limiter.check(key, now, self.cluster_size)
    }

    pub fn handle(&mut self, event: Event<'_>, now: Nanos) -> &[Action] {
        self.advance(now);
        self.actions.clear();

        match event {
            Event::Tick => self.limiter.tick(now, self.cluster_size),
            Event::MessageReceived { .. } => {}
        }

        &self.actions
    }

    fn advance(&mut self, now: Nanos) {
        assert!(
            now >= self.last_now,
            "time must not run backwards: {now} < {}",
            self.last_now
        );
        self.last_now = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::PeerId;

    const ONE_SEC: Nanos = 1_000_000_000;
    const KEY: u64 = 42;

    fn config() -> Config {
        Config {
            limit_per_sec: 1000,
            burst: 10,
            alpha: 0.5,
            cooldown: 5 * ONE_SEC,
            hot_set_size: 4,
            max_tracked_keys: 8,
            demand_time_constant: ONE_SEC,
        }
    }

    fn node() -> Node {
        Node::new(config())
    }

    fn measured_interval(node: &mut Node, now: Nanos) -> Nanos {
        for _ in 0..config().burst {
            assert_eq!(node.check(KEY, now), Decision::Allow);
        }
        let Decision::Deny { retry_at } = node.check(KEY, now) else {
            panic!("burst must be exhausted");
        };
        retry_at - now
    }

    #[test]
    fn a_freash_node_is_a_cluster_of_one() {
        assert_eq!(node().cluster_size(), 1);
    }

    #[test]
    fn check_delegates_to_the_limiter_at_the_cold_share() {
        assert_eq!(measured_interval(&mut node(), 0), 2_000_000);
    }

    #[test]
    fn the_cluster_size_narrows_the_cold_share() {
        let mut n = node();
        n.set_cluster_size(2);
        assert_eq!(measured_interval(&mut n, 0), 4_000_000);
    }

    #[test]
    #[should_panic]
    fn an_empty_cluster_is_rejected() {
        node().set_cluster_size(0);
    }

    #[test]
    fn a_tick_drives_promotion() {
        let mut n = node();
        for _ in 0..5000 {
            n.check(KEY, 0);
        }

        assert_eq!(
            measured_interval(&mut n, 100_000_000),
            2_000_000,
            "promotion belongs to the tick, not the check()"
        );

        n.handle(Event::Tick, ONE_SEC);
        assert_eq!(
            measured_interval(&mut n, 2 * ONE_SEC),
            1_000_000,
            "a hot key gets the full share, not the alphs one"
        );
    }

    #[test]
    fn a_node_has_nothing_to_send_yet() {
        let mut n = node();
        assert!(n.handle(Event::Tick, 0).is_empty());
    }

    #[test]
    fn a_diagram_is_dropped_without_a_wire_format() {
        let mut n = node();
        let from = PeerId::new(7);
        let bytes = [0xde, 0xad, 0xbe, 0xef];

        assert!(
            n.handle(
                Event::MessageReceived {
                    from,
                    bytes: &bytes
                },
                0
            )
            .is_empty()
        );
    }

    #[test]
    #[should_panic(expected = "time must not run backwards")]
    fn an_event_may_not_arrive_in_the_past() {
        let mut n = node();
        n.handle(Event::Tick, ONE_SEC);
        n.handle(Event::Tick, 0);
    }

    #[test]
    #[should_panic(expected = "time must not run backwards")]
    fn a_request_may_not_arrive_in_the_past() {
        let mut n = node();
        n.handle(Event::Tick, ONE_SEC);
        n.check(KEY, 0);
    }
}
