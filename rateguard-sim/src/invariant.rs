//! Properties checked after every event of a simulation.

use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::Config;
use rateguard_core::membership;
use rateguard_core::node::Node;

use std::collections::{HashMap, VecDeque};

use crate::link::NodeIndex;
use crate::sim::Admission;

const ONE_SEC: Nanos = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Happened {
    Tick,
    Delivery,
    Admission(Admission),
    Restart,
}

pub struct View<'a> {
    pub event: u64,
    pub now: Nanos,
    pub target: NodeIndex,
    pub happened: &'a Happened,
    pub nodes: &'a [Node],
    pub config: &'a Config,
}

/// A property checked after every event of a run. A violation stops the run
/// at the event that broke it.
pub trait Invariant {
    fn name(&self) -> &'static str;
    fn check(&mut self, view: &View<'_>) -> Result<(), String>;
}

/// The invariants every simulation checks.
pub fn standard() -> Vec<Box<dyn Invariant>> {
    vec![
        Box::new(HotSetBounded),
        Box::new(TrackedKeysBounded),
        Box::new(AdmissionWindow::new(ONE_SEC)),
        Box::new(MembershipContract),
        Box::new(IncarnationNeverDrops::default()),
    ]
}

pub struct MembershipContract;
impl Invariant for MembershipContract {
    fn name(&self) -> &'static str {
        "membership-contract"
    }

    fn check(&mut self, view: &View<'_>) -> Result<(), String> {
        membership::check_contract(view.nodes[view.target].members())
    }
}

/// Only the node itself raises its incarnation, and only upward: a
/// refutation that went back in time would lose to the very suspicion it
/// answers. A restart is the one legitimate reset.
#[derive(Default)]
pub struct IncarnationNeverDrops {
    seen: HashMap<NodeIndex, u32>,
}
impl Invariant for IncarnationNeverDrops {
    fn name(&self) -> &'static str {
        "incarnation-never-drops"
    }

    fn check(&mut self, view: &View<'_>) -> Result<(), String> {
        let now = view.nodes[view.target].members().incarnation();
        if *view.happened == Happened::Restart {
            self.seen.insert(view.target, now);
            return Ok(());
        }
        let before = self.seen.insert(view.target, now).unwrap_or(0);
        if now < before {
            return Err(format!("incarnation went from {before} down to {now}"));
        }
        Ok(())
    }
}

pub struct HotSetBounded;
impl Invariant for HotSetBounded {
    fn name(&self) -> &'static str {
        "hot-set-bounded"
    }

    fn check(&mut self, view: &View<'_>) -> Result<(), String> {
        let hot = view.nodes[view.target].limiter().hot_keys();
        let max = view.config.hot_set_size;
        if hot > max {
            return Err(format!("{hot} hot keys, hot_set_size is {max}"));
        }

        Ok(())
    }
}

pub struct TrackedKeysBounded;
impl Invariant for TrackedKeysBounded {
    fn name(&self) -> &'static str {
        "tracked-keys-bounded"
    }

    fn check(&mut self, view: &View<'_>) -> Result<(), String> {
        let node = &view.nodes[view.target];
        if *view.happened != Happened::Tick || node.last_round() != Some(view.now) {
            return Ok(());
        }

        let tracked = node.limiter().tracked_keys();
        let max = view.config.max_tracked_keys;
        if tracked > max {
            return Err(format!(
                "{tracked} tracked keys after a round, max_tracked_keys is {max}"
            ));
        }

        Ok(())
    }
}

pub struct AdmissionWindow {
    window: Nanos,
    allowed: HashMap<u64, VecDeque<Nanos>>,
}
impl AdmissionWindow {
    pub fn new(window: Nanos) -> Self {
        assert!(window > 0, "an empty window admits nothing to check");
        Self {
            window,
            allowed: HashMap::new(),
        }
    }
}
impl Invariant for AdmissionWindow {
    fn name(&self) -> &'static str {
        "admission-window"
    }

    fn check(&mut self, view: &View<'_>) -> Result<(), String> {
        let Happened::Admission(admission) = view.happened else {
            return Ok(());
        };
        if admission.decision != Decision::Allow {
            return Ok(());
        }

        let times = self.allowed.entry(admission.key).or_default();
        times.push_back(admission.at);
        while times
            .front()
            .is_some_and(|&t| t + self.window <= admission.at)
        {
            times.pop_front();
        }

        let admitted = times.len() as u128;
        let rate_part = view.config.limit_per_sec as u128 * self.window as u128;
        let burst_part = view.nodes.len() as u128 * view.config.burst as u128 * ONE_SEC as u128;

        if admitted * ONE_SEC as u128 > rate_part + burst_part {
            return Err(format!(
                "key {} admitted {admitted} times in the last {} ns, the bound is {:.1}",
                admission.key,
                self.window,
                (rate_part + burst_part) as f64 / ONE_SEC as f64
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rateguard_core::boundary::{Event, PeerId};
    use rateguard_core::node::SwimConfig;

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

    fn view<'a>(nodes: &'a [Node], config: &'a Config, happened: &'a Happened) -> View<'a> {
        View {
            event: 0,
            now: 0,
            target: 0,
            happened,
            nodes,
            config,
        }
    }

    fn allowed(at: Nanos, key: u64) -> Happened {
        Happened::Admission(Admission {
            at,
            node: 0,
            key,
            decision: Decision::Allow,
        })
    }

    #[test]
    fn a_hot_set_over_its_size_is_caught() {
        let mut node = Node::new(config(), SwimConfig::default(), PeerId::new(0), 0);
        for _ in 0..5000 {
            node.check(KEY, 0);
        }
        node.handle(Event::Tick, ONE_SEC);
        assert_eq!(node.limiter().hot_keys(), 1, "the key must be promoted");

        let nodes = [node];
        let happened = Happened::Tick;
        let fits = config();
        let too_small = Config {
            hot_set_size: 0,
            ..config()
        };

        assert!(HotSetBounded.check(&view(&nodes, &fits, &happened)).is_ok());
        assert!(
            HotSetBounded
                .check(&view(&nodes, &too_small, &happened))
                .is_err()
        );
    }

    #[test]
    fn tracked_keys_are_bounded_only_after_a_round() {
        let mut node = Node::new(config(), SwimConfig::default(), PeerId::new(0), 0);
        for key in 0..3 {
            node.check(key, 0);
        }
        node.handle(Event::Tick, 0);
        let between_rounds = 50_000_000;
        node.handle(Event::Tick, between_rounds);

        let nodes = [node];
        let too_small = Config {
            max_tracked_keys: 2,
            ..config()
        };

        assert!(
            TrackedKeysBounded
                .check(&view(&nodes, &too_small, &Happened::Tick))
                .is_err()
        );
        assert!(
            TrackedKeysBounded
                .check(&view(&nodes, &too_small, &allowed(0, KEY)))
                .is_ok(),
            "between rounds the map may grow, the cap is enforced by the round"
        );
        assert!(
            TrackedKeysBounded
                .check(&View {
                    now: between_rounds,
                    ..view(&nodes, &too_small, &Happened::Tick)
                })
                .is_ok(),
            "a tick between rounds does not run the limiter"
        );
    }

    #[test]
    fn the_window_admits_the_rate_plus_one_burst_per_node() {
        let nodes = [Node::new(
            config(),
            SwimConfig::default(),
            PeerId::new(0),
            0,
        )];
        let config = config();
        let mut window = AdmissionWindow::new(ONE_SEC);

        for _ in 0..1010 {
            assert!(
                window
                    .check(&view(&nodes, &config, &allowed(0, KEY)))
                    .is_ok()
            );
        }
        let err = window
            .check(&view(&nodes, &config, &allowed(0, KEY)))
            .unwrap_err();
        assert!(err.contains("admitted 1011 times"), "{err}");
    }

    #[test]
    fn the_window_forgets_what_slid_out_of_it() {
        let nodes = [Node::new(
            config(),
            SwimConfig::default(),
            PeerId::new(0),
            0,
        )];
        let config = config();
        let mut window = AdmissionWindow::new(ONE_SEC);

        for _ in 0..1010 {
            window
                .check(&view(&nodes, &config, &allowed(0, KEY)))
                .unwrap();
        }
        assert!(
            window
                .check(&view(&nodes, &config, &allowed(ONE_SEC, KEY)))
                .is_ok()
        );
    }

    #[test]
    fn the_window_counts_keys_apart_and_ignores_denials() {
        let nodes = [Node::new(
            config(),
            SwimConfig::default(),
            PeerId::new(0),
            0,
        )];
        let config = config();
        let mut window = AdmissionWindow::new(ONE_SEC);

        for _ in 0..1010 {
            window
                .check(&view(&nodes, &config, &allowed(0, KEY)))
                .unwrap();
        }
        let denied = Happened::Admission(Admission {
            at: 0,
            node: 0,
            key: KEY,
            decision: Decision::Deny { retry_at: ONE_SEC },
        });

        assert!(window.check(&view(&nodes, &config, &denied)).is_ok());
        assert!(
            window
                .check(&view(&nodes, &config, &allowed(0, KEY + 1)))
                .is_ok()
        );
    }

    fn refuted_once() -> Node {
        let mut node = Node::new(config(), SwimConfig::default(), PeerId::new(0), 0);
        let accusation = rateguard_proto::encode(&rateguard_proto::Message::Ping {
            seq: 1,
            updates: vec![rateguard_proto::Update {
                member: 0,
                incarnation: 0,
                status: rateguard_proto::Status::Suspect,
            }],
        })
        .unwrap();
        node.handle(
            Event::MessageReceived {
                from: PeerId::new(1),
                bytes: &accusation,
            },
            0,
        );
        assert_eq!(node.members().incarnation(), 1);
        node
    }

    #[test]
    fn a_dropping_incarnation_is_caught_unless_the_node_restarted() {
        let config = config();
        let refuted = [refuted_once()];
        let fresh = [Node::new(config, SwimConfig::default(), PeerId::new(0), 0)];

        let mut invariant = IncarnationNeverDrops::default();
        invariant
            .check(&view(&refuted, &config, &Happened::Tick))
            .unwrap();
        let err = invariant
            .check(&view(&fresh, &config, &Happened::Tick))
            .unwrap_err();
        assert!(err.contains("from 1 down to 0"), "{err}");

        let mut invariant = IncarnationNeverDrops::default();
        invariant
            .check(&view(&refuted, &config, &Happened::Tick))
            .unwrap();
        assert!(
            invariant
                .check(&view(&fresh, &config, &Happened::Restart))
                .is_ok()
        );
        assert!(
            invariant
                .check(&view(&fresh, &config, &Happened::Tick))
                .is_ok(),
            "the count restarts with the node"
        );
    }

    #[test]
    fn a_healthy_node_keeps_the_membership_contract() {
        let config = config();
        let mut node = Node::new(config, SwimConfig::default(), PeerId::new(0), 0);
        node.introduce(PeerId::new(2));
        node.introduce(PeerId::new(1));
        let nodes = [node];
        assert!(
            MembershipContract
                .check(&view(&nodes, &config, &Happened::Tick))
                .is_ok()
        );
    }
}
