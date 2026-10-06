use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::Config;
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
}

pub struct View<'a> {
    pub event: u64,
    pub now: Nanos,
    pub target: NodeIndex,
    pub happened: &'a Happened,
    pub nodes: &'a [Node],
    pub config: &'a Config,
}

pub trait Invariant {
    fn name(&self) -> &'static str;
    fn check(&mut self, view: &View<'_>) -> Result<(), String>;
}

pub fn standard() -> Vec<Box<dyn Invariant>> {
    vec![
        Box::new(HotSetBounded),
        Box::new(TrackedKeysBounded),
        Box::new(AdmissionWindow::new(ONE_SEC)),
    ]
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
        if *view.happened != Happened::Tick {
            return Ok(());
        }

        let tracked = view.nodes[view.target].limiter().tracked_keys();
        let max = view.config.max_tracked_keys;
        if tracked > max {
            return Err(format!(
                "{tracked} tracked keys after a tick, max_tracked_keys is {max}"
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
        let mut node = Node::new(config(), PeerId::new(0), 0);
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
    fn tracked_keys_are_bounded_only_after_a_tick() {
        let mut node = Node::new(config(), PeerId::new(0), 0);
        for key in 0..3 {
            node.check(key, 0);
        }
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
            "between ticks the map may grow, the cap is enforced by tick()"
        );
    }

    #[test]
    fn the_window_admits_the_rate_plus_one_burst_per_node() {
        let nodes = [Node::new(config(), PeerId::new(0), 0)];
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
        let nodes = [Node::new(config(), PeerId::new(0), 0)];
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
        let nodes = [Node::new(config(), PeerId::new(0), 0)];
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
}
