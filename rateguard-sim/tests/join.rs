// Two nodes join a running pair through the same seeds, one a little after
// the other, as a deployment scaled from 2 to 4 does. The news of a join is
// sent a few times and dropped: once the news of the first newcomer had run
// out before the second arrived, neither ever heard of the other, each
// counted a cluster of three and took a third of R, and the fleet held
// 1.17 R for good (found by the k3d demo, 2026-10-30). Whoever first hears
// from a newcomer now tells it of every member it knows.

use rateguard_core::gcra::Nanos;
use rateguard_core::limiter::{Config, Limit};
use rateguard_sim::link::{Fate, Link, NodeIndex};
use rateguard_sim::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const KEY: u64 = 42;

fn defaults() -> Config {
    Config {
        limit: Limit {
            rate: 1000.0,
            burst: 50,
        },
        alpha: 0.5,
        floor_factor: 0.05,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 64,
        max_tracked_keys: 4096,
        demand_time_constant: ONE_SEC,
    }
}

// Nodes 2 and 3 are cut off until they join.
struct Joining {
    second: Nanos,
    third: Nanos,
}
impl Link for Joining {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        let up = |node: NodeIndex| match node {
            2 => self.second,
            3 => self.third,
            _ => 0,
        };
        if now < up(from) || now < up(to) {
            Fate::Lost
        } else {
            Fate::Delivered(now + ONE_MS)
        }
    }
}

// 600 a second on every node from the moment it joins; the cluster sizes
// 8 s after the second newcomer, and what the fleet admitted a second over
// the 10 s after that.
fn join(gap: Nanos) -> (Vec<usize>, f64) {
    let (second, third) = (10 * ONE_SEC, 10 * ONE_SEC + gap);
    let settled = third + 8 * ONE_SEC;
    let end = settled + 10 * ONE_SEC;
    let mut s = Sim::with_seeds(4, defaults(), Joining { second, third }, &[0, 1]);
    s.remove_invariant("admission-window");
    for (node, from) in [(0, 0), (1, 0), (2, second), (3, third)] {
        s.schedule_request_stream(node, KEY, 600, from, end);
    }
    s.run_until(settled);
    let sizes = (0..4).map(|node| s.node(node).cluster_size()).collect();
    s.run_until(end);
    let fleet = s.admitted_between(settled, end) as f64 / 10.0;
    (sizes, fleet)
}

#[test]
fn two_newcomers_a_little_apart_see_each_other() {
    // Every 50 ms up to 2 s, and every 10 ms over the gaps that split the
    // cluster before the fix, 1160 to 1350 ms.
    let gaps = (0..2000)
        .step_by(50)
        .chain((1150..1360).step_by(10))
        .map(|ms| ms * ONE_MS);
    for gap in gaps {
        let (sizes, fleet) = join(gap);
        assert_eq!(sizes, [4; 4], "gap {} ms", gap / ONE_MS);
        assert!(
            fleet <= 1010.0,
            "gap {} ms: {fleet:.0} a second over R = 1000",
            gap / ONE_MS
        );
    }
}
