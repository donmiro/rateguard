//! The accuracy report of spec §6.3: every named scenario on the settings
//! the runtime ships with, measured, as a markdown table for the README.
//!
//! Five nodes, R = 1000 a second on one key, the other settings at their
//! defaults: β = 0.05, 64 hot keys, 4096 tracked, a burst of R / 20 on each
//! node. Seeds are fixed, so the table is the same on every run.

use rateguard_core::gcra::{Decision, Nanos};
use rateguard_core::limiter::{Config, Limit};
use rateguard_core::partition::PartitionPolicy;

use crate::link::{Fate, Link, NetConfig, NodeIndex, PerfectLink, SeededLink};
use crate::sim::Sim;

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const NODES: usize = 5;
const KEY: u64 = 42;
const LIMIT: f64 = 1000.0;

/// rateguard's `Guard::builder().limit(1000)`, nothing else set.
pub fn defaults() -> Config {
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

/// One row of the report.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// What happens.
    pub scenario: &'static str,
    /// What the design promises.
    pub expected: &'static str,
    /// Admitted a second over the steady window, as a share of R.
    pub admitted: f64,
    /// The busiest one-second window of the whole run, as a share of R.
    pub peak: f64,
    /// Whole seconds of the run that admitted more than 1.01 R: over R by
    /// more than the bursts' rounding.
    pub seconds_over: usize,
    /// From the scenario's event until every second stays within 5% of the
    /// steady rate; `None` if it never settles before the window ends.
    pub settled_after: Option<Nanos>,
}

struct Measure {
    event: Nanos,
    steady: (Nanos, Nanos),
    nodes: Vec<NodeIndex>,
}

// Admitted on KEY, per whole second of the run, on `nodes`.
fn per_second(s: &Sim<impl Link>, nodes: &[NodeIndex], until: Nanos) -> Vec<f64> {
    let mut seconds = vec![0.0; (until / ONE_SEC) as usize];
    for a in s.admissions() {
        if a.key == KEY
            && a.decision == Decision::Allow
            && nodes.contains(&a.node)
            && let Some(second) = seconds.get_mut((a.at / ONE_SEC) as usize)
        {
            *second += 1.0;
        }
    }
    seconds
}

fn row(scenario: &'static str, expected: &'static str, s: &Sim<impl Link>, m: Measure) -> Row {
    let (from, until) = m.steady;
    let seconds = per_second(s, &m.nodes, until);
    let steady = &seconds[(from / ONE_SEC) as usize..(until / ONE_SEC) as usize];
    let admitted = steady.iter().sum::<f64>() / steady.len() as f64;
    let peak = seconds.iter().copied().fold(0.0, f64::max);
    let seconds_over = seconds.iter().filter(|&&n| n > 1.01 * LIMIT).count();
    let first = (m.event / ONE_SEC) as usize;
    let last = (until / ONE_SEC) as usize;
    let within = |n: f64| (n - admitted).abs() <= 0.05 * admitted.max(1.0);
    let settled_after = (first..last)
        .find(|&k| seconds[k..last].iter().all(|&n| within(n)))
        .map(|k| (k as Nanos * ONE_SEC).saturating_sub(m.event));
    Row {
        scenario,
        expected,
        admitted: admitted / LIMIT,
        peak: peak / LIMIT,
        seconds_over,
        settled_after,
    }
}

const ALL: [NodeIndex; NODES] = [0, 1, 2, 3, 4];

fn whole(event: Nanos, from: Nanos, until: Nanos) -> Measure {
    Measure {
        event,
        steady: (from, until),
        nodes: ALL.to_vec(),
    }
}

fn streams(s: &mut Sim<impl Link>, rates: [u32; NODES], until: Nanos) {
    for (node, rate) in rates.into_iter().enumerate() {
        if rate > 0 {
            s.schedule_request_stream(node, KEY, rate, 0, until);
        }
    }
}

const SKEWED: [u32; NODES] = [1200, 150, 150, 150, 150];

fn steady(scenario: &'static str, rates: [u32; NODES], link: impl Link) -> Row {
    let run = 40 * ONE_SEC;
    let mut s = Sim::new(NODES, defaults(), link);
    s.remove_invariant("admission-window");
    streams(&mut s, rates, run);
    s.run_until(run);
    row(scenario, "R", &s, whole(0, 20 * ONE_SEC, run))
}

fn floors_unused() -> Row {
    let run = 40 * ONE_SEC;
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    s.remove_invariant("admission-window");
    streams(&mut s, [1500, 0, 0, 0, 0], run);
    s.run_until(run);
    row(
        "Demand on 1 of 5 nodes",
        "≥ R × (1 − 4β/5) = 0.96 R",
        &s,
        whole(0, 20 * ONE_SEC, run),
    )
}

const CUT: Nanos = 10 * ONE_SEC;
const HEAL: Nanos = 50 * ONE_SEC;

struct Split;
impl Link for Split {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if (CUT..HEAL).contains(&now) && (from < 3) != (to < 3) {
            Fate::Lost
        } else {
            Fate::Delivered(now + ONE_MS)
        }
    }
}

fn split(scenario: &'static str, expected: &'static str, policy: PartitionPolicy) -> Row {
    let run = 60 * ONE_SEC;
    let mut s = Sim::new(NODES, defaults(), Split);
    s.remove_invariant("admission-window");
    s.set_partition_policy(policy);
    streams(&mut s, [400; NODES], run);
    s.run_until(run);
    row(scenario, expected, &s, whole(CUT, CUT + 30 * ONE_SEC, HEAL))
}

struct OneWay;
impl Link for OneWay {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        if (from, to) == (0, 1) && now >= CUT {
            Fate::Lost
        } else {
            Fate::Delivered(now + ONE_MS)
        }
    }
}

fn one_way() -> Row {
    let run = 50 * ONE_SEC;
    let mut s = Sim::new(NODES, defaults(), OneWay);
    s.remove_invariant("admission-window");
    streams(&mut s, SKEWED, run);
    s.run_until(run);
    row(
        "One-way link, 0 → 1 lost",
        "R",
        &s,
        whole(CUT, 30 * ONE_SEC, run),
    )
}

// The four nodes that keep running; node 4 pauses.
fn gc_pause() -> Row {
    let run = 50 * ONE_SEC;
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    s.remove_invariant("admission-window");
    streams(&mut s, [400; NODES], run);
    s.pause(4, 25 * ONE_SEC, 30 * ONE_SEC);
    s.run_until(run);
    row(
        "GC pause, 5 s on node 4 (the other four)",
        "their shares unchanged",
        &s,
        Measure {
            event: 25 * ONE_SEC,
            steady: (40 * ONE_SEC, run),
            nodes: vec![0, 1, 2, 3],
        },
    )
}

// Settling counts from the last restart, the steady window after it.
fn rolling_restart() -> Row {
    let run = 80 * ONE_SEC;
    let mut s = Sim::with_seeds(NODES, defaults(), PerfectLink::new(ONE_MS), &[0, 1]);
    s.remove_invariant("admission-window");
    streams(&mut s, [400; NODES], run);
    for node in 0..NODES {
        let down = (15 + 8 * node as u64) * ONE_SEC;
        s.schedule_restart(node, down, down + ONE_SEC);
    }
    s.run_until(run);
    row(
        "Rolling restart, one node every 8 s",
        "≤ R, no second without service",
        &s,
        whole(48 * ONE_SEC, 60 * ONE_SEC, run),
    )
}

// Node 4 stops for good at 20 s; the other four, 1600 a second between
// them, take up its share. A crash costs them the suspicion timeout and the
// hold first, a leave nothing.
fn stop(scenario: &'static str, expected: &'static str, leave: bool) -> Row {
    let stop = 20 * ONE_SEC;
    let run = 60 * ONE_SEC;
    let mut s = Sim::with_seeds(NODES, defaults(), PerfectLink::new(ONE_MS), &[0, 1]);
    s.remove_invariant("admission-window");
    streams(&mut s, [400; NODES], run);
    if leave {
        s.schedule_leave(4, stop);
    } else {
        s.schedule_restart(4, stop, Nanos::MAX / 2);
    }
    s.run_until(run);
    row(
        scenario,
        expected,
        &s,
        Measure {
            event: stop,
            steady: (45 * ONE_SEC, run),
            nodes: vec![0, 1, 2, 3],
        },
    )
}

fn jump() -> Row {
    let jump = 20 * ONE_SEC;
    let run = 50 * ONE_SEC;
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    s.remove_invariant("admission-window");
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 10, 0, jump);
        s.schedule_request_stream(node, KEY, 1000, jump, run);
    }
    s.run_until(run);
    row(
        "Traffic jumps 100×, cold to hot",
        "R",
        &s,
        whole(jump, jump + 15 * ONE_SEC, run),
    )
}

fn late_joiner() -> Row {
    struct Late;
    impl Link for Late {
        fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, _len: usize) -> Fate {
            if now < 10 * ONE_SEC && (from == 4 || to == 4) {
                Fate::Lost
            } else {
                Fate::Delivered(now + ONE_MS)
            }
        }
    }
    let run = 40 * ONE_SEC;
    let mut s = Sim::with_seeds(NODES, defaults(), Late, &[0, 1, 2, 3]);
    s.remove_invariant("admission-window");
    streams(&mut s, [400; NODES], run);
    s.run_until(run);
    row(
        "A node joins 10 s late",
        "R",
        &s,
        whole(10 * ONE_SEC, 25 * ONE_SEC, run),
    )
}

// The key's limit changes from `from` to `to` while the cluster runs: the
// news reaches the nodes one a second, as a rolling config change would.
fn limit_change(scenario: &'static str, expected: &'static str, from: u32, to: u32) -> Row {
    let change = 20 * ONE_SEC;
    let run = 50 * ONE_SEC;
    let limit = |per_second: u32| Limit {
        rate: per_second as f64,
        burst: per_second / 20,
    };
    let mut s = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    s.remove_invariant("admission-window");
    s.set_key_limit(KEY, limit(from));
    for node in 0..NODES {
        s.schedule_key_limit(change + node as Nanos * ONE_SEC, node, KEY, limit(to));
    }
    streams(&mut s, SKEWED.map(|rate| rate * 2), run);
    s.run_until(run);
    row(
        scenario,
        expected,
        &s,
        whole(change, change + 15 * ONE_SEC, run),
    )
}

/// The shadow line of the report: a shadow at R/5 next to a limit of R,
/// against the same run with R/5 enforced.
#[derive(Debug, Clone, PartialEq)]
pub struct ShadowLine {
    /// Requests the shadow would have denied, a second over the steady
    /// window.
    pub shadow_denied: f64,
    /// Requests denied with the shadow's limit enforced, a second over the
    /// same window.
    pub denied: f64,
}

/// Skewed demand on R = 1000 with a shadow at R/5, and the same with R/5
/// enforced: the key is hot under either, so the shadow's share is the
/// enforced one, scaled.
pub fn shadow() -> ShadowLine {
    let run = 40 * ONE_SEC;
    let (from, until) = (20 * ONE_SEC, run);
    let fifth = Limit {
        rate: LIMIT / 5.0,
        burst: (LIMIT / 5.0 / 20.0) as u32,
    };
    let in_window = |a: &&crate::sim::Admission| a.key == KEY && (from..until).contains(&a.at);
    let secs = ((until - from) / ONE_SEC) as f64;

    let mut shadowed = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    shadowed.remove_invariant("admission-window");
    shadowed.set_shadow(KEY, Some(fifth));
    streams(&mut shadowed, SKEWED, run);
    shadowed.run_until(run);
    let shadow_denied = shadowed.shadow_denials().iter().filter(in_window).count();

    let mut enforced = Sim::new(NODES, defaults(), PerfectLink::new(ONE_MS));
    enforced.remove_invariant("admission-window");
    enforced.set_key_limit(KEY, fifth);
    streams(&mut enforced, SKEWED, run);
    enforced.run_until(run);
    let denied = enforced
        .admissions()
        .iter()
        .filter(in_window)
        .filter(|a| a.decision != Decision::Allow)
        .count();

    ShadowLine {
        shadow_denied: shadow_denied as f64 / secs,
        denied: denied as f64 / secs,
    }
}

/// Every scenario, measured.
pub fn rows() -> Vec<Row> {
    let lossy = NetConfig {
        loss: 0.3,
        ..NetConfig::perfect(ONE_MS)
    };
    let messy = NetConfig {
        duplicate: 0.2,
        reorder: 0.5,
        reorder_window: 6 * 200 * ONE_MS,
        ..NetConfig::perfect(ONE_MS)
    };
    vec![
        steady(
            "Even demand, 5 × 400",
            [400; NODES],
            PerfectLink::new(ONE_MS),
        ),
        steady(
            "Skewed demand, 1200 + 4 × 150",
            SKEWED,
            PerfectLink::new(ONE_MS),
        ),
        floors_unused(),
        steady(
            "Skewed, 30% packet loss",
            SKEWED,
            SeededLink::new(8371, lossy),
        ),
        steady(
            "Skewed, 20% duplicated, half delayed up to 6 periods",
            SKEWED,
            SeededLink::new(8371, messy),
        ),
        split(
            "3/2 split, Optimistic",
            "up to k × R = 2 R",
            PartitionPolicy::Optimistic,
        ),
        split(
            "3/2 split, HoldDown(10 s)",
            "≈ R for 10 s, then up to 2 R",
            PartitionPolicy::HoldDown(10 * ONE_SEC),
        ),
        split(
            "3/2 split, HoldDown(60 s), longer than the split",
            "≈ R",
            PartitionPolicy::HoldDown(60 * ONE_SEC),
        ),
        split(
            "3/2 split, Quorum",
            "R + the minority's floors = 1.02 R",
            PartitionPolicy::Quorum,
        ),
        one_way(),
        gc_pause(),
        rolling_restart(),
        stop(
            "Node 4 crashes for good (the other four)",
            "R once the suspicion and the hold are over",
            false,
        ),
        stop("Node 4 leaves for good (the other four)", "R at once", true),
        jump(),
        late_joiner(),
        limit_change(
            "Limit lowered to R/2, one node a second",
            "≤ R in passing, R/2 after",
            1000,
            500,
        ),
        limit_change(
            "Limit raised from R/2 to R, one node a second",
            "≤ R, then R",
            500,
            1000,
        ),
    ]
}

/// The whole report as the README holds it: the table of every scenario,
/// then the shadow line.
pub fn report() -> String {
    format!("{}\n{}", markdown(&rows()), shadow_markdown(&shadow()))
}

/// The shadow line as the README's.
pub fn shadow_markdown(line: &ShadowLine) -> String {
    // Rounded to a tenth of a percent first, and `+ 0.0` turns a -0.0 into
    // 0.0.
    let off = ((line.shadow_denied / line.denied - 1.0) * 1000.0).round() / 10.0 + 0.0;
    format!(
        "Shadow at R/5, skewed demand on R: shadow denials vs. enforced, \
         {:.1} vs. {:.1} a second ({off:+.1}%).\n",
        line.shadow_denied, line.denied
    )
}

/// The rows as the README's table.
pub fn markdown(rows: &[Row]) -> String {
    let mut out = String::from(
        "| Scenario | Design bound | Admitted, steady | Peak second | Seconds over 1.01 R | Settles in |\n\
         |---|---|---|---|---|---|\n",
    );
    for r in rows {
        let settled = match r.settled_after {
            Some(t) => format!("{} s", t / ONE_SEC),
            None => "—".to_string(),
        };
        out.push_str(&format!(
            "| {} | {} | {:.2} R | {:.2} R | {} | {} |\n",
            r.scenario, r.expected, r.admitted, r.peak, r.seconds_over, settled
        ));
    }
    out
}
