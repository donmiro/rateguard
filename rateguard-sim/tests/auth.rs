// Signed gossip in a running cluster, the steps of spec §3: every pair of
// neighbouring steps hears each other, a skipped step splits the cluster,
// and a forger without the secret changes nothing.
//
// Five nodes serve 400 attempts a second each on one key, 2000 in all
// against a limit of 1000. The standard invariants hold throughout.

use rateguard_core::gcra::{Decision, Nanos, Quota};
use rateguard_core::limiter::Config;
use rateguard_proto::auth::{Auth, GossipAuth, Secret};
use rateguard_proto::{Address, Message, Status, Update, encode};
use rateguard_sim::invariant::AdmissionWindow;
use rateguard_sim::link::{Link, NodeIndex, PerfectLink};
use rateguard_sim::sim::{Admission, Sim, address_of, peer_of};

const ONE_MS: Nanos = 1_000_000;
const ONE_SEC: Nanos = 1_000_000_000;
const NODES: usize = 5;
const KEY: u64 = 42;
const END: Nanos = 45 * ONE_SEC;

const A: [u8; 32] = [0xA1; 32];
const B: [u8; 32] = [0xB2; 32];
const FORGED: [u8; 32] = [0xF0; 32];

fn config() -> Config {
    Config {
        limit_per_sec: 1000,
        burst: 10,
        alpha: 0.5,
        floor_factor: 0.1,
        cooldown: 5 * ONE_SEC,
        hot_set_size: 4,
        max_tracked_keys: 8,
        demand_time_constant: ONE_SEC,
    }
}

fn auth(mode: GossipAuth, secrets: &[[u8; 32]]) -> Auth {
    let secrets = secrets.iter().map(|s| Secret::new(*s).unwrap()).collect();
    Auth::new(mode, secrets).unwrap()
}

fn required(secrets: &[[u8; 32]]) -> Auth {
    auth(GossipAuth::Required, secrets)
}

fn cluster() -> Sim<PerfectLink> {
    let mut s = Sim::new(NODES, config(), PerfectLink::new(ONE_MS));
    for node in 0..NODES {
        s.schedule_request_stream(node, KEY, 400, 0, END);
    }
    s
}

// Each step reaches the nodes one by one, two seconds apart, and the next
// step starts once the last one has reached every node.
fn roll_out(s: &mut Sim<PerfectLink>, steps: &[Auth], from: Nanos) {
    let mut at = from;
    for step in steps {
        for node in 0..NODES {
            s.schedule_gossip_auth(at, node, step.clone());
            at += 2 * ONE_SEC;
        }
    }
    assert!(
        at < END - 5 * ONE_SEC,
        "the rollout must end well before the run"
    );
}

// Runs to the end in steps of 100 ms, the cluster whole at every one.
fn run_whole(s: &mut Sim<PerfectLink>) {
    for step in 1..=END / (100 * ONE_MS) {
        s.run_until(step * 100 * ONE_MS);
        for node in 0..NODES {
            assert_eq!(
                s.node(node).cluster_size(),
                NODES,
                "at {} ms node {node} lost someone",
                step * 100
            );
            assert_eq!(
                s.rejected(node),
                0,
                "at {} ms node {node} refused a datagram",
                step * 100
            );
        }
    }
}

fn rate(s: &Sim<impl Link>, from: Nanos, until: Nanos) -> f64 {
    let admitted = s
        .admissions()
        .iter()
        .filter(|a| a.key == KEY && (from..until).contains(&a.at))
        .filter(|a| a.decision == Decision::Allow)
        .count();
    admitted as f64 / ((until - from) as f64 / ONE_SEC as f64)
}

// R, plus one burst per node over the window measured.
fn assert_near_the_limit(s: &Sim<impl Link>) {
    let (from, until) = (10 * ONE_SEC, END);
    let ceiling = 1000.0 + (NODES * 10) as f64 / ((until - from) / ONE_SEC) as f64;
    let rate = rate(s, from, until);
    assert!(
        (950.0..=ceiling).contains(&rate),
        "{rate:.1} a second, expected 950..={ceiling:.1}"
    );
}

#[test]
fn enabling_signing_by_rolling_update_keeps_the_cluster() {
    let mut s = cluster();
    roll_out(
        &mut s,
        &[auth(GossipAuth::Transitional, &[A]), required(&[A])],
        10 * ONE_SEC,
    );
    run_whole(&mut s);
    assert_near_the_limit(&s);
}

#[test]
fn rotating_the_secret_by_rolling_update_keeps_the_cluster() {
    let mut s = cluster();
    for node in 0..NODES {
        s.set_gossip_auth(node, required(&[A]));
    }
    roll_out(
        &mut s,
        &[required(&[A, B]), required(&[B, A]), required(&[B])],
        5 * ONE_SEC,
    );
    run_whole(&mut s);
    assert_near_the_limit(&s);
}

// The operator mistake of spec §3: nodes at the first step of a rotation
// and at the third at once, or at two unrelated secrets, stop hearing each
// other and the cluster falls apart. The refusals show it, at the nodes
// that lack the secret the other side signs with: one that still accepts
// B hears a probe signed with it, but its answer, signed with A, is
// refused, so the prober suspects it all the same. A split is a
// partition, and until the sides notice it each takes up to R, as in
// tests/partition.rs.
#[test]
fn skipping_a_rotation_step_splits_the_cluster() {
    for (split, left, right, left_refuses) in [
        ("[A] | [B]", vec![A], vec![B], true),
        ("[A, B] | [B]", vec![A, B], vec![B], false),
    ] {
        let mut s = cluster();
        s.remove_invariant("admission-window");
        s.add_invariant(AdmissionWindow::scaled(ONE_SEC, 2));
        for node in 0..NODES {
            let secrets = if node < 3 { &left } else { &right };
            s.set_gossip_auth(node, required(secrets));
        }
        s.run_until(END);

        for node in 0..NODES {
            let expected = if node < 3 { 3 } else { 2 };
            assert_eq!(
                s.node(node).cluster_size(),
                expected,
                "{split}: node {node}"
            );
            let refuses = node >= 3 || left_refuses;
            assert_eq!(
                s.rejected(node) > 0,
                refuses,
                "{split}: node {node} refused {}",
                s.rejected(node)
            );
        }
    }
}

// Twelve updates, the most one message carries: real members declared dead
// at an incarnation they never reached, invented ones declared alive.
fn forgery(seq: u32) -> Vec<u8> {
    let updates = (0..12u64)
        .map(|i| {
            if i < NODES as u64 {
                Update {
                    member: i,
                    addr: address_of(i as NodeIndex),
                    incarnation: 1000,
                    status: Status::Dead,
                }
            } else {
                Update {
                    member: 100 + i,
                    addr: Address::V4([10, 9, 9, i as u8], 7946),
                    incarnation: 0,
                    status: Status::Alive,
                }
            }
        })
        .collect();
    encode(&Message::Ping {
        seq,
        updates,
        demand: Vec::new(),
    })
    .unwrap()
}

// Every node's view of the cluster and its share of the key, in a form two
// runs can be compared by.
type Outcome = (Vec<Vec<Option<Update>>>, Vec<Option<Quota>>, Vec<Admission>);

fn outcome(s: &Sim<impl Link>) -> Outcome {
    let views = (0..NODES)
        .map(|node| {
            (0..NODES as u64 + 120)
                .map(|peer| s.node(node).members().update_about(peer_of(peer as usize)))
                .collect()
        })
        .collect();
    let shares = (0..NODES).map(|node| s.node(node).quota(KEY)).collect();
    (views, shares, s.admissions().to_vec())
}

#[test]
fn a_forger_without_the_secret_changes_nothing() {
    let run = |forger: bool| {
        let mut s = cluster();
        for node in 0..NODES {
            s.set_gossip_auth(node, required(&[A]));
        }
        if forger {
            let wrong = required(&[FORGED]);
            let mut seq = 0;
            for at in (5..30).map(|second| second * ONE_SEC + 7 * ONE_MS) {
                for to in 0..NODES {
                    let from = (to + 1) % NODES;
                    seq += 1;
                    s.schedule_message(at, from, to, forgery(seq));
                    s.schedule_message(at, from, to, wrong.seal(&forgery(seq)));
                }
            }
        }
        s.run_until(END);
        s
    };

    let clean = run(false);
    let forged = run(true);
    for node in 0..NODES {
        assert_eq!(clean.rejected(node), 0, "node {node}");
        assert_eq!(forged.rejected(node), 2 * 25, "node {node}");
        assert_eq!(forged.node(node).cluster_size(), NODES, "node {node}");
    }
    assert!(
        outcome(&clean) == outcome(&forged),
        "the forgery changed membership, shares or admissions"
    );
}

// The forgery above is no dud: a cluster that checks nothing takes it.
#[test]
fn the_same_forgery_bites_a_cluster_without_signing() {
    let mut s = cluster();
    s.schedule_message(5 * ONE_SEC, 1, 0, forgery(1));
    s.run_until(5 * ONE_SEC + 10 * ONE_MS);
    assert_eq!(s.node(0).members().status(peer_of(2)), Some(Status::Dead));
}
