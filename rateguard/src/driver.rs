//! The background task: the node, the socket and the ticker.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use rateguard_core::boundary::{Action, Event, PeerId};
use rateguard_core::node::Node;

use crate::guard::Shared;
use crate::transport::Transport;

/// The sender of a datagram: the member of its first record, which every
/// message opens with (spec §3). Not the socket address, which a NAT on the
/// way may have rewritten.
pub(crate) fn sender(bytes: &[u8]) -> Option<PeerId> {
    let message = rateguard_proto::decode(bytes).ok()?;
    message
        .updates()
        .first()
        .map(|update| PeerId::new(update.member))
}

/// Runs the node until the last `Guard` is gone; it notices on the next
/// tick.
pub(crate) async fn run<T: Transport>(
    mut node: Node,
    transport: T,
    shared: Weak<Shared>,
    mut ticker: tokio::time::Interval,
) {
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Sending is given one tick: a transport that never completes a send
    // must not freeze the node, nor keep it alive once the last Guard is
    // gone.
    let budget = ticker.period();
    let mut buf = vec![0u8; rateguard_proto::MAX_DATAGRAM];
    loop {
        // The shared state is held only while the node works, never across
        // an await: the last Guard dropped must be noticed.
        let actions = tokio::select! {
            received = transport.recv_from(&mut buf) => {
                let Some(shared) = shared.upgrade() else { return };
                match received {
                    Ok((len, _)) => match sender(&buf[..len]) {
                        Some(from) => {
                            let event = Event::MessageReceived { from, bytes: &buf[..len] };
                            node.handle(event, shared.now()).to_vec()
                        }
                        None => Vec::new(),
                    },
                    Err(_) => Vec::new(),
                }
            }
            _ = ticker.tick() => {
                let Some(shared) = shared.upgrade() else { return };
                sync(&mut node, &shared)
            }
        };
        send(&node, &transport, actions, budget).await;
    }
}

/// Waits for the background task. If it panicked, a bug, the node is gone
/// from its peers' view, and they hand its share out among themselves:
/// kept at its last quotas, it would admit that share a second time. It
/// drops every key to the floor `R × β / N` instead, the part of the limit
/// a cluster leaves to each member, and says it no longer runs.
pub(crate) async fn watch(
    task: tokio::task::JoinHandle<()>,
    shared: Weak<Shared>,
    config: rateguard_core::limiter::Config,
) {
    let Err(error) = task.await else { return };
    let Some(shared) = shared.upgrade() else {
        return;
    };
    if error.is_panic() {
        let n = shared.cluster_size.load(Ordering::Relaxed).max(1);
        let floor = config.limit_per_sec as f64 * config.floor_factor / n as f64;
        let quota = rateguard_core::gcra::Quota::per_second(floor, config.burst);
        let now = shared.now();
        shared
            .table
            .for_each(|_, slot| slot.gcra.set_quota(quota, now));
        shared
            .table
            .for_each_free(|slot| slot.gcra.set_quota(quota, now));
        shared.table.overflow().gcra.set_quota(quota, now);
    }
    shared.running.store(false, Ordering::Relaxed);
}

// One tick: attempts in, the core's round, quotas out (spec §5.3).
fn sync(node: &mut Node, shared: &Arc<Shared>) -> Vec<Action> {
    let now = shared.now();
    shared.table.for_each(|key, slot| {
        let n = slot.attempts.swap(0, crate::sync::Ordering::Relaxed);
        if n > 0 {
            node.record_attempts(key, n, now);
        }
    });
    let actions = node.handle(Event::Tick, now).to_vec();

    let new_key = node.new_key_quota();
    shared.table.for_each(|key, slot| match node.quota(key) {
        Some(quota) => slot.gcra.set_quota(quota, now),
        None if !slot.gcra.has_debt(now) => {
            shared.table.release(slot, key, new_key);
        }
        None => {}
    });
    shared
        .table
        .for_each_free(|slot| slot.gcra.set_quota(new_key, now));
    shared.table.overflow().gcra.set_quota(new_key, now);
    shared
        .cluster_size
        .store(node.cluster_size(), Ordering::Relaxed);
    actions
}

// The whole batch gets the budget, not each datagram: three PING-REQs on a
// hung transport would hold the node three ticks. A lost datagram, or one
// that took too long to leave, is the protocol's normal case.
async fn send<T: Transport>(node: &Node, transport: &T, actions: Vec<Action>, budget: Duration) {
    let batch = async {
        for Action::SendTo { peer, bytes } in actions {
            if let Some(address) = node.address(peer) {
                let _ = transport.send_to(&bytes, address.into()).await;
            }
        }
    };
    let _ = tokio::time::timeout(budget, batch).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rateguard_proto::{Address, Message, Status, Update, encode};

    struct HungSend;
    impl Transport for HungSend {
        async fn send_to(&self, _: &[u8], _: std::net::SocketAddr) -> std::io::Result<usize> {
            std::future::pending().await
        }
        async fn recv_from(&self, _: &mut [u8]) -> std::io::Result<(usize, std::net::SocketAddr)> {
            std::future::pending().await
        }
    }

    // A probe gone unanswered sends three PING-REQs in one tick: on a hung
    // transport they must cost the node one tick, not three.
    #[tokio::test(start_paused = true)]
    async fn a_hung_transport_costs_one_budget_per_batch() {
        use rateguard_core::limiter::Config;
        use rateguard_core::node::SwimConfig;

        let config = Config {
            limit_per_sec: 100,
            burst: 5,
            alpha: 0.5,
            floor_factor: 0.05,
            cooldown: 5_000_000_000,
            hot_set_size: 4,
            max_tracked_keys: 8,
            demand_time_constant: 1_000_000_000,
        };
        let me = Address::V4([10, 0, 0, 1], 7946);
        let mut node = Node::new(config, SwimConfig::default(), PeerId::new(1), me, 1);
        let actions: Vec<Action> = (2..5)
            .map(|peer| {
                node.introduce(PeerId::new(peer), Address::V4([10, 0, 0, peer as u8], 7946));
                Action::SendTo {
                    peer: PeerId::new(peer),
                    bytes: vec![1],
                }
            })
            .collect();

        let budget = Duration::from_millis(50);
        let start = tokio::time::Instant::now();
        send(&node, &HungSend, actions, budget).await;
        assert_eq!(start.elapsed(), budget);
    }

    #[test]
    fn the_sender_is_the_first_record_not_the_socket_address() {
        let me = Address::V4([10, 0, 0, 9], 7946);
        let bytes = encode(&Message::Ping {
            seq: 1,
            updates: vec![Update {
                member: 77,
                addr: me,
                incarnation: 0,
                status: Status::Alive,
            }],
            demand: Vec::new(),
        })
        .unwrap();
        assert_eq!(sender(&bytes).map(|peer| peer.get()), Some(77));

        let empty = encode(&Message::Ping {
            seq: 1,
            updates: Vec::new(),
            demand: Vec::new(),
        })
        .unwrap();
        assert_eq!(sender(&empty), None);
        assert_eq!(sender(&[0xde, 0xad]), None);
    }
}
