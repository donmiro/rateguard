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
    // A send is given one tick: a transport that never completes one must
    // not freeze the node, nor keep it alive once the last Guard is gone.
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

async fn send<T: Transport>(node: &Node, transport: &T, actions: Vec<Action>, budget: Duration) {
    for Action::SendTo { peer, bytes } in actions {
        if let Some(address) = node.address(peer) {
            // A lost datagram, or one that took too long to leave, is the
            // protocol's normal case.
            let _ = tokio::time::timeout(budget, transport.send_to(&bytes, address.into())).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rateguard_proto::{Address, Message, Status, Update, encode};

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
