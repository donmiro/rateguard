//! The seam between membership and everything that depends on it.
//!
//! Allocation needs to know who is in the cluster, not how SWIM finds out,
//! so membership sits behind the [`Membership`] trait and another
//! implementation could take its place. Changes are drained rather than
//! pushed to subscribers: the core is sans-I/O and owns no channels.

use crate::boundary::PeerId;
use std::collections::BTreeSet;

/// A peer entering or leaving [`peers`](Membership::peers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    Joined(PeerId),
    Left(PeerId),
}

/// Who is in the cluster, as the rest of the node needs to know it.
///
/// [`check_contract`] lists what every implementation must hold.
pub trait Membership {
    fn local(&self) -> PeerId;

    /// The live peers in ascending order, never including the node itself.
    fn peers(&self) -> &[PeerId];

    /// The changes since the last call, in order. A join and a leave that
    /// cancel out are both reported: a consumer may have per-peer state to
    /// drop.
    fn drain_changes(&mut self) -> Vec<Change>;

    /// N, the number of nodes sharing the limit, this one included.
    fn cluster_size(&self) -> usize {
        self.peers().len() + 1
    }
}

/// Checks the contract of [`Membership`]: no self among the peers, peers
/// strictly ascending, and a cluster size that matches them. For tests and
/// the simulator's invariants.
pub fn check_contract(membership: &impl Membership) -> Result<(), String> {
    let local = membership.local();
    let peers = membership.peers();

    if peers.contains(&local) {
        return Err(format!("{local:?} lists itself among its peers"));
    }
    if let Some(pair) = peers.windows(2).find(|pair| pair[0] >= pair[1]) {
        return Err(format!(
            "peers are not strictly ascending: {:?} then {:?}",
            pair[0], pair[1]
        ));
    }

    let size = membership.cluster_size();
    if size != peers.len() + 1 {
        return Err(format!(
            "cluster_size is {size}, but {} peers plus self is {}",
            peers.len(),
            peers.len() + 1
        ));
    }

    Ok(())
}

/// A membership set by hand, for tests and fixed fleets.
#[derive(Debug, Clone)]
pub struct ManualMembership {
    local: PeerId,
    peers: Vec<PeerId>,
    changes: Vec<Change>,
}
impl ManualMembership {
    pub fn new(local: PeerId, peers: impl IntoIterator<Item = PeerId>) -> Self {
        let peers: Vec<PeerId> = peers
            .into_iter()
            .filter(|&peer| peer != local)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let changes = peers.iter().copied().map(Change::Joined).collect();

        Self {
            local,
            peers,
            changes,
        }
    }

    pub fn alone(local: PeerId) -> Self {
        Self::new(local, [])
    }

    pub fn add(&mut self, peer: PeerId) -> bool {
        assert_ne!(peer, self.local, "a node is not its own peer");
        match self.peers.binary_search(&peer) {
            Ok(_) => false,
            Err(at) => {
                self.peers.insert(at, peer);
                self.changes.push(Change::Joined(peer));
                true
            }
        }
    }

    pub fn remove(&mut self, peer: PeerId) -> bool {
        match self.peers.binary_search(&peer) {
            Ok(at) => {
                self.peers.remove(at);
                self.changes.push(Change::Left(peer));
                true
            }
            Err(_) => false,
        }
    }
}
impl Membership for ManualMembership {
    fn local(&self) -> PeerId {
        self.local
    }

    fn peers(&self) -> &[PeerId] {
        &self.peers
    }

    fn drain_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(raw: u64) -> PeerId {
        PeerId::new(raw)
    }

    fn replay(onto: &mut BTreeSet<PeerId>, changes: &[Change]) {
        for change in changes {
            match *change {
                Change::Joined(peer) => {
                    assert!(onto.insert(peer), "{peer:?} joined twice without leaving")
                }
                Change::Left(peer) => {
                    assert!(onto.remove(&peer), "{peer:?} left without joining")
                }
            }
        }
    }

    #[test]
    fn a_lone_node_is_a_cluster_of_one() {
        let mut m = ManualMembership::alone(id(1));
        assert_eq!(m.cluster_size(), 1);
        assert!(m.peers().is_empty());
        assert!(m.drain_changes().is_empty());
        check_contract(&m).unwrap();
    }

    #[test]
    fn peers_are_sorted_unique_and_never_self() {
        let m = ManualMembership::new(id(2), [id(5), id(2), id(3), id(5), id(1)]);
        assert_eq!(m.peers(), [id(1), id(3), id(5)]);
        assert_eq!(m.cluster_size(), 4);
        check_contract(&m).unwrap();
    }

    #[test]
    fn the_starting_peers_are_reported_as_joins() {
        let mut m = ManualMembership::new(id(0), [id(2), id(1)]);
        assert_eq!(
            m.drain_changes(),
            [Change::Joined(id(1)), Change::Joined(id(2))]
        );
        assert!(m.drain_changes().is_empty(), "a drain must empty the queue");
    }

    #[test]
    fn only_real_changes_are_reported() {
        let mut m = ManualMembership::new(id(0), [id(1)]);
        m.drain_changes();

        assert!(!m.add(id(1)), "already a peer");
        assert!(!m.remove(id(9)), "never was a peer");
        assert!(m.drain_changes().is_empty());

        assert!(m.add(id(2)));
        assert!(m.remove(id(1)));
        assert_eq!(
            m.drain_changes(),
            [Change::Joined(id(2)), Change::Left(id(1))]
        );
    }

    #[test]
    fn a_join_and_leave_between_drains_are_both_reported() {
        let mut m = ManualMembership::alone(id(0));
        m.add(id(7));
        m.remove(id(7));
        assert_eq!(
            m.drain_changes(),
            [Change::Joined(id(7)), Change::Left(id(7))],
            "coalescing would hide from a consumer that the peer ever existed"
        );
    }

    #[test]
    fn replaying_the_changes_rebuilds_the_peers() {
        let mut m = ManualMembership::new(id(0), [id(3), id(1)]);
        let mut seen = BTreeSet::new();
        replay(&mut seen, &m.drain_changes());

        m.add(id(2));
        m.remove(id(3));
        m.add(id(3));
        m.remove(id(1));
        replay(&mut seen, &m.drain_changes());

        assert!(seen.iter().copied().eq(m.peers().iter().copied()));
        check_contract(&m).unwrap();
    }

    #[test]
    #[should_panic(expected = "not its own peer")]
    fn a_node_cannot_add_itself() {
        ManualMembership::alone(id(0)).add(id(0));
    }

    #[test]
    fn the_trait_can_sit_behind_a_pointer() {
        let m: Box<dyn Membership> = Box::new(ManualMembership::new(id(0), [id(1)]));
        assert_eq!(m.cluster_size(), 2);
    }

    struct Broken {
        peers: Vec<PeerId>,
        size: usize,
    }
    impl Membership for Broken {
        fn local(&self) -> PeerId {
            id(0)
        }

        fn peers(&self) -> &[PeerId] {
            &self.peers
        }

        fn drain_changes(&mut self) -> Vec<Change> {
            Vec::new()
        }

        fn cluster_size(&self) -> usize {
            self.size
        }
    }

    #[test]
    fn the_contract_catches_each_way_to_break_it() {
        let listing_self = Broken {
            peers: vec![id(0), id(1)],
            size: 3,
        };
        assert!(
            check_contract(&listing_self)
                .unwrap_err()
                .contains("itself")
        );

        let unsorted = Broken {
            peers: vec![id(2), id(1)],
            size: 3,
        };
        assert!(check_contract(&unsorted).unwrap_err().contains("ascending"));

        let repeated = Broken {
            peers: vec![id(1), id(1)],
            size: 3,
        };
        assert!(check_contract(&repeated).unwrap_err().contains("ascending"));

        let miscounted = Broken {
            peers: vec![id(1)],
            size: 1,
        };
        assert!(
            check_contract(&miscounted)
                .unwrap_err()
                .contains("cluster_size")
        );
    }
}
