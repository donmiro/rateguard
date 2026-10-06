use rateguard_proto::Update;
use std::collections::BTreeMap;

pub const RETRANSMIT_MULT: u32 = 4;

// SWIM spreads a piece of news λ·log(N) times: enough for an infection to
// reach the whole cluster with high probability, and no more.
pub fn retransmit_limit(cluster_size: usize) -> u32 {
    assert!(cluster_size > 0, "a cluster has at least the local node");
    let rounds = ((cluster_size + 1) as f64).log10().ceil() as u32;
    RETRANSMIT_MULT * rounds.max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pending {
    update: Update,
    transmits: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Gossip {
    queue: BTreeMap<u64, Pending>,
}
impl Gossip {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn push(&mut self, update: Update) -> bool {
        if let Some(pending) = self.queue.get(&update.member)
            && !update.supersedes(&pending.update)
        {
            return false;
        }
        self.queue.insert(
            update.member,
            Pending {
                update,
                transmits: 0,
            },
        );
        true
    }

    pub fn take(&mut self, max: usize, limit: u32) -> Vec<Update> {
        assert!(limit > 0, "news sent zero times reaches nobody");

        let mut freshest: Vec<(u32, u64)> = self
            .queue
            .iter()
            .map(|(&member, pending)| (pending.transmits, member))
            .collect();
        freshest.sort_unstable();
        freshest.truncate(max);

        freshest
            .into_iter()
            .map(|(_, member)| {
                let pending = self.queue.get_mut(&member).expect("just listed");
                pending.transmits += 1;
                let update = pending.update;
                if pending.transmits >= limit {
                    self.queue.remove(&member);
                }
                update
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rateguard_proto::Status::{self, Alive, Dead, Suspect};

    fn news(member: u64, incarnation: u32, status: Status) -> Update {
        Update {
            member,
            incarnation,
            status,
        }
    }

    #[test]
    fn the_limit_grows_with_the_log_of_the_cluster() {
        assert_eq!(retransmit_limit(1), 4);
        assert_eq!(retransmit_limit(9), 4);
        assert_eq!(retransmit_limit(10), 8);
        assert_eq!(retransmit_limit(99), 8);
        assert_eq!(retransmit_limit(100), 12);
    }

    #[test]
    fn nothing_to_say_is_an_empty_list() {
        assert!(Gossip::new().take(16, 4).is_empty());
    }

    #[test]
    fn news_is_sent_up_to_the_limit_then_forgotten() {
        let mut g = Gossip::new();
        g.push(news(1, 0, Suspect));

        for _ in 0..3 {
            assert_eq!(g.take(16, 3), [news(1, 0, Suspect)]);
        }
        assert!(g.take(16, 3).is_empty());
        assert!(g.is_empty());
    }

    #[test]
    fn the_least_sent_news_goes_first() {
        let mut g = Gossip::new();
        g.push(news(1, 0, Suspect));
        g.push(news(2, 0, Suspect));
        assert_eq!(g.take(1, 10), [news(1, 0, Suspect)]);

        g.push(news(3, 0, Dead));
        assert_eq!(
            g.take(2, 10),
            [news(2, 0, Suspect), news(3, 0, Dead)],
            "never sent beats sent once"
        );
        assert_eq!(g.take(1, 10), [news(1, 0, Suspect)]);
    }

    #[test]
    fn a_message_carries_at_most_max_updates() {
        let mut g = Gossip::new();
        for member in 0..40 {
            g.push(news(member, 0, Alive));
        }
        assert_eq!(g.take(16, 4).len(), 16);
        assert_eq!(g.len(), 40);
    }

    #[test]
    fn newer_news_replaces_the_old_and_restarts_its_count() {
        let mut g = Gossip::new();
        g.push(news(1, 0, Suspect));
        g.take(16, 2);

        assert!(g.push(news(1, 1, Alive)));
        assert_eq!(g.len(), 1, "one entry per member");
        assert_eq!(g.take(16, 2), [news(1, 1, Alive)]);
        assert_eq!(g.take(16, 2), [news(1, 1, Alive)], "a fresh count");
        assert!(g.is_empty());
    }

    #[test]
    fn stale_news_does_not_replace_the_fresh() {
        let mut g = Gossip::new();
        g.push(news(1, 2, Alive));
        assert!(!g.push(news(1, 1, Dead)));
        assert!(!g.push(news(1, 2, Alive)), "an echo");
        assert_eq!(g.take(16, 4), [news(1, 2, Alive)]);
    }

    #[test]
    #[should_panic(expected = "reaches nobody")]
    fn a_zero_limit_is_rejected() {
        Gossip::new().take(16, 0);
    }
}
