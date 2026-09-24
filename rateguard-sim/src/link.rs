use rateguard_core::gcra::Nanos;

use crate::rng::Rng;

pub type NodeIndex = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fate {
    Lost,
    Delivered(Nanos),
    Duplicated(Nanos, Nanos),
}
impl Fate {
    pub fn arrivals(self) -> impl Iterator<Item = Nanos> {
        let (first, second) = match self {
            Fate::Lost => (None, None),
            Fate::Delivered(at) => (Some(at), None),
            Fate::Duplicated(a, b) => (Some(a), Some(b)),
        };
        first.into_iter().chain(second)
    }
}

pub trait Link {
    fn fate(&mut self, from: NodeIndex, to: NodeIndex, now: Nanos, len: usize) -> Fate;
}

fn assert_real_latency(latency: Nanos) {
    assert!(
        latency > 0,
        "latency must be > 0: a datagram that arrives in the same nanosecond it was sent is not a network, and it hides every ordering bug"
    );
}

pub struct PerfectLink {
    latency: Nanos,
}
impl PerfectLink {
    pub fn new(latency: Nanos) -> Self {
        assert_real_latency(latency);
        Self { latency }
    }
}
impl Link for PerfectLink {
    fn fate(&mut self, _from: NodeIndex, _to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        Fate::Delivered(now + self.latency)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NetConfig {
    pub latency: Nanos,
    pub jitter: Nanos,
    pub loss: f64,
    pub duplicate: f64,
    pub reorder: f64,
    pub reorder_window: Nanos,
}
impl NetConfig {
    pub fn perfect(latency: Nanos) -> Self {
        Self {
            latency,
            jitter: 0,
            loss: 0.0,
            duplicate: 0.0,
            reorder: 0.0,
            reorder_window: 0,
        }
    }
}

pub struct SeededLink {
    config: NetConfig,
    rng: Rng,
}
impl SeededLink {
    pub fn new(seed: u64, config: NetConfig) -> Self {
        assert_real_latency(config.latency);
        for (name, p) in [
            ("loss", config.loss),
            ("duplicate", config.duplicate),
            ("reorder", config.reorder),
        ] {
            assert!((0.0..=1.0).contains(&p), "{name} is a probability, got {p}");
        }

        Self {
            config,
            rng: Rng::new(seed),
        }
    }

    // A held-back datagram is overtaken by the ones sent after it: that is
    // what reordering is. Jitter alone reorders only datagrams sent closer
    // together than the jitter itself.
    fn arrival(&mut self, now: Nanos) -> Nanos {
        let jitter = self.rng.up_to(self.config.jitter);
        let held = self.rng.unit() < self.config.reorder;
        let hold = self.rng.up_to(self.config.reorder_window);

        now + self.config.latency + jitter + if held { hold } else { 0 }
    }
}
impl Link for SeededLink {
    fn fate(&mut self, _from: NodeIndex, _to: NodeIndex, now: Nanos, _len: usize) -> Fate {
        // Every datagram consumes the same number of draws whatever its fate,
        // so turning one knob does not reshuffle the fates of all later ones.
        let lost = self.rng.unit() < self.config.loss;
        let duplicated = self.rng.unit() < self.config.duplicate;
        let first = self.arrival(now);
        let second = self.arrival(now);

        match (lost, duplicated) {
            (true, _) => Fate::Lost,
            (false, false) => Fate::Delivered(first),
            (false, true) => Fate::Duplicated(first, second),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_MS: Nanos = 1_000_000;
    const SEED: u64 = 8371;

    fn chaos() -> NetConfig {
        NetConfig {
            latency: ONE_MS,
            jitter: 4 * ONE_MS,
            loss: 0.3,
            duplicate: 0.1,
            reorder: 0.05,
            reorder_window: 50 * ONE_MS,
        }
    }

    fn fates(seed: u64, config: NetConfig, count: u64) -> Vec<(Nanos, Fate)> {
        let mut link = SeededLink::new(seed, config);
        (0..count)
            .map(|i| {
                let now = i * ONE_MS;
                (now, link.fate(0, 1, now, 64))
            })
            .collect()
    }
    fn count(fates: &[(Nanos, Fate)], pred: impl Fn(&Fate) -> bool) -> usize {
        fates.iter().filter(|(_, f)| pred(f)).count()
    }

    #[test]
    #[should_panic]
    fn an_instant_wire_is_rejected() {
        PerfectLink::new(0);
    }

    #[test]
    fn a_perfect_wire_always_delivers_one_latency_later() {
        let mut link = PerfectLink::new(ONE_MS);
        assert_eq!(link.fate(0, 1, 5 * ONE_MS, 64), Fate::Delivered(6 * ONE_MS));
    }

    #[test]
    fn a_fate_lists_every_copy_that_arrives() {
        assert_eq!(Fate::Lost.arrivals().count(), 0);
        assert_eq!(Fate::Delivered(5).arrivals().collect::<Vec<_>>(), vec![5]);
        assert_eq!(
            Fate::Duplicated(7, 3).arrivals().collect::<Vec<_>>(),
            vec![7, 3]
        );
    }

    #[test]
    #[should_panic]
    fn a_seeded_instant_wire_is_rejected() {
        SeededLink::new(SEED, NetConfig::perfect(0));
    }

    #[test]
    #[should_panic(expected = "loss is a probability")]
    fn a_probability_above_one_is_rejected() {
        SeededLink::new(
            SEED,
            NetConfig {
                loss: 1.5,
                ..chaos()
            },
        );
    }

    #[test]
    fn the_same_seed_gives_the_same_fates() {
        assert_eq!(fates(SEED, chaos(), 1000), fates(SEED, chaos(), 1000));
    }

    #[test]
    fn another_seed_gives_other_fates() {
        assert_ne!(fates(SEED, chaos(), 1000), fates(SEED + 1, chaos(), 1000));
    }

    #[test]
    fn all_knobs_at_zero_is_a_perfect_wire() {
        for (now, fate) in fates(SEED, NetConfig::perfect(ONE_MS), 1000) {
            assert_eq!(fate, Fate::Delivered(now + ONE_MS));
        }
    }

    #[test]
    fn full_loss_is_a_dead_wire() {
        let config = NetConfig {
            loss: 1.0,
            ..chaos()
        };
        let all = fates(SEED, config, 1000);
        assert_eq!(count(&all, |f| *f == Fate::Lost), all.len());
    }

    #[test]
    fn loss_and_duplication_happen_at_the_configured_rates() {
        let all = fates(SEED, chaos(), 10_000);

        let lost = count(&all, |f| *f == Fate::Lost);
        assert!((2700..=3300).contains(&lost), "30% loss, lost {lost}");

        let delivered = all.len() - lost;
        let duplicated = count(&all, |f| matches!(f, Fate::Duplicated(..)));
        assert!(
            duplicated * 100 >= delivered * 8 && duplicated * 100 <= delivered * 12,
            "10% of delivered are duplicated, got {duplicated} of {delivered}"
        );
    }

    #[test]
    fn every_copy_arrives_within_the_configured_bounds() {
        let c = chaos();
        for (now, fate) in fates(SEED, c, 10_000) {
            for at in fate.arrivals() {
                assert!(at >= now + c.latency, "faster than the wire: {at}");
                assert!(
                    at <= now + c.latency + c.jitter + c.reorder_window,
                    "held longer than configured: {at}"
                );
            }
        }
    }

    #[test]
    fn a_later_datagram_can_overtake_an_earlier_one() {
        fn inversions(config: NetConfig) -> usize {
            let arrivals: Vec<Nanos> = fates(SEED, config, 10_000)
                .into_iter()
                .filter_map(|(_, f)| f.arrivals().next())
                .collect();
            arrivals.windows(2).filter(|w| w[1] < w[0]).count()
        }

        assert_eq!(
            inversions(NetConfig::perfect(ONE_MS)),
            0,
            "a steady wire keeps the send order"
        );
        assert!(inversions(chaos()) > 0, "chaos must reorder something");
    }

    #[test]
    fn turning_one_knob_keeps_the_other_fates() {
        let calm = fates(
            SEED,
            NetConfig {
                loss: 0.0,
                ..chaos()
            },
            1000,
        );
        let lossy = fates(
            SEED,
            NetConfig {
                loss: 0.5,
                ..chaos()
            },
            1000,
        );

        for ((_, before), (_, after)) in calm.iter().zip(&lossy) {
            if *after != Fate::Lost {
                assert_eq!(before, after, "loss must not move surviving datagrams");
                assert_eq!(before, after, "loss must not move surviving datagrams");
            }
        }
    }
}
