#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}
impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn up_to(&mut self, max: u64) -> u64 {
        let draw = self.next_u64();
        match max.checked_add(1) {
            None => draw,
            Some(span) => ((draw as u128 * span as u128) >> 64) as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stream_matches_the_reference_splitmix64() {
        let mut rng = Rng::new(0);
        assert_eq!(rng.next_u64(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(rng.next_u64(), 0x6e78_9e6a_a1b9_65f4);
    }

    #[test]
    fn a_seed_is_the_whole_state() {
        let mut a = Rng::new(8371);
        let mut b = Rng::new(8371);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn units_stays_in_the_half_open_interval() {
        let mut rng = Rng::new(1);
        for _ in 0..10_000 {
            let x = rng.unit();
            assert!((0.0..1.0).contains(&x), "{x}");
        }
    }

    #[test]
    fn up_to_is_inclusive_and_bounded() {
        let mut rng = Rng::new(2);
        assert_eq!(rng.up_to(0), 0);

        let mut seen = [false; 4];
        for _ in 0..1000 {
            let x = rng.up_to(3);
            assert!(x <= 3, "{x}");
            seen[x as usize] = true;
        }
        assert_eq!(seen, [true; 4], "both ends of 0..=3 must be reachable");

        rng.up_to(u64::MAX);
    }
}
