use std::time::Duration;

pub type Nanos = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quota {
    rate_per_sec: u32,
    burst: u32,
}
impl Quota {
    pub fn new(rate_per_sec: u32, burst: u32) -> Self {
        assert!(rate_per_sec > 0, "rate_per_sec must be > 0");
        assert!(burst > 0, "burst must be > 0");
        Self {
            rate_per_sec,
            burst,
        }
    }

    fn t_nanos(&self) -> Nanos {
        let one_sec = 1_000_000_000u64;
        one_sec.div_ceil(self.rate_per_sec as u64)
    }

    fn tau_nanos(&self) -> Nanos {
        self.t_nanos() * (self.burst as u64 - 1)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Gcra {
    quota: Quota,
    tat: Option<Nanos>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny { retry_at: Nanos },
}
impl Gcra {
    pub fn new(quota: Quota) -> Self {
        Self { quota, tat: None }
    }

    pub fn check(&mut self, now: Nanos) -> Decision {
        let t = self.quota.t_nanos();
        let tau = self.quota.tau_nanos();

        let tat = self.tat.unwrap_or(now);
        let earliest = tat.saturating_sub(tau);

        if now < earliest {
            return Decision::Deny { retry_at: earliest };
        }

        self.tat = Some(std::cmp::max(tat, now) + t);
        Decision::Allow
    }

    pub fn set_quota(&mut self, quota: Quota, now: Nanos) {
        if let Some(old_tat) = self.tat {
            let old_t = self.quota.t_nanos();
            let debt_nanos = old_tat.saturating_sub(now);
            let debt_slots = debt_nanos as f64 / old_t as f64;

            self.quota = quota;
            let new_t = self.quota.t_nanos();
            let new_debt_nanos = (debt_slots * new_t as f64).round() as u64;
            self.tat = Some(now + new_debt_nanos);
        } else {
            self.quota = quota;
        }
    }

    pub fn retry_after(&self, now: Nanos) -> Option<Duration> {
        let tat = self.tat?;
        let tau = self.quota.tau_nanos();
        let earliest = tat.saturating_sub(tau);
        (now < earliest).then(|| Duration::from_nanos(earliest - now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota(rate: u32, burst: u32) -> Quota {
        Quota::new(rate, burst)
    }

    #[test]
    #[should_panic]
    fn negative_rps_or_burst_schould_panic() {
        Quota::new(0 as u32, 0 as u32);
    }

    #[test]
    fn burst_of_n_then_deny() {
        let mut g = Gcra::new(quota(1000, 3));
        let t0: Nanos = 0;

        assert_eq!(g.check(t0), Decision::Allow);
        assert_eq!(g.check(t0), Decision::Allow);
        assert_eq!(g.check(t0), Decision::Allow);
        assert!(matches!(g.check(t0), Decision::Deny { .. }));
    }

    #[test]
    fn deny_then_allow_after_retry_at() {
        let mut g = Gcra::new(quota(1000, 1));
        let t0: Nanos = 0;

        assert_eq!(g.check(t0), Decision::Allow);
        let Decision::Deny { retry_at } = g.check(t0) else {
            panic!("second immidiate request must be denied");
        };
        assert_eq!(g.check(retry_at), Decision::Allow);
    }

    #[test]
    fn idle_key_does_not_accumulate_unlimited_credit() {
        let mut g = Gcra::new(quota(1000, 3));
        let t0: Nanos = 0;
        let later = t0 + 10_000_000_000;

        assert_eq!(g.check(later), Decision::Allow);
        assert_eq!(g.check(later), Decision::Allow);
        assert_eq!(g.check(later), Decision::Allow);
        assert!(matches!(g.check(later), Decision::Deny { .. }));
    }

    #[test]
    fn set_quota_preserves_debt_proportionally() {
        let mut g = Gcra::new(quota(1000, 10));
        let t0: Nanos = 0;
        for _ in 0..10 {
            assert_eq!(g.check(t0), Decision::Allow);
        }
        assert!(matches!(g.check(t0), Decision::Deny { .. }));

        g.set_quota(quota(100, 10), t0);
        assert!(matches!(g.check(t0), Decision::Deny { .. }));
    }
}
