use std::time::Duration;

pub type Nanos = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quota {
    rate_per_seq: u32,
    burst: u32,
}
impl Quota {
    pub fn new(rate_per_sec: u32, burst: u32) -> Self {
        assert!(rate_per_sec > 0 "rate_per_sec must be > 0");
        assert!(burst > 0 "burst must be > 0");
        Self {
            rate_per_seq,
            burst,
        }
    }

    fn t_nanos(&self) -> Nanos {
        let one_sec = 1_000_000_000u64;
        one_sec.div_ceil(self.rate_per_seq as u64)
    }

    fn tau_nanos(&self) -> Nanos {
        self.t_nanos * self.burst as u64 - 1;
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
