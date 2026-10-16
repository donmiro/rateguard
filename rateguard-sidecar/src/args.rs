//! The sidecar's flags, each also a `RATEGUARD_*` variable for Kubernetes.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use clap::builder::BoolishValueParser;
use clap::{ArgAction, Parser};
use rateguard::{Builder, Guard, Limit, PartitionPolicy};

/// rateguard as a sidecar: one global rate limit across a fleet, for a
/// service in any language, over HTTP on loopback.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Args {
    /// Requests per second per key in the default class, across the fleet.
    #[arg(long, env = "RATEGUARD_LIMIT")]
    pub limit: u32,
    /// Requests one instance may admit back to back; limit / 20 by default.
    #[arg(long, env = "RATEGUARD_BURST")]
    pub burst: Option<u32>,
    /// A class of limits, as name=rate or name=rate/burst; repeated, or
    /// comma-separated in the variable.
    #[arg(long = "class", env = "RATEGUARD_CLASSES", value_delimiter = ',')]
    pub classes: Vec<ClassSpec>,
    /// UDP address for gossip with the other instances, as ip:port.
    #[arg(long, env = "RATEGUARD_BIND", default_value = "0.0.0.0:7946")]
    pub bind: String,
    /// Where the other instances reach this one; required when bound to
    /// 0.0.0.0. In Kubernetes, the pod IP.
    #[arg(long, env = "RATEGUARD_ADVERTISE")]
    pub advertise: Option<String>,
    /// Peers to join through, as ip:port or host:port, comma-separated;
    /// one Kubernetes headless service name will do.
    #[arg(long, env = "RATEGUARD_SEEDS", value_delimiter = ',')]
    pub seeds: Vec<String>,
    /// `optimistic`, `quorum`, or `hold-down:<duration>` such as `hold-down:10s`.
    #[arg(long, env = "RATEGUARD_PARTITION_POLICY")]
    pub partition_policy: Option<Policy>,
    /// Keys one instance keeps state for at once; 4096 by default.
    #[arg(long, env = "RATEGUARD_TRACKED_KEYS")]
    pub tracked_keys: Option<usize>,
    /// How many keys may be coordinated at once, at most 64.
    #[arg(long, env = "RATEGUARD_HOT_KEYS")]
    pub hot_keys: Option<usize>,
    /// How many named classes the instance keeps; 64 by default.
    #[arg(long, env = "RATEGUARD_MAX_CLASSES")]
    pub max_classes: Option<usize>,
    /// Admit every request, counting those that would be denied.
    #[arg(long, env = "RATEGUARD_DRY_RUN", action = ArgAction::SetTrue, value_parser = BoolishValueParser::new())]
    pub dry_run: bool,
    /// Where the HTTP API listens. Not on loopback, anyone who reaches it
    /// can check keys, and with --admin change limits.
    #[arg(long, env = "RATEGUARD_LISTEN", default_value = "127.0.0.1:7947")]
    pub listen: SocketAddr,
    /// Also serve /healthz and /metrics, and only those, here: for
    /// kubelet's probes and Prometheus, which reach the pod's IP, not its
    /// loopback.
    #[arg(long, env = "RATEGUARD_METRICS_LISTEN")]
    pub metrics_listen: Option<SocketAddr>,
    /// Serve the routes that change limits: PUT /v1/classes, /v1/limit,
    /// /v1/dry_run.
    #[arg(long, env = "RATEGUARD_ADMIN", action = ArgAction::SetTrue, value_parser = BoolishValueParser::new())]
    pub admin: bool,
    /// How long to go on answering after leaving the cluster on SIGTERM,
    /// at the floor: the service next to it gets the signal at the same
    /// moment and drains its requests meanwhile. Keep it under the pod's
    /// termination grace period.
    #[arg(long, env = "RATEGUARD_DRAIN", default_value = "10s")]
    pub drain: Wait,
}
impl Args {
    /// A [`Guard`] builder set up as the flags say.
    pub fn builder(&self) -> Builder {
        let mut builder = Guard::builder()
            .limit(self.limit)
            .bind(&self.bind)
            .seeds(self.seeds.iter().cloned())
            .dry_run(self.dry_run);
        if let Some(burst) = self.burst {
            builder = builder.burst(burst);
        }
        if let Some(advertise) = &self.advertise {
            builder = builder.advertise(advertise);
        }
        if let Some(Policy(policy)) = self.partition_policy {
            builder = builder.partition_policy(policy);
        }
        if let Some(n) = self.tracked_keys {
            builder = builder.tracked_keys(n);
        }
        if let Some(n) = self.hot_keys {
            builder = builder.hot_keys(n);
        }
        if let Some(n) = self.max_classes {
            builder = builder.max_classes(n);
        }
        for ClassSpec(name, limit) in &self.classes {
            builder = builder.class(name, *limit);
        }
        builder
    }
}

/// `name=rate` or `name=rate/burst`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassSpec(pub String, pub Limit);
impl FromStr for ClassSpec {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let bad = || format!("not name=rate or name=rate/burst: {text}");
        let (name, limit) = text.split_once('=').ok_or_else(bad)?;
        if name.is_empty() {
            return Err(bad());
        }
        let (rate, burst) = match limit.split_once('/') {
            Some((rate, burst)) => (rate, Some(burst)),
            None => (limit, None),
        };
        let rate: u32 = rate.parse().map_err(|_| bad())?;
        let limit = match burst {
            Some(burst) => Limit::per_second(rate).burst(burst.parse().map_err(|_| bad())?),
            None => Limit::per_second(rate),
        };
        Ok(Self(name.to_owned(), limit))
    }
}

/// A [`PartitionPolicy`] by name: `optimistic`, `quorum`, or
/// `hold-down:<duration>` with a unit, `ms`, `s` or `m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy(pub PartitionPolicy);
impl FromStr for Policy {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        match text {
            "optimistic" => return Ok(Self(PartitionPolicy::Optimistic)),
            "quorum" => return Ok(Self(PartitionPolicy::Quorum)),
            _ => {}
        }
        let bad = || format!("not optimistic, quorum or hold-down:<duration>: {text}");
        let hold = text.strip_prefix("hold-down:").ok_or_else(bad)?;
        let duration = duration(hold).ok_or_else(bad)?;
        Ok(Self(PartitionPolicy::HoldDown(duration)))
    }
}

/// A duration with a unit: `500ms`, `10s`, `2m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wait(pub Duration);
impl FromStr for Wait {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        duration(text)
            .map(Self)
            .ok_or_else(|| format!("not a duration such as 500ms, 10s or 2m: {text}"))
    }
}

fn duration(text: &str) -> Option<Duration> {
    let (number, unit) = text.split_at(text.find(|c: char| !c.is_ascii_digit())?);
    let n: u64 = number.parse().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(n)),
        "s" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_secs(n.checked_mul(60)?)),
        _ => None,
    }
}
