//! The sidecar's flags, each also a `RATEGUARD_*` variable for Kubernetes.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::builder::BoolishValueParser;
use clap::{ArgAction, Parser};
use rateguard::{Builder, GossipAuth, Guard, Limit, PartitionPolicy};

use crate::secrets::{SecretsError, read_secrets};

/// rateguard as a sidecar: one global rate limit across a fleet, for a
/// service in any language, over HTTP on loopback.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Args {
    /// Requests per key in the default class, across the fleet, as
    /// rate, rate/unit or rate/unit/burst, with unit s (the default), min or
    /// h: 100, 1000/min, 5000/h/10.
    #[arg(long, env = "RATEGUARD_LIMIT")]
    pub limit: LimitSpec,
    /// Requests one instance may admit back to back, when the limit gives
    /// none; a twentieth of the count by default.
    #[arg(long, env = "RATEGUARD_BURST")]
    pub burst: Option<u32>,
    /// A class of limits, as name=limit, the limit in the form of
    /// --limit, such as pro=1000/min/50; repeated, or comma-separated in
    /// the variable.
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
    /// How many keys `/v1/stats` lists in `most_denied` and
    /// `most_shadow_denied`: 10 by default, at most 1024, 0 for none.
    #[arg(long, env = "RATEGUARD_MOST_DENIED")]
    pub most_denied: Option<usize>,
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
    /// /v1/dry_run, /v1/key_limits, /v1/shadows.
    #[arg(long, env = "RATEGUARD_ADMIN", action = ArgAction::SetTrue, value_parser = BoolishValueParser::new())]
    pub admin: bool,
    /// How long to go on answering after leaving the cluster on SIGTERM,
    /// at the floor: the service next to it gets the signal at the same
    /// moment and drains its requests meanwhile. Keep it under the pod's
    /// termination grace period.
    #[arg(long, env = "RATEGUARD_DRAIN", default_value = "10s")]
    pub drain: Wait,
    /// A file of gossip secrets, one per line in base64, at most two: the
    /// first signs, either verifies. Reread every 10 s, so that a rotated
    /// Kubernetes Secret takes effect without a restart. Make one with
    /// `openssl rand -base64 32`. Read, and reread, under `--gossip-auth
    /// off` too: a file that is refused stops the start all the same.
    #[arg(long, env = "RATEGUARD_GOSSIP_SECRET_FILE")]
    pub gossip_secret_file: Option<PathBuf>,
    /// `off`, `transitional` or `required`: whether gossip is signed, and
    /// what unsigned gossip is let in. `required` with a secret file,
    /// `off` without, by default.
    #[arg(long = "gossip-auth", env = "RATEGUARD_GOSSIP_AUTH")]
    pub gossip_auth_mode: Option<Mode>,
    /// A file whose bytes, as they are, salt the hashes of keys: at least
    /// 16. The same at every instance; changed only by restarting them all
    /// at once.
    #[arg(long, env = "RATEGUARD_KEY_SALT_FILE")]
    pub key_salt_file: Option<PathBuf>,
}
impl Args {
    /// The mode gossip is signed in: as given, or else `required` with a
    /// secret file and `off` without.
    pub fn gossip_auth(&self) -> GossipAuth {
        match (self.gossip_auth_mode, &self.gossip_secret_file) {
            (Some(Mode(mode)), _) => mode,
            (None, Some(_)) => GossipAuth::Required,
            (None, None) => GossipAuth::Off,
        }
    }

    /// A [`Guard`] builder set up as the flags say, with the secret and
    /// salt files read.
    pub fn builder(&self) -> Result<Builder, ArgsError> {
        let mut builder = Guard::builder()
            .default_limit(self.limit.limit)
            .bind(&self.bind)
            .seeds(self.seeds.iter().cloned())
            .dry_run(self.dry_run);
        // The builder's burst goes on top of the limit's own, so it is
        // left out when the limit gave one.
        if let (Some(burst), false) = (self.burst, self.limit.has_burst) {
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
        if let Some(k) = self.most_denied {
            builder = builder.most_denied(k);
        }
        for ClassSpec(name, limit) in &self.classes {
            builder = builder.class(name, *limit);
        }
        let mode = self.gossip_auth();
        match &self.gossip_secret_file {
            Some(path) => {
                let secrets = read_secrets(path).map_err(ArgsError::Secrets)?;
                // The library would refuse it too, but without naming the
                // file that is empty.
                if secrets.is_empty() && mode != GossipAuth::Off {
                    return Err(ArgsError::NoSecrets);
                }
                builder = builder.gossip_auth(mode, secrets);
            }
            None if mode != GossipAuth::Off => return Err(ArgsError::NoSecretFile(mode)),
            None => {}
        }
        if let Some(path) = &self.key_salt_file {
            let salt = std::fs::read(path).map_err(ArgsError::SaltUnreadable)?;
            // Checked here as well as by the library, so that the error
            // names the flag.
            if salt.len() < MIN_SALT {
                return Err(ArgsError::ShortSalt(salt.len()));
            }
            builder = builder.key_salt(salt);
        }
        Ok(builder)
    }
}

// What `Builder::key_salt` asks for at least.
const MIN_SALT: usize = 16;

/// Why the flags make no builder. Never carries a secret or the salt.
#[derive(Debug)]
#[non_exhaustive]
pub enum ArgsError {
    /// A mode that signs, and no secret file.
    NoSecretFile(GossipAuth),
    /// The secret file refused.
    Secrets(SecretsError),
    /// A mode that signs, and a secret file with no secret in it.
    NoSecrets,
    /// The salt file could not be read.
    SaltUnreadable(std::io::Error),
    /// A salt file of this many bytes, fewer than 16.
    ShortSalt(usize),
}
impl fmt::Display for ArgsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArgsError::NoSecretFile(mode) => write!(
                f,
                "--gossip-auth {} signs gossip, and needs --gossip-secret-file",
                mode_name(*mode)
            ),
            ArgsError::Secrets(error) => write!(f, "--gossip-secret-file: {error}"),
            ArgsError::NoSecrets => write!(f, "--gossip-secret-file: no secret in it"),
            ArgsError::SaltUnreadable(error) => {
                write!(f, "--key-salt-file: cannot read it: {error}")
            }
            ArgsError::ShortSalt(n) => write!(
                f,
                "--key-salt-file: {n} bytes, at least {MIN_SALT} are needed"
            ),
        }
    }
}
impl std::error::Error for ArgsError {}

/// A [`GossipAuth`] by name: `off`, `transitional` or `required`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mode(pub GossipAuth);
impl FromStr for Mode {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        match text {
            "off" => Ok(Self(GossipAuth::Off)),
            "transitional" => Ok(Self(GossipAuth::Transitional)),
            "required" => Ok(Self(GossipAuth::Required)),
            _ => Err(format!("not off, transitional or required: {text}")),
        }
    }
}

/// The name of a mode, as the flag and `/v1/stats` say it.
pub fn mode_name(mode: GossipAuth) -> &'static str {
    match mode {
        GossipAuth::Off => "off",
        GossipAuth::Transitional => "transitional",
        GossipAuth::Required => "required",
        // A mode added to the library before the sidecar learns its name.
        _ => "unknown",
    }
}

/// A [`Limit`] as written in a flag: `rate[/unit][/burst]`, with `unit`
/// one of `s` (the default), `min` or `h`. A second segment that is a
/// number is the burst, as before units: `100/5` is 100 a second with a
/// burst of 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitSpec {
    /// The limit, its burst the default one when none was given.
    pub limit: Limit,
    /// Whether the text gave a burst: if not, `--burst` sets it.
    pub has_burst: bool,
}
impl FromStr for LimitSpec {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let bad = || format!("not rate, rate/unit or rate/unit/burst, unit s, min or h: {text}");
        let mut parts = text.split('/');
        let rate: u32 = parts.next().unwrap_or("").parse().map_err(|_| bad())?;
        let second = parts.next();
        let third = parts.next();
        if parts.next().is_some() {
            return Err(bad());
        }
        let (unit, burst) = match (second, third) {
            (None, _) => ("s", None),
            (Some(n), None) if n.parse::<u32>().is_ok() => ("s", Some(n)),
            (Some(unit), burst) => (unit, burst),
        };
        let limit = match unit {
            "s" => Limit::per_second(rate),
            "min" => Limit::per_minute(rate),
            "h" => Limit::per_hour(rate),
            _ => return Err(bad()),
        };
        match burst {
            Some(burst) => Ok(Self {
                limit: limit.burst(burst.parse().map_err(|_| bad())?),
                has_burst: true,
            }),
            None => Ok(Self {
                limit,
                has_burst: false,
            }),
        }
    }
}

/// `name=rate[/unit][/burst]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassSpec(pub String, pub Limit);
impl FromStr for ClassSpec {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let bad = || format!("not name=rate[/unit][/burst]: {text}");
        let (name, limit) = text.split_once('=').ok_or_else(bad)?;
        if name.is_empty() {
            return Err(bad());
        }
        // The limit's own error would name only its half of the text.
        let LimitSpec { limit, .. } = limit.parse().map_err(|_| bad())?;
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
