//! The builder: what a node is told, checked before anything starts.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use rateguard_core::gcra::MAX_BURST;
use rateguard_core::limiter::Config;
use rateguard_core::node::SwimConfig;
use rateguard_core::partition;

/// α: the part of the per-node rate a cold key gets, and the hot threshold.
const ALPHA: f64 = 0.5;
/// β: the floor of a hot key's share; calibrated in the simulator (spec
/// §10.3).
const BETA: f64 = 0.05;
const ONE_SEC: u64 = 1_000_000_000;
/// The most keys one node keeps state for: a table of 2²¹ slots, 64 MB.
const MAX_TRACKED_KEYS: usize = 1 << 20;

/// A duration in nanoseconds, the longest ones held at `u64::MAX` (584
/// years) rather than cut down to whatever their low bits say.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What a node does when its cluster shrinks: the CAP trade-off as a
/// setting. When the network splits into k groups, each sees only itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartitionPolicy {
    /// Shares follow the surviving cluster at once: up to k times the limit
    /// during a split, nothing ever left unused.
    Optimistic,
    /// For this long after the cluster shrinks, shares are computed as if it
    /// had not: about the limit for that long, k times it after. Most
    /// apparent splits are a GC pause or a restart lasting seconds.
    HoldDown(Duration),
    /// A node that cannot see a majority of the cluster it knows drops every
    /// key to a small floor: the limit plus the minority's floors during a
    /// split, and the minority all but stops serving.
    Quorum,
}
impl Default for PartitionPolicy {
    /// A 10-second hold-down.
    fn default() -> Self {
        PartitionPolicy::HoldDown(Duration::from_secs(10))
    }
}
impl PartitionPolicy {
    fn to_core(self) -> partition::PartitionPolicy {
        match self {
            PartitionPolicy::Optimistic => partition::PartitionPolicy::Optimistic,
            PartitionPolicy::HoldDown(hold) => partition::PartitionPolicy::HoldDown(nanos(hold)),
            PartitionPolicy::Quorum => partition::PartitionPolicy::Quorum,
        }
    }
}

/// Configures and starts a node; see [`Guard::builder`](crate::Guard).
#[derive(Debug, Clone, Default)]
pub struct Builder {
    bind: Option<String>,
    advertise: Option<String>,
    seeds: Vec<String>,
    limit: Option<u32>,
    burst: Option<u32>,
    policy: Option<PartitionPolicy>,
    period: Option<Duration>,
    hot_keys: Option<usize>,
    tracked_keys: Option<usize>,
}

impl Builder {
    /// A builder with nothing set; the same as [`Guard::builder`](crate::Guard::builder).
    pub fn new() -> Self {
        Self::default()
    }

    /// The UDP address to listen on, as `ip:port`.
    pub fn bind(mut self, addr: impl Into<String>) -> Self {
        self.bind = Some(addr.into());
        self
    }

    /// The address the other nodes reach this one at. Required when binding
    /// to an unspecified IP (`0.0.0.0`, `::`); in Kubernetes, the pod IP.
    pub fn advertise(mut self, addr: impl Into<String>) -> Self {
        self.advertise = Some(addr.into());
        self
    }

    /// Peers to join through, as `ip:port` or `host:port`; any live member
    /// is enough. A name is looked up when the node starts and again while
    /// it runs, every address it resolves to a seed: a DNS name for the
    /// whole fleet, such as a Kubernetes headless service, keeps up with
    /// instances that come and go. Until the node joins someone, every key
    /// is held at the floor `R × β`, even while a name resolves to nobody
    /// yet: an instance meant to run alone takes no seeds.
    pub fn seeds<S: Into<String>>(mut self, seeds: impl IntoIterator<Item = S>) -> Self {
        self.seeds = seeds.into_iter().map(Into::into).collect();
        self
    }

    /// Requests per second per key, across the whole cluster.
    pub fn limit(mut self, per_sec: u32) -> Self {
        self.limit = Some(per_sec);
        self
    }

    /// How many requests a key may get back to back on one node. Defaults
    /// to `limit / 20`, and never less than 1.
    pub fn burst(mut self, burst: u32) -> Self {
        self.burst = Some(burst);
        self
    }

    /// What to do when the cluster shrinks; defaults to a 10 s hold-down.
    pub fn partition_policy(mut self, policy: PartitionPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// How often nodes exchange membership and demand; defaults to 200 ms.
    pub fn protocol_period(mut self, period: Duration) -> Self {
        self.period = Some(period);
        self
    }

    /// How many keys one node coordinates at once; at most 64, the default.
    pub fn hot_keys(mut self, n: usize) -> Self {
        self.hot_keys = Some(n);
        self
    }

    /// The ceiling on keys one node keeps state for; defaults to 4096.
    pub fn tracked_keys(mut self, n: usize) -> Self {
        self.tracked_keys = Some(n);
        self
    }

    /// Binds the socket and starts the node in the current tokio runtime.
    ///
    /// # Errors
    ///
    /// If a setting is missing or out of range, if there is no tokio
    /// runtime or it lacks timers or IO, or if the socket cannot be bound;
    /// see [`Error`]. Nothing is started then. tokio cannot be asked whether
    /// a runtime has timers and IO, only tried, so a runtime without them
    /// also shows its panic message on stderr, through the panic hook.
    pub fn spawn(self) -> Result<crate::Guard, Error> {
        let mut settings = self.settings()?;
        tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        let ticker = ticker(&settings)?;
        let socket = std::net::UdpSocket::bind(settings.bind).map_err(Error::Bind)?;
        // Advertising the bind address: the port the OS picked if it was 0.
        if self.advertise.is_none() {
            settings.advertise = socket.local_addr().map_err(Error::Bind)?;
        }
        socket.set_nonblocking(true).map_err(Error::Bind)?;
        // Like timers, a runtime built without IO only shows by trying.
        let socket = std::panic::catch_unwind(|| tokio::net::UdpSocket::from_std(socket))
            .map_err(|_| Error::NoIo)?
            .map_err(Error::Bind)?;
        Ok(start(settings, socket, ticker))
    }

    /// [`spawn`](Builder::spawn) over a transport of the caller's, for tests
    /// and simulators; `bind` is then only checked, not bound.
    #[doc(hidden)]
    pub fn spawn_on<T: crate::Transport>(self, transport: T) -> Result<crate::Guard, Error> {
        let settings = self.settings()?;
        tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        let ticker = ticker(&settings)?;
        Ok(start(settings, transport, ticker))
    }

    pub(crate) fn settings(&self) -> Result<Settings, Error> {
        let address = |text: &String| {
            text.parse::<SocketAddr>()
                .map_err(|_| Error::BadAddress(text.clone()))
        };
        let bind = address(self.bind.as_ref().ok_or(Error::MissingBind)?)?;
        let advertise = match &self.advertise {
            Some(text) => address(text)?,
            None if bind.ip().is_unspecified() => return Err(Error::MissingAdvertise),
            None => bind,
        };
        if advertise.ip().is_unspecified() {
            return Err(Error::UnspecifiedAdvertise);
        }
        // Port 0 is the OS's pick, known only once bound. Without an
        // explicit advertise, spawn() advertises the bound socket; with one,
        // port 0 on either side means nobody could reach the node.
        if self.advertise.is_some() && (advertise.port() == 0 || bind.port() == 0) {
            return Err(Error::AdvertisePortZero);
        }
        let mut seeds = Vec::new();
        let mut names = Vec::new();
        for text in &self.seeds {
            match seed(text) {
                Some(Seed::Address(address)) => seeds.push(address),
                Some(Seed::Name(host, port)) => names.push((host, port)),
                None => return Err(Error::BadSeed(text.clone())),
            }
        }

        let limit = self.limit.ok_or(Error::MissingLimit)?;
        if limit == 0 {
            return Err(Error::ZeroLimit);
        }
        let burst = match self.burst {
            Some(burst) if burst > MAX_BURST => return Err(Error::TooLargeBurst(burst)),
            Some(burst) => burst.max(1),
            None => (limit / 20).clamp(1, MAX_BURST),
        };
        let hot_keys = self.hot_keys.unwrap_or(rateguard_proto::MAX_DEMAND_KEYS);
        if hot_keys == 0 || hot_keys > rateguard_proto::MAX_DEMAND_KEYS {
            return Err(Error::TooManyHotKeys(hot_keys));
        }
        let tracked_keys = self.tracked_keys.unwrap_or(4096);
        if tracked_keys < hot_keys {
            return Err(Error::TooFewTrackedKeys);
        }
        if tracked_keys > MAX_TRACKED_KEYS {
            return Err(Error::TooManyTrackedKeys(tracked_keys));
        }
        let period = nanos(self.period.unwrap_or(Duration::from_millis(200)));
        if period / 4 == 0 {
            return Err(Error::ZeroPeriod);
        }

        let core = Config {
            limit_per_sec: limit,
            burst,
            alpha: ALPHA,
            floor_factor: BETA,
            cooldown: 5 * ONE_SEC,
            hot_set_size: hot_keys,
            max_tracked_keys: tracked_keys,
            demand_time_constant: ONE_SEC,
        };
        // The defaults are tuned for 200 ms; whatever must outlast a period
        // is stretched to one if the period is longer.
        let defaults = SwimConfig::default();
        let swim = SwimConfig {
            protocol_period: period,
            ack_timeout: period / 2,
            suspicion_timeout: defaults.suspicion_timeout.max(period),
            reconnect_interval: defaults.reconnect_interval.max(period),
            ..defaults
        };
        Ok(Settings {
            bind,
            advertise,
            seeds,
            names,
            core,
            swim,
            policy: self.policy.unwrap_or_default().to_core(),
        })
    }
}

// The background task's ticker, made here rather than in the task: a
// runtime built without timers would otherwise only show in the task,
// which would die at once and leave a node that started "fine" and never
// runs a round. tokio offers no way to ask, so the attempt is the test.
fn ticker(settings: &Settings) -> Result<tokio::time::Interval, Error> {
    let tick = Duration::from_nanos(settings.swim.protocol_period / 4);
    let ticker =
        std::panic::catch_unwind(|| tokio::time::interval(tick)).map_err(|_| Error::NoTimer)?;
    Ok(ticker)
}

fn start<T: crate::Transport>(
    settings: Settings,
    transport: T,
    ticker: tokio::time::Interval,
) -> crate::Guard {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use rateguard_core::node::Node;
    use rateguard_proto::Address;

    let me = Address::from(settings.advertise);
    let local = crate::key::peer_id(me);
    // Only the order of probes depends on it; any value will do.
    let entropy = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64);
    let mut node = Node::new(
        settings.core,
        settings.swim,
        local,
        me,
        local.get() ^ entropy,
    );
    node.set_partition_policy(settings.policy);
    let mut names = crate::driver::Names::new(local);
    for seed in settings.seeds {
        let address = Address::from(seed);
        let id = crate::key::peer_id(address);
        // One seed list for the whole fleet names the seeds themselves too.
        if id != local {
            node.add_seed(id, address);
            names.keep(id);
        }
    }
    // A name may have nobody behind it yet: the node waits to join all the
    // same, rather than take itself for a cluster of one meanwhile.
    if !settings.names.is_empty() {
        node.expect_peers();
    }

    let shared = Arc::new(crate::guard::Shared {
        table: crate::table::KeyTable::new(settings.core.max_tracked_keys, node.new_key_quota()),
        cluster_size: AtomicUsize::new(node.cluster_size()),
        epoch: tokio::time::Instant::now(),
        running: AtomicBool::new(true),
    });
    let transport = Arc::new(transport);
    if !settings.names.is_empty() {
        let (found, lookups) = tokio::sync::mpsc::channel(1);
        names.listen(lookups);
        tokio::spawn(crate::driver::resolve(
            transport.clone(),
            settings.names,
            Arc::downgrade(&shared),
            Duration::from_nanos(settings.swim.reconnect_interval),
            found,
        ));
    }
    let task = tokio::spawn(crate::driver::run(
        node,
        transport,
        names,
        Arc::downgrade(&shared),
        ticker,
    ));
    tokio::spawn(crate::driver::watch(
        task,
        Arc::downgrade(&shared),
        settings.core,
    ));
    crate::Guard { shared }
}

enum Seed {
    Address(SocketAddr),
    Name(String, u16),
}

// An `ip:port`, or a `host:port` whose host is a DNS name: dot-separated
// labels of letters, digits, `-` and `_`, a trailing dot allowed, the last
// not all digits, as no top-level domain is: `10.0.0.256` is a mistyped
// address, not a name. An IPv6 address goes in brackets, `[::1]:7946`, as
// it does for `bind`.
fn seed(text: &str) -> Option<Seed> {
    if let Ok(address) = text.parse() {
        return Some(Seed::Address(address));
    }
    let (host, port) = text.rsplit_once(':')?;
    let port = port.parse().ok()?;
    let labels = host.strip_suffix('.').unwrap_or(host);
    let is_name = !labels.is_empty()
        && labels.split('.').all(|label| {
            !label.is_empty()
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
        && !labels
            .rsplit('.')
            .next()
            .is_some_and(|last| last.chars().all(|c| c.is_ascii_digit()));
    is_name.then(|| Seed::Name(host.to_owned(), port))
}

/// A builder's settings, checked.
#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub bind: SocketAddr,
    pub advertise: SocketAddr,
    pub seeds: Vec<SocketAddr>,
    /// Seeds given as `host:port`, resolved by the background task.
    pub names: Vec<(String, u16)>,
    pub core: Config,
    pub swim: SwimConfig,
    pub policy: partition::PartitionPolicy,
}

/// Why a node could not start.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// No address to bind: [`Builder::bind`] was not called.
    MissingBind,
    /// `bind` or `advertise` is not an `ip:port`.
    BadAddress(String),
    /// No limit: [`Builder::limit`] was not called.
    MissingLimit,
    /// A limit of 0, which admits nothing.
    ZeroLimit,
    /// A protocol period too short to tick: under 4 ns.
    ZeroPeriod,
    /// Bound to an unspecified IP with no address to advertise.
    MissingAdvertise,
    /// The advertised address is an unspecified IP: nobody could reach it.
    UnspecifiedAdvertise,
    /// A seed is neither an `ip:port` nor a `host:port`.
    BadSeed(String),
    /// More hot keys than the 64 one datagram can report.
    TooManyHotKeys(usize),
    /// A burst over the 16,777,215 a quota holds.
    TooLargeBurst(u32),
    /// Fewer tracked keys than hot keys: a hot key is always tracked.
    TooFewTrackedKeys,
    /// More tracked keys than the 1,048,576 a node keeps.
    TooManyTrackedKeys(usize),
    /// Port 0 advertised, or bound to while advertising another port:
    /// nobody could reach the node.
    AdvertisePortZero,
    /// Not called from within a tokio runtime.
    NoRuntime,
    /// The tokio runtime was built without timers (`enable_time`).
    NoTimer,
    /// The tokio runtime was built without IO (`enable_io`).
    NoIo,
    /// The socket could not be bound.
    Bind(std::io::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MissingBind => write!(f, "no address to bind"),
            Error::BadAddress(text) => write!(f, "not an ip:port: {text}"),
            Error::MissingLimit => write!(f, "no limit"),
            Error::ZeroLimit => write!(f, "a limit of 0 admits nothing"),
            Error::ZeroPeriod => write!(f, "a protocol period too short to tick"),
            Error::MissingAdvertise => write!(
                f,
                "bound to an unspecified IP: set the address to advertise"
            ),
            Error::UnspecifiedAdvertise => write!(
                f,
                "the advertised address is an unspecified IP, nobody could reach it"
            ),
            Error::BadSeed(text) => write!(f, "a seed is not an ip:port or host:port: {text}"),
            Error::TooManyHotKeys(n) => write!(
                f,
                "{n} hot keys, between 1 and {} fit",
                rateguard_proto::MAX_DEMAND_KEYS
            ),
            Error::TooFewTrackedKeys => write!(f, "fewer tracked keys than hot keys"),
            Error::TooManyTrackedKeys(n) => {
                write!(f, "{n} tracked keys, at most {MAX_TRACKED_KEYS}")
            }
            Error::AdvertisePortZero => write!(
                f,
                "port 0 is only known once bound: nobody could reach the advertised address"
            ),
            Error::TooLargeBurst(burst) => {
                write!(f, "a burst of {burst}, at most {MAX_BURST} fit")
            }
            Error::NoRuntime => write!(f, "not within a tokio runtime"),
            Error::NoTimer => write!(f, "the tokio runtime has no timers enabled"),
            Error::NoIo => write!(f, "the tokio runtime has no IO enabled"),
            Error::Bind(error) => write!(f, "cannot bind: {error}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Bind(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Builder {
        Builder::new().bind("10.0.0.1:7946").limit(1000)
    }

    #[test]
    fn defaults_follow_the_spec() {
        let s = valid().settings().unwrap();
        assert_eq!(
            s.advertise,
            "10.0.0.1:7946".parse().unwrap(),
            "the bind address"
        );
        assert_eq!(s.core.burst, 50, "limit / 20");
        assert_eq!(s.core.hot_set_size, 64);
        assert_eq!(s.core.max_tracked_keys, 4096);
        assert_eq!((s.core.alpha, s.core.floor_factor), (0.5, 0.05));
        assert_eq!(s.swim.protocol_period, 200_000_000);
        assert_eq!(s.policy, partition::PartitionPolicy::HoldDown(10 * ONE_SEC));
    }

    #[test]
    fn a_huge_limit_gets_the_largest_burst_a_quota_holds() {
        let s = Builder::new()
            .bind("10.0.0.1:1")
            .limit(u32::MAX)
            .settings()
            .unwrap();
        assert_eq!(s.core.burst, rateguard_core::gcra::MAX_BURST);
    }

    #[test]
    fn durations_too_long_for_nanoseconds_are_held_at_the_longest() {
        let s = valid()
            .partition_policy(PartitionPolicy::HoldDown(Duration::MAX))
            .settings()
            .unwrap();
        assert_eq!(s.policy, partition::PartitionPolicy::HoldDown(u64::MAX));
    }

    #[test]
    fn a_small_limit_still_gets_a_burst_of_one() {
        let s = Builder::new()
            .bind("10.0.0.1:1")
            .limit(7)
            .settings()
            .unwrap();
        assert_eq!(s.core.burst, 1);
    }

    #[test]
    fn a_shorter_period_scales_the_timeouts_that_must_outlast_it() {
        let s = valid()
            .protocol_period(Duration::from_millis(50))
            .settings()
            .unwrap();
        assert_eq!(s.swim.protocol_period, 50_000_000);
        assert_eq!(s.swim.ack_timeout, 25_000_000);
        let s = valid()
            .protocol_period(Duration::from_secs(3))
            .settings()
            .unwrap();
        assert!(s.swim.reconnect_interval >= s.swim.protocol_period);
        assert!(s.swim.suspicion_timeout >= s.swim.protocol_period);
    }

    #[test]
    fn seeds_are_addresses_or_names() {
        let s = valid()
            .seeds([
                "10.0.0.2:7946",
                "[fd00::2]:7946",
                "node-b:7946",
                "rateguard.prod.svc.cluster.local.:7946",
                "_gossip.example.com:1",
            ])
            .settings()
            .unwrap();
        assert_eq!(
            s.seeds,
            [
                "10.0.0.2:7946".parse().unwrap(),
                "[fd00::2]:7946".parse().unwrap()
            ]
        );
        let names: Vec<_> = s.names.iter().map(|(h, p)| (h.as_str(), *p)).collect();
        assert_eq!(
            names,
            [
                ("node-b", 7946),
                ("rateguard.prod.svc.cluster.local.", 7946),
                ("_gossip.example.com", 1),
            ]
        );
    }

    #[test]
    fn a_seed_that_is_no_address_nor_name_is_an_error() {
        for bad in [
            "node-b",
            "node-b:",
            "node-b:port",
            "node-b:65536",
            ":7946",
            "a..b:7946",
            ".:7946",
            "fd00::2:7946",
            "node b:7946",
            "http://node-b:7946",
            "10.0.0.256:7946",
            "10.0.0:7946",
            "7946:7946",
        ] {
            let error = valid().seeds([bad]).settings().unwrap_err();
            assert!(
                matches!(&error, Error::BadSeed(text) if text == bad),
                "{bad}: {error:?}"
            );
        }
    }

    #[test]
    fn every_bad_configuration_is_an_error() {
        use Error::*;
        type Case = (Builder, fn(&Error) -> bool);
        let cases: Vec<Case> = vec![
            (Builder::new().limit(1000), |e| matches!(e, MissingBind)),
            (Builder::new().bind("nowhere").limit(1000), |e| {
                matches!(e, BadAddress(_))
            }),
            (Builder::new().bind("10.0.0.1:1"), |e| {
                matches!(e, MissingLimit)
            }),
            (valid().limit(0), |e| matches!(e, ZeroLimit)),
            (valid().protocol_period(Duration::ZERO), |e| {
                matches!(e, ZeroPeriod)
            }),
            (valid().bind("0.0.0.0:7946"), |e| {
                matches!(e, MissingAdvertise)
            }),
            (
                valid().bind("0.0.0.0:7946").advertise("0.0.0.0:7946"),
                |e| matches!(e, UnspecifiedAdvertise),
            ),
            (valid().seeds(["node-a"]), |e| matches!(e, BadSeed(_))),
            (valid().hot_keys(65), |e| matches!(e, TooManyHotKeys(65))),
            (valid().tracked_keys(10), |e| matches!(e, TooFewTrackedKeys)),
            (valid().burst(u32::MAX), |e| matches!(e, TooLargeBurst(_))),
            (valid().advertise("10.0.0.1:0"), |e| {
                matches!(e, AdvertisePortZero)
            }),
            (
                Builder::new()
                    .bind("10.0.0.1:0")
                    .advertise("10.0.0.1:7946")
                    .limit(10),
                |e| matches!(e, AdvertisePortZero),
            ),
            (valid().tracked_keys(usize::MAX), |e| {
                matches!(e, TooManyTrackedKeys(_))
            }),
        ];
        for (builder, expected) in cases {
            let error = builder.settings().unwrap_err();
            assert!(expected(&error), "{error:?}");
            assert!(!error.to_string().is_empty());
        }
    }
}
