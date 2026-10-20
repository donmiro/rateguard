//! Distributed rate limiting for Rust services: one global limit across a
//! fleet, no datastore, no network call on the request path.
//!
//! Every instance of the service runs a [`Guard`]. The nodes find each other
//! over UDP and agree, every 200 ms, on how the limit is shared out; each
//! request is then decided locally, with no I/O and no lock shared with the
//! network.
//!
//! ```no_run
//! use rateguard::{Decision, Guard};
//!
//! # async fn run() -> Result<(), rateguard::Error> {
//! let guard = Guard::builder()
//!     .bind("0.0.0.0:7946")
//!     .advertise("10.0.0.3:7946")
//!     .seeds(["10.0.0.1:7946", "10.0.0.2:7946"])
//!     .limit(1_000) // per key, across the whole fleet, per second
//!     .spawn()?;
//!
//! match guard.check("api:tenant-42") {
//!     Decision::Allow => { /* serve */ }
//!     Decision::Deny { retry_after } => { /* 429, Retry-After */ }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The gossip is not encrypted, and by default not authenticated either:
//! run the nodes on a network you trust, or have them sign it with a
//! shared secret, see [`Builder::gossip_auth`].
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod classes;
mod config;
mod driver;
mod guard;
mod key;
mod limit;
#[cfg(feature = "metrics")]
mod metrics;
mod stats;
mod sync;
mod table;
#[cfg(feature = "tower")]
pub mod tower;
mod transport;

pub use config::{Builder, Error, PartitionPolicy};
pub use guard::{Decision, Guard};
pub use key::{key_hash, key_hash_in};
pub use limit::Limit;
pub use rateguard_proto::auth::{AuthError, GossipAuth, Secret};
pub use stats::{DeniedKey, RejectedDatagrams, Stats};
pub use transport::Transport;
