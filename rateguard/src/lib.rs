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
//! The gossip is neither encrypted nor authenticated: run the nodes on a
//! network you trust.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod config;
mod driver;
mod guard;
mod key;
mod sync;
mod table;
mod transport;

pub use config::{Builder, Error, PartitionPolicy};
pub use guard::{Decision, Guard};
pub use key::key_hash;
pub use transport::Transport;
