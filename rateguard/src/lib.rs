//! Distributed rate limiting for Rust services: one global limit across a
//! fleet, no datastore, no network call on the request path.
#![forbid(unsafe_code)]

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
