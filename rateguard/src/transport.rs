//! The socket under a node, behind a trait so that tests and simulators can
//! bring their own.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

/// A datagram socket. tokio's UDP socket is one.
///
/// For tests and simulators, through `Builder::spawn_on`; not part of the
/// stable API, and free to change in any release.
#[doc(hidden)]
pub trait Transport: Send + Sync + 'static {
    /// Sends one datagram.
    fn send_to(
        &self,
        bytes: &[u8],
        to: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send;

    /// Receives one datagram. Must be cancel-safe: the node drops a pending
    /// receive at every tick.
    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send;

    /// Looks up the addresses of a seed given as `host:port`; the system's
    /// resolver, through tokio, unless the transport brings its own.
    fn resolve(
        &self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = io::Result<Vec<SocketAddr>>> + Send {
        let target = (host.to_owned(), port);
        async move { Ok(tokio::net::lookup_host(target).await?.collect()) }
    }
}

impl Transport for tokio::net::UdpSocket {
    fn send_to(
        &self,
        bytes: &[u8],
        to: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send {
        tokio::net::UdpSocket::send_to(self, bytes, to)
    }

    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send {
        tokio::net::UdpSocket::recv_from(self, buf)
    }
}

#[cfg(feature = "turmoil")]
impl Transport for turmoil::net::UdpSocket {
    fn send_to(
        &self,
        bytes: &[u8],
        to: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send {
        turmoil::net::UdpSocket::send_to(self, bytes, to)
    }

    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send {
        turmoil::net::UdpSocket::recv_from(self, buf)
    }

    fn resolve(
        &self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = io::Result<Vec<SocketAddr>>> + Send {
        let found = SocketAddr::new(turmoil::lookup(host), port);
        async move { Ok(vec![found]) }
    }
}
