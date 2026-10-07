//! The socket under a node, behind a trait so that tests and simulators can
//! bring their own.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

/// A datagram socket. tokio's UDP socket is one.
pub trait Transport: Send + Sync + 'static {
    fn send_to(
        &self,
        bytes: &[u8],
        to: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send;

    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send;
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
}
