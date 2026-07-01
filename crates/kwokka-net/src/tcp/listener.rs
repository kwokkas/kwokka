//! Listening TCP socket -- the synchronous cold-path endpoint.
//!
//! Binding allocates the socket, names it, and opens the backlog in one
//! shot; none of those steps waits on a peer, so they stay synchronous.
//! Waiting happens in the accept op, which the runtime drives through its
//! completion backend against this listener's raw fd.
//!
//! The fd stays in blocking mode as created. That is correct for the
//! `io_uring` accept op, which completes in the kernel regardless of the
//! mode; the readiness-based epoll / kqueue fallback requires the fd
//! switched to non-blocking before submission, where a blocking
//! `accept(2)` after a readiness event would stall the worker. The
//! switch lands with the fallback driver, which owns that contract.

use std::{
    io,
    net::{self, SocketAddr, ToSocketAddrs},
    os::fd::{AsRawFd, RawFd},
};

use kwokka_io::boundary;

use crate::tcp::{AcceptFuture, AcceptStream, TcpStream};

/// A TCP socket listening for inbound connections.
///
/// Owns the bound socket for its lifetime; dropping the listener closes
/// the fd and the backlog with it. The asynchronous accept arrives with
/// the stream type; until then [`AsRawFd`] feeds the accept op directly.
pub struct TcpListener {
    /// The bound socket, owned through the std listener.
    inner: net::TcpListener,
}

impl TcpListener {
    /// Binds a TCP listener to `addr` and opens its backlog.
    ///
    /// Resolution may yield several addresses; the first that binds wins,
    /// matching the std contract.
    ///
    /// # Errors
    ///
    /// Returns the OS error when no resolved address can be bound -- the
    /// port is taken, the address is not local, or resolution itself
    /// failed.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let inner = net::TcpListener::bind(addr)?;
        Ok(Self { inner })
    }

    /// Returns the local address the listener is bound to.
    ///
    /// The OS-assigned port shows here after binding port 0.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the socket name cannot be read -- the fd
    /// was invalidated outside this type's control.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Accepts one inbound connection, resolving to the connected stream.
    ///
    /// Drives the accept op through the runtime's completion backend; a
    /// connection arriving through the backlog resolves the future. The
    /// accepted socket lands owned -- dropping the stream closes it.
    ///
    /// # Errors
    ///
    /// Returns the OS error the kernel reported for the accept op.
    ///
    /// # Panics
    ///
    /// Panics when awaited outside a runtime task or through a combinator
    /// that wraps the waker, per the accept future's contract.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        let result = AcceptFuture::new(self.as_raw_fd()).await;
        let Some(fd) = boundary::adopt_accepted_fd(result) else {
            return Err(io::Error::from_raw_os_error(-result));
        };
        Ok(TcpStream::from(fd))
    }

    /// Accepts connections as a stream driven by one multishot accept.
    ///
    /// See [`AcceptStream`]: one submitted op yields a completion per incoming
    /// connection on a capable kernel, degrading to single-shot accepts
    /// otherwise. The returned connections are owned by the caller.
    pub fn accept_multi(&self) -> AcceptStream<'_> {
        AcceptStream::new(self.as_raw_fd())
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl From<net::TcpListener> for TcpListener {
    /// Adopts an already-bound std listener, taking ownership of its fd.
    fn from(inner: net::TcpListener) -> Self {
        Self { inner }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_assigns_a_loopback_port() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        let Ok(addr) = listener.local_addr() else {
            panic!("a bound listener must report its local address");
        };
        assert!(addr.ip().is_loopback());
        assert_ne!(addr.port(), 0, "port 0 resolves to an OS-assigned port");
    }

    #[test]
    fn the_raw_fd_is_a_live_descriptor() {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        assert!(listener.as_raw_fd() >= 0);
    }

    #[test]
    fn bind_surfaces_the_os_error_for_a_taken_port() {
        let Ok(first) = TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a loopback listener must succeed");
        };
        let Ok(addr) = first.local_addr() else {
            panic!("a bound listener must report its local address");
        };
        let Err(error) = TcpListener::bind(addr) else {
            panic!("rebinding a taken port must fail");
        };
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn adopting_a_std_listener_keeps_its_address() {
        let Ok(std_listener) = net::TcpListener::bind("127.0.0.1:0") else {
            panic!("binding a std loopback listener must succeed");
        };
        let Ok(expected) = std_listener.local_addr() else {
            panic!("a bound listener must report its local address");
        };
        let adopted = TcpListener::from(std_listener);
        let Ok(addr) = adopted.local_addr() else {
            panic!("the adopted listener must report its local address");
        };
        assert_eq!(addr, expected);
    }
}
