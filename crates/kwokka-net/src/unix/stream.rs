//! Connected Unix-domain stream socket -- the owned endpoint an accept or
//! connect lands.
//!
//! Connecting is a synchronous cold-path call: a Unix-domain connect resolves
//! against a local path with no network round trip, so it names the peer in one
//! syscall rather than driving a completion op. The byte transfers wait on the
//! kernel and arrive as the stream send / recv futures, driven through the
//! runtime's completion backend against this socket's raw fd.

use core::future::Future;
use std::{
    io,
    os::{
        fd::{AsRawFd, OwnedFd, RawFd},
        unix::net::{self, SocketAddr},
    },
    path::Path,
};

use kwokka_io::operation::{IoBuf, IoBufMut, RecvFuture, SendFuture};

/// A connected Unix-domain stream socket.
///
/// Owns the socket for its lifetime; dropping the stream closes the fd and the
/// connection with it. Address inspection delegates to the std socket;
/// [`recv_buf`](Self::recv_buf) and [`send_buf`](Self::send_buf) hand out the
/// completion futures conversing over it.
pub struct UnixStream {
    /// The connected socket, owned through the std stream.
    inner: net::UnixStream,
}

impl UnixStream {
    /// Connects to the Unix-domain socket bound at `path`.
    ///
    /// A Unix-domain connect resolves locally, so it stays synchronous like
    /// [`UnixListener::bind`](crate::unix::UnixListener::bind) rather than
    /// driving a completion op. The connected socket lands owned, so dropping
    /// the stream closes it.
    ///
    /// # Errors
    ///
    /// Returns the OS error the connect reports -- no socket is bound at `path`,
    /// the path is not a socket, or the peer refused the connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: connects to a live socket the doctest host may lack.
    /// use kwokka_net::unix::UnixStream;
    ///
    /// let stream = UnixStream::connect("/tmp/kwokka.sock")?;
    /// let _peer = stream.peer_addr()?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn connect<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let inner = net::UnixStream::connect(path)?;
        Ok(Self { inner })
    }

    /// Returns the local address this end of the connection is bound to.
    ///
    /// An unnamed client end reports an unnamed address; an accepted end
    /// reports the listener's path.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the socket name cannot be read -- the fd was
    /// invalidated outside this type's control.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Returns the peer address this connection is established to.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the peer name cannot be read -- the connection
    /// reset, or the fd was invalidated outside this type's control.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Hands out the future receiving into the caller-owned buffer `buf`.
    ///
    /// The caller supplies any [`IoBufMut`] buffer (today an `[u8; N]` array)
    /// and awaiting the future resolves to an [`io::Result`] byte count paired
    /// with that buffer handed back -- the bytes received (a short count on a
    /// partial read, or `0` at end of stream), or the mapped error. The bytes
    /// live in a worker-owned registry for the op lifetime, so dropping the
    /// future mid-flight is safe. Await it directly on a runtime task: polling it
    /// through a waker the runtime did not build panics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer and io_uring at runtime.
    /// use kwokka_net::unix::UnixListener;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = UnixListener::bind("/tmp/kwokka.sock")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let (result, _buf) = runtime.block_on(stream.recv_buf([0u8; 64]));
    /// let _read = result?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn recv_buf<B: IoBufMut>(
        &self,
        buf: B,
    ) -> impl Future<Output = (io::Result<usize>, B)> + use<B> {
        RecvFuture::new(self.inner.as_raw_fd(), buf)
    }

    /// Hands out the future sending the initialized bytes of the caller-owned
    /// buffer `buf`.
    ///
    /// The caller supplies any [`IoBuf`] source -- an `[u8; N]` array (all `N`
    /// bytes) or a [`FixedBuf`](crate::tcp::FixedBuf) carrying a partial length.
    /// Awaiting it resolves to an [`io::Result`] byte count (a short count when
    /// the socket send buffer fills); `buf` is a pre-submit source the future
    /// drops on resolve, not handed back. The kernel reads a worker-owned copy
    /// of the bytes, so dropping the future mid-flight is safe. Await it directly
    /// on a runtime task: polling it through a waker the runtime did not build
    /// panics.
    ///
    /// # Errors
    ///
    /// Resolves to the [`io::Error`] the kernel maps the send to (for example a
    /// reset or closed connection), or an `-EINVAL` error when the buffer's
    /// initialized length exceeds the in-flight slot stride the worker copies
    /// the bytes through.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connected peer and io_uring at runtime.
    /// use kwokka_net::unix::UnixListener;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = UnixListener::bind("/tmp/kwokka.sock")?;
    /// let stream = runtime.block_on(listener.accept())?;
    /// let _sent = runtime.block_on(stream.send_buf(*b"hello!"))?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn send_buf<B: IoBuf>(&self, buf: B) -> impl Future<Output = io::Result<usize>> + use<B> {
        SendFuture::new(self.inner.as_raw_fd(), buf)
    }
}

impl AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl From<OwnedFd> for UnixStream {
    /// Adopts an owned connected-socket descriptor as a stream.
    fn from(fd: OwnedFd) -> Self {
        Self {
            inner: net::UnixStream::from(fd),
        }
    }
}

impl From<net::UnixStream> for UnixStream {
    /// Adopts an already-connected std stream, taking ownership of its fd.
    fn from(inner: net::UnixStream) -> Self {
        Self { inner }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    #[test]
    fn connect_fails_without_a_listener() {
        let mut path = std::env::temp_dir();
        path.push(format!("kwokka-unix-{}-absent.sock", std::process::id()));
        // IGNORE: best-effort clear; the path must be absent for this test.
        let _ = std::fs::remove_file(&path);
        let Err(error) = UnixStream::connect(&path) else {
            panic!("connecting to an unbound path must fail");
        };
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ),
            "an absent socket path surfaces a not-found or refused error, got {error:?}",
        );
    }
}
