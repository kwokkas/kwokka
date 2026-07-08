//! Listening Unix-domain socket -- the synchronous cold-path endpoint.
//!
//! Binding allocates the socket, names it at a filesystem path, and opens the
//! backlog in one shot; none of those steps waits on a peer, so they stay
//! synchronous. Waiting happens in the accept op, which the runtime drives
//! through its completion backend against this listener's raw fd -- the same
//! `io_uring` accept the TCP listener drives, reused over the Unix fd.
//!
//! The fd stays in blocking mode as created, matching the TCP listener's
//! blocking-mode contract: the `io_uring` accept completes in the kernel
//! regardless of the mode, and the readiness-based epoll / kqueue fallback
//! owns the switch to non-blocking when it lands.

use std::{
    io,
    os::{
        fd::{AsRawFd, RawFd},
        unix::net::{self, SocketAddr},
    },
    path::Path,
};

use kwokka_io::boundary;

use crate::{tcp::AcceptFuture, unix::UnixStream};

/// A Unix-domain socket listening for inbound connections.
///
/// Owns the bound socket for its lifetime; dropping the listener closes the fd
/// and the backlog with it. The bound path is not unlinked on drop, matching
/// the std listener. The asynchronous accept arrives with the stream type;
/// until then [`AsRawFd`] feeds the accept op directly.
pub struct UnixListener {
    /// The bound socket, owned through the std listener.
    inner: net::UnixListener,
}

impl UnixListener {
    /// Binds a Unix-domain listener at `path` and opens its backlog.
    ///
    /// Naming the socket creates the path as a filesystem entry, so a stale
    /// path from an earlier bind must be removed before rebinding.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the socket cannot be bound -- the path exists,
    /// its directory is not writable, or the name exceeds the platform limit.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: binds a live socket path the doctest host may lack.
    /// use kwokka_net::unix::UnixListener;
    ///
    /// let listener = UnixListener::bind("/tmp/kwokka.sock")?;
    /// let _local = listener.local_addr()?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let inner = net::UnixListener::bind(path)?;
        Ok(Self { inner })
    }

    /// Returns the local address the listener is bound to.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the socket name cannot be read -- the fd was
    /// invalidated outside this type's control.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Accepts one inbound connection, resolving to the connected stream.
    ///
    /// Drives the accept op through the runtime's completion backend; a
    /// connection arriving through the backlog resolves the future. The accepted
    /// socket lands owned -- dropping the stream closes it.
    ///
    /// # Errors
    ///
    /// Returns the OS error the kernel reported for the accept op.
    ///
    /// # Panics
    ///
    /// Panics when awaited outside a runtime task or through a combinator that
    /// wraps the waker, per the accept future's contract.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # // no_run: needs a connecting peer and io_uring at runtime.
    /// use kwokka_net::unix::UnixListener;
    /// use kwokka_runtime::Runtime;
    ///
    /// let mut runtime = Runtime::affine()?;
    /// let listener = UnixListener::bind("/tmp/kwokka.sock")?;
    /// let _stream = runtime.block_on(listener.accept())?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub async fn accept(&self) -> io::Result<UnixStream> {
        let result = AcceptFuture::new(self.as_raw_fd()).await;
        let Some(fd) = boundary::adopt_accepted_fd(result) else {
            return Err(io::Error::from_raw_os_error(-result));
        };
        Ok(UnixStream::from(fd))
    }
}

impl AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl From<net::UnixListener> for UnixListener {
    /// Adopts an already-bound std listener, taking ownership of its fd.
    fn from(inner: net::UnixListener) -> Self {
        Self { inner }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use std::path::PathBuf;

    use super::*;

    // Unlinks the socket path on drop so a test leaves no filesystem residue;
    // the guard owns the path, and tests borrow it so no clone is needed.
    struct PathGuard(PathBuf);

    impl Drop for PathGuard {
        fn drop(&mut self) {
            // IGNORE: best-effort cleanup; the path may already be gone.
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // A per-process, per-test socket path under the temp dir, cleared of any
    // socket a crashed prior run left behind so the bind starts clean, and
    // owned by a guard that unlinks it on drop.
    fn unique_path(suffix: &str) -> PathGuard {
        let mut path = std::env::temp_dir();
        path.push(format!("kwokka-unix-{}-{suffix}.sock", std::process::id()));
        // IGNORE: best-effort clear of a stale socket; absence is the goal.
        let _ = std::fs::remove_file(&path);
        PathGuard(path)
    }

    #[test]
    fn bind_reports_its_local_path() {
        let guard = unique_path("bind");
        let path = guard.0.as_path();
        let Ok(listener) = UnixListener::bind(path) else {
            panic!("binding a unix listener must succeed");
        };
        let Ok(addr) = listener.local_addr() else {
            panic!("a bound listener must report its local address");
        };
        assert_eq!(addr.as_pathname(), Some(path));
    }

    #[test]
    fn the_raw_fd_is_a_live_descriptor() {
        let guard = unique_path("fd");
        let Ok(listener) = UnixListener::bind(guard.0.as_path()) else {
            panic!("binding a unix listener must succeed");
        };
        assert!(listener.as_raw_fd() >= 0);
    }

    #[test]
    fn bind_fails_on_a_taken_path() {
        let guard = unique_path("taken");
        let path = guard.0.as_path();
        let Ok(_first) = UnixListener::bind(path) else {
            panic!("binding a unix listener must succeed");
        };
        let Err(error) = UnixListener::bind(path) else {
            panic!("rebinding a taken path must fail");
        };
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn adopting_a_std_listener_keeps_its_path() {
        let guard = unique_path("adopt");
        let path = guard.0.as_path();
        let Ok(std_listener) = net::UnixListener::bind(path) else {
            panic!("binding a std unix listener must succeed");
        };
        let adopted = UnixListener::from(std_listener);
        let Ok(addr) = adopted.local_addr() else {
            panic!("the adopted listener must report its local address");
        };
        assert_eq!(addr.as_pathname(), Some(path));
    }
}
