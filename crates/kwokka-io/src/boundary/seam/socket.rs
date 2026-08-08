//! Creating and adopting the file descriptors the seam hands out.

use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use crate::addr::AddressFamily;

#[cfg(not(target_os = "macos"))]
const SOCKET_CLOEXEC: libc::c_int = libc::SOCK_CLOEXEC;
#[cfg(target_os = "macos")]
const SOCKET_CLOEXEC: libc::c_int = 0;

// `recv.2:105-125` / `send.2:167-187` define this Linux value. Darwin's
// `socket.h:624` defines its distinct value and gives it the same per-call
// nonblocking meaning, so neither platform needs to alter the caller's fd.
#[cfg(target_os = "linux")]
const MSG_DONTWAIT: libc::c_int = 0x40;
#[cfg(target_os = "macos")]
const MSG_DONTWAIT: libc::c_int = 0x80;

// `send.2:211-227` / `send.2:434-440` require this Linux flag to turn a
// closed peer into `EPIPE` rather than a process-wide SIGPIPE. Darwin's
// `socket.h:640` defines the corresponding per-call flag.
#[cfg(target_os = "linux")]
const MSG_NOSIGNAL: libc::c_int = 0x4000;
#[cfg(target_os = "macos")]
const MSG_NOSIGNAL: libc::c_int = 0x80000;

/// A descriptor acquired for exactly one readiness attempt.
///
/// The private constructor makes the duplication the first and only descriptor
/// lookup before the attempt syscall.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) struct Duplicated(OwnedFd);

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Duplicated {
    /// Releases the owned descriptor to the deferral record.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "consumed by the readiness retry path in #327")
    )]
    pub(crate) fn into_owned(self) -> OwnedFd {
        self.0
    }
}

/// The result of one nonblocking readiness attempt.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "consumed by the readiness retry path in #327")
)]
pub(crate) enum Attempt {
    /// The syscall transferred this many bytes; its duplicate was closed.
    Done(u32),
    /// The syscall failed with this negative errno; its duplicate was closed.
    Failed(i32),
    /// The operation must wait; the duplicate owns the retry descriptor.
    WouldBlock(OwnedFd),
}

/// Duplicates `fd` with close-on-exec set by the same kernel operation.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "consumed by the readiness retry path in #327")
)]
pub(crate) fn duplicate_fd(fd: i32) -> io::Result<Duplicated> {
    // SAFETY: Invariant -- `F_DUPFD_CLOEXEC` atomically creates a distinct
    // descriptor for the open file description named by `fd`
    // (`F_DUPFD.2const:38-46`; `fcntl.2:108-112` on Darwin). Precondition:
    // `fd` names a live descriptor at this single lookup. Failure mode: an
    // invalid or exhausted descriptor table returns -1 and `errno`, which is
    // returned without adopting an integer that is not ours.
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: Invariant -- the successful `F_DUPFD_CLOEXEC` call above created
    // one descriptor now owned by this process. Precondition: `duplicated` is
    // nonnegative, checked above. Failure mode: adopting an integer not newly
    // returned by `fcntl` would close another owner's descriptor on drop.
    Ok(Duplicated(unsafe { OwnedFd::from_raw_fd(duplicated) }))
}

/// Attempts a receive through an already-owned duplicate without blocking.
///
/// # Safety
///
/// Invariant: `ptr` addresses the live in-flight slot for this operation.
/// Precondition: it is valid and exclusively available for `cap` writes until
/// this call returns. Failure mode: an invalid or aliased region lets the
/// kernel write outside the slot, which is undefined behavior.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "consumed by the readiness retry path in #327")
)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the seam retry boundary reports the same io::Result shape as duplication"
)]
pub(crate) unsafe fn attempt_recv(
    duplicated: Duplicated,
    ptr: *mut u8,
    cap: usize,
) -> io::Result<Attempt> {
    // SAFETY: Invariant -- `duplicated` owns a live socket descriptor and the
    // caller's unsafe contract makes `ptr` writable for `cap` bytes. Precondition:
    // the pointer remains valid and exclusive for this synchronous syscall.
    // Failure mode: an invalid pointer lets `recv` write invalid memory; an
    // invalid descriptor returns -1 and is represented below.
    let result = unsafe { libc::recv(duplicated.0.as_raw_fd(), ptr.cast(), cap, MSG_DONTWAIT) };
    Ok(attempt_result(duplicated, result))
}

/// Attempts a send through an already-owned duplicate without blocking.
///
/// # Safety
///
/// Invariant: `ptr` addresses the live in-flight slot for this operation.
/// Precondition: it is valid for `len` reads until this call returns. Failure
/// mode: an invalid region lets the kernel read outside the slot, which is
/// undefined behavior.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "consumed by the readiness retry path in #327")
)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the seam retry boundary reports the same io::Result shape as duplication"
)]
pub(crate) unsafe fn attempt_send(
    duplicated: Duplicated,
    ptr: *const u8,
    len: usize,
) -> io::Result<Attempt> {
    // SAFETY: Invariant -- `duplicated` owns a live socket descriptor and the
    // caller's unsafe contract makes `ptr` readable for `len` bytes. Precondition:
    // the pointer remains valid for this synchronous syscall. Failure mode: an
    // invalid pointer lets `send` read invalid memory; an invalid descriptor
    // returns -1 and is represented below.
    let result = unsafe {
        libc::send(
            duplicated.0.as_raw_fd(),
            ptr.cast(),
            len,
            MSG_DONTWAIT | MSG_NOSIGNAL,
        )
    };
    Ok(attempt_result(duplicated, result))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn attempt_result(duplicated: Duplicated, result: isize) -> Attempt {
    if result >= 0 {
        return u32::try_from(result)
            .map_or_else(|_| Attempt::Failed(-libc::EINVAL), Attempt::Done);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EAGAIN) {
        return Attempt::WouldBlock(duplicated.0);
    }
    Attempt::Failed(-error.raw_os_error().unwrap_or(libc::EIO))
}

/// Adopts a nonnegative accept-completion result as an owned descriptor.
///
/// Returns `None` for a negative result -- an `-errno`, not a descriptor.
///
/// Call this only on the result of an accept-class completion. A
/// nonnegative accept result names a descriptor the kernel just created
/// for this process, with no other owner. Adopting any other integer
/// asserts ownership of a descriptor this process may not own, and the
/// returned handle closes it on drop -- an IO-ownership violation
/// (incorrect close), not a memory-corruption concern.
pub fn adopt_accepted_fd(result: i32) -> Option<OwnedFd> {
    if result < 0 {
        return None;
    }
    // SAFETY: Invariant -- a nonnegative accept-class CQE result is a
    // freshly created descriptor the kernel handed to this process, with
    // exactly one owner: the adopter. Precondition: the caller passes an
    // accept-completion result per the documented contract above; the sign
    // check excludes errno results. Failure mode: adopting a value that is
    // not an accept result claims a descriptor owned elsewhere -- it closes
    // on drop and use-after-close races follow. This is an IO-ownership
    // concern (incorrect close), not a memory-corruption concern: no pointer
    // dereference occurs.
    Some(unsafe { OwnedFd::from_raw_fd(result) })
}

/// Creates an unconnected, close-on-exec socket of `socket_type` for `family`.
///
/// Shared by the stream and datagram constructors: a client-side op (connect,
/// sendmsg) needs an owned socket of the peer's address family before the
/// `io_uring` op runs, and the standard library exposes no such constructor.
/// The descriptor is left blocking; the op is submitted as an `io_uring`
/// completion rather than a blocking syscall on this fd.
///
/// # Errors
///
/// Returns the OS error when the `socket` syscall or the macOS `fcntl` call
/// fails, or
/// [`io::ErrorKind::Unsupported`] for `AddressFamily::Unix` (only IPv4 and IPv6
/// are supported here).
fn create_socket(family: AddressFamily, socket_type: i32) -> io::Result<OwnedFd> {
    let domain = match family {
        AddressFamily::Inet => libc::AF_INET,
        AddressFamily::Inet6 => libc::AF_INET6,
        AddressFamily::Unix => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "only IPv4 and IPv6 sockets are supported",
            ));
        }
    };
    // SAFETY: Invariant -- `libc::socket` (socket.2) is an FFI call that takes
    // three integers and returns a fresh descriptor or -1; it has no pointer or
    // memory precondition. Precondition: `domain` is a valid `AF_*` constant
    // (matched above) and `socket_type | SOCKET_CLOEXEC` is a valid type per
    // socket.2. Failure mode: an unsupported argument yields -1 plus `errno`,
    // handled just below; the call itself cannot corrupt memory.
    let raw = unsafe { libc::socket(domain, socket_type | SOCKET_CLOEXEC, 0) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: Invariant -- `socket` just returned a fresh descriptor owned by
    // this process alone, exactly like an accept result. Precondition: `raw` is
    // nonnegative (checked above), so it names a real descriptor with no other
    // owner. Failure mode: adopting a negative value would claim a descriptor
    // owned elsewhere and close it on drop; the sign check excludes that. No
    // pointer dereference occurs (IO-ownership, not memory corruption).
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    #[cfg(target_os = "macos")]
    {
        // SAFETY: Invariant -- `socket` owns the fresh descriptor returned by
        // `socket`, so it remains valid for this `fcntl` call. Precondition:
        // `socket` has not been moved or dropped before its descriptor is read.
        // Failure mode: `fcntl` returns -1 for an invalid descriptor
        // (`fcntl.2:115-133`), and this separate call leaves a fork/exec race
        // until it succeeds (`open.2:198-215`), which can expose the descriptor
        // to an exec'd child.
        let result = unsafe {
            libc::fcntl(
                std::os::fd::AsRawFd::as_raw_fd(&socket),
                libc::F_SETFD,
                libc::FD_CLOEXEC,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(socket)
}

/// Creates an unconnected, close-on-exec stream socket for `family`.
///
/// The client counterpart of adopting an accepted descriptor: a connect needs
/// an owned socket of the peer's address family before the `io_uring` connect
/// op runs. The shared syscall path lives in `create_socket`.
///
/// # Errors
///
/// Returns the OS error when the `socket` syscall or the macOS `fcntl` call
/// fails, or
/// [`io::ErrorKind::Unsupported`] for `AddressFamily::Unix` (only IPv4 and IPv6
/// stream sockets are created).
pub fn create_stream_socket(family: AddressFamily) -> io::Result<OwnedFd> {
    create_socket(family, libc::SOCK_STREAM)
}

/// Creates an unconnected, close-on-exec datagram socket for `family`.
///
/// The UDP counterpart of [`create_stream_socket`]: a `sendmsg` / `recvmsg`
/// needs an owned datagram socket of the peer's address family before the
/// `io_uring` op runs. The shared syscall path lives in `create_socket`.
///
/// # Errors
///
/// Returns the OS error when the `socket` syscall or the macOS `fcntl` call
/// fails, or
/// [`io::ErrorKind::Unsupported`] for `AddressFamily::Unix` (only IPv4 and IPv6
/// datagram sockets are created).
pub fn create_datagram_socket(family: AddressFamily) -> io::Result<OwnedFd> {
    create_socket(family, libc::SOCK_DGRAM)
}

#[cfg(test)]
mod tests {
    #[cfg(all(unix, not(miri)))]
    use std::{
        fs::File,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
    };

    use super::*;

    #[cfg(all(unix, not(miri)))]
    fn loopback_pair() -> io::Result<(TcpStream, TcpStream)> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;
        let client = TcpStream::connect(address)?;
        let (server, _) = listener.accept()?;
        Ok((server, client))
    }

    #[cfg(all(unix, not(miri)))]
    fn recv_attempt(stream: &TcpStream, bytes: &mut [u8]) -> io::Result<Attempt> {
        let duplicated = duplicate_fd(stream.as_raw_fd())?;
        // SAFETY: Invariant -- `bytes` remains exclusively borrowed across this
        // synchronous kernel call. Precondition: its pointer is valid for its
        // full length. Failure mode: an invalid or aliased region lets `recv`
        // write outside the test buffer.
        unsafe { attempt_recv(duplicated, bytes.as_mut_ptr(), bytes.len()) }
    }

    #[cfg(all(unix, not(miri)))]
    fn send_attempt(stream: &TcpStream, bytes: &[u8]) -> io::Result<Attempt> {
        let duplicated = duplicate_fd(stream.as_raw_fd())?;
        // SAFETY: Invariant -- `bytes` remains borrowed across this synchronous
        // kernel call. Precondition: its pointer is valid for its full length.
        // Failure mode: an invalid region lets `send` read outside the test
        // buffer.
        unsafe { attempt_send(duplicated, bytes.as_ptr(), bytes.len()) }
    }

    #[cfg(all(unix, not(miri)))]
    fn shrink_socket_buffers(stream: &TcpStream) -> io::Result<()> {
        let size: libc::c_int = 1;
        let Ok(size_len) = libc::socklen_t::try_from(core::mem::size_of_val(&size)) else {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        };
        // SAFETY: Invariant -- `stream` owns a live TCP descriptor and `size`
        // points at one initialized `c_int`. Precondition: the pointer and its
        // byte count remain valid for each synchronous `setsockopt` call.
        // Failure mode: an invalid descriptor or option returns -1 and errno;
        // an invalid pointer would let the kernel read invalid memory.
        let send = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&raw const size).cast(),
                size_len,
            )
        };
        if send < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: Invariant -- identical to the preceding `setsockopt` call;
        // this call changes only the receive-buffer bound on the same live
        // descriptor. Precondition: `size` remains initialized for `size_len`
        // bytes. Failure mode: an invalid descriptor or pointer returns errno
        // or lets the kernel read invalid memory, respectively.
        let recv = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&raw const size).cast(),
                size_len,
            )
        };
        if recv < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    // A real `socket()` syscall is unsupported under miri's isolation, so this
    // runs off-miri; the Unix-rejection test below returns before any syscall
    // and stays miri-safe.
    #[cfg(all(unix, not(miri)))]
    #[test]
    fn create_stream_socket_makes_an_ipv6_socket() {
        let Ok(_socket) = create_stream_socket(crate::addr::AddressFamily::Inet6) else {
            panic!("an IPv6 stream socket must be created");
        };
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn create_stream_socket_sets_close_on_exec() {
        let Ok(socket) = create_stream_socket(crate::addr::AddressFamily::Inet6) else {
            panic!("an IPv6 stream socket must be created");
        };
        // SAFETY: Invariant -- `socket` owns a live descriptor whose flags can
        // be read without transferring ownership. Precondition: `socket` has
        // not been moved or dropped before its descriptor is read. Failure mode:
        // an invalid descriptor makes `fcntl` return -1 (`fcntl.2:115-133`),
        // which the assertion below rejects before inspecting the returned bits.
        let flags = unsafe { libc::fcntl(std::os::fd::AsRawFd::as_raw_fd(&socket), libc::F_GETFD) };
        assert!(flags >= 0, "reading descriptor flags must succeed");
        assert_ne!(flags & libc::FD_CLOEXEC, 0, "the socket is close-on-exec");
    }

    #[test]
    fn create_stream_socket_rejects_unix() {
        let Err(error) = create_stream_socket(crate::addr::AddressFamily::Unix) else {
            panic!("a Unix family is rejected for a TCP stream socket");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    }

    // A real `socket()` syscall is unsupported under miri's isolation, so this
    // runs off-miri; the Unix-rejection test below returns before any syscall.
    #[cfg(all(unix, not(miri)))]
    #[test]
    fn create_datagram_socket_makes_an_ipv4_socket() {
        let Ok(_socket) = create_datagram_socket(crate::addr::AddressFamily::Inet) else {
            panic!("an IPv4 datagram socket must be created");
        };
    }

    #[test]
    fn create_datagram_socket_rejects_unix() {
        let Err(error) = create_datagram_socket(crate::addr::AddressFamily::Unix) else {
            panic!("a Unix family is rejected for a UDP datagram socket");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn recv_attempt_on_unready_blocking_socket_would_block() {
        let Ok((server, _client)) = loopback_pair() else {
            panic!("a loopback pair must be created");
        };
        let mut bytes = [0u8; 8];
        let Ok(result) = recv_attempt(&server, &mut bytes) else {
            panic!("the receive attempt must return an outcome");
        };
        assert!(matches!(result, Attempt::WouldBlock(_)));
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn recv_attempt_reads_ready_socket_bytes() {
        let Ok((server, mut client)) = loopback_pair() else {
            panic!("a loopback pair must be created");
        };
        let payload = b"recv";
        let Ok(()) = client.write_all(payload) else {
            panic!("the client must seed the receive buffer");
        };
        let mut bytes = [0u8; 8];
        let Ok(result) = recv_attempt(&server, &mut bytes) else {
            panic!("the receive attempt must return an outcome");
        };
        assert!(matches!(result, Attempt::Done(4)));
        assert_eq!(&bytes[..payload.len()], payload);
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn send_attempt_writes_ready_socket_bytes() {
        let Ok((mut server, client)) = loopback_pair() else {
            panic!("a loopback pair must be created");
        };
        let payload = b"send";
        let Ok(result) = send_attempt(&client, payload) else {
            panic!("the send attempt must return an outcome");
        };
        assert!(matches!(result, Attempt::Done(4)));
        let mut received = [0u8; 4];
        let Ok(()) = server.read_exact(&mut received) else {
            panic!("the peer must receive the sent bytes");
        };
        assert_eq!(received, *payload);
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn send_attempt_on_full_socket_would_block() {
        let Ok((server, client)) = loopback_pair() else {
            panic!("a loopback pair must be created");
        };
        let Ok(()) = shrink_socket_buffers(&server) else {
            panic!("the server receive buffer must shrink");
        };
        let Ok(()) = shrink_socket_buffers(&client) else {
            panic!("the client send buffer must shrink");
        };
        let bytes = [0u8; 1024];
        let mut deferred = false;
        for _ in 0..1024 {
            let Ok(result) = send_attempt(&client, &bytes) else {
                panic!("the send attempt must return an outcome");
            };
            if matches!(result, Attempt::WouldBlock(_)) {
                deferred = true;
                break;
            }
            assert!(matches!(result, Attempt::Done(_)), "a full send must defer");
        }
        assert!(deferred, "a bounded unconsumed socket buffer must fill");
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn send_attempt_avoids_sigpipe_with_default_disposition() {
        const CHILD: &str = "KWOKKA_IO_SIGPIPE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let Ok(executable) = std::env::current_exe() else {
                panic!("the test executable path must be available");
            };
            let Ok(status) = std::process::Command::new(executable)
                .arg("--exact")
                .arg("boundary::seam::socket::tests::send_attempt_avoids_sigpipe_with_default_disposition")
                .env(CHILD, "1")
                .status()
            else {
                panic!("the SIGPIPE fixture must start in its own process");
            };
            assert!(
                status.success(),
                "a per-call MSG_NOSIGNAL send must survive the default disposition"
            );
            return;
        }
        let Ok((server, mut client)) = loopback_pair() else {
            panic!("a loopback pair must be created");
        };
        drop(server);
        let mut eof = [0u8; 1];
        let Ok(0) = client.read(&mut eof) else {
            panic!("the closed peer must be observed before sending");
        };
        // SAFETY: Invariant -- this child process runs only this test, so its
        // signal disposition has no other test consumer. Precondition: no code
        // in this fixture sends after restoring SIGPIPE except `attempt_send`.
        // Failure mode: changing a shared process disposition would make an
        // unrelated send fatal; the parent launches an isolated child to avoid it.
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
        }
        let mut saw_epipe = false;
        for _ in 0..2 {
            let Ok(result) = send_attempt(&client, b"x") else {
                panic!("the send attempt must return an outcome");
            };
            if matches!(result, Attempt::Failed(errno) if errno == -libc::EPIPE) {
                saw_epipe = true;
                break;
            }
        }
        assert!(saw_epipe, "the closed peer must eventually return EPIPE");
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn duplicate_outlives_original_and_is_close_on_exec() {
        let Ok(file) = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")) else {
            panic!("the crate manifest must open");
        };
        let Ok(duplicated) = duplicate_fd(file.as_raw_fd()) else {
            panic!("an open file descriptor must duplicate");
        };
        // SAFETY: Invariant -- `duplicated` owns the descriptor being queried.
        // Precondition: it remains live while `fcntl` reads its flags. Failure
        // mode: an invalid descriptor returns -1, which the assertion rejects.
        let flags = unsafe { libc::fcntl(duplicated.0.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0, "reading duplicate flags must succeed");
        assert_ne!(
            flags & libc::FD_CLOEXEC,
            0,
            "the duplicate is close-on-exec"
        );
        drop(file);
        let mut duplicate_file: File = duplicated.into_owned().into();
        let mut byte = [0u8; 1];
        let Ok(1) = duplicate_file.read(&mut byte) else {
            panic!("the duplicate must remain readable after the original drops");
        };
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn duplicate_keeps_the_socket_across_descriptor_reuse_for_1000_rounds() {
        let mut reused = 0;
        for _ in 0..1_000 {
            let Ok((server, mut client)) = loopback_pair() else {
                panic!("a loopback pair must be created");
            };
            let original_fd = server.as_raw_fd();
            let mut bytes = [0u8; 1];
            let Ok(Attempt::WouldBlock(owned)) = recv_attempt(&server, &mut bytes) else {
                panic!("an empty blocking socket must defer");
            };
            drop(server);
            let Ok(taken) = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")) else {
                panic!("an open must claim the released descriptor number");
            };
            if taken.as_raw_fd() == original_fd {
                reused += 1;
            }
            let Ok(()) = client.write_all(b"x") else {
                panic!("the client must send through the retained description");
            };
            let mut owned = owned;
            let mut attempts = 0;
            let received = loop {
                attempts += 1;
                // SAFETY: Invariant -- `bytes` is exclusively borrowed for this
                // synchronous retry. Precondition: its pointer is valid for its
                // full length. Failure mode: an invalid or aliased region lets
                // `recv` write outside the test buffer.
                let Ok(result) =
                    (unsafe { attempt_recv(Duplicated(owned), bytes.as_mut_ptr(), bytes.len()) })
                else {
                    panic!("the duplicate receive must return an outcome");
                };
                match result {
                    Attempt::Done(received) => break received,
                    Attempt::WouldBlock(next) if attempts < 100 => owned = next,
                    Attempt::WouldBlock(_) => {
                        panic!("the duplicate receive must complete within 100 attempts");
                    }
                    Attempt::Failed(errno) => {
                        panic!("the duplicate receive must not fail: {errno}");
                    }
                }
            };
            assert_eq!(received, 1);
            assert_eq!(bytes, [b'x']);
        }
        assert!(
            reused > 0,
            "the test must observe descriptor reuse in its own process"
        );
    }
}
