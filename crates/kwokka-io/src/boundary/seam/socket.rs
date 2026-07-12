//! Creating and adopting the file descriptors the seam hands out.

use std::{
    io,
    os::fd::{FromRawFd, OwnedFd},
};

use crate::addr::AddressFamily;

/// Adopts a nonnegative accept-completion result as an owned descriptor.
///
/// Returns `None` for a negative result -- an `-errno`, not a descriptor.
///
/// Call this only on the result of an accept-class completion. A
/// nonnegative accept result names a descriptor the kernel just created
/// for this process, with no other owner. Adopting any other integer
/// asserts ownership of a descriptor this process may not own, and the
/// returned handle closes it on drop -- an IO-safety violation
/// (incorrect close), not a memory-safety violation.
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
    // on drop and use-after-close races follow. This is an IO-safety
    // concern (incorrect close), not a memory-safety concern: no pointer
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
/// Returns the OS error when the `socket` syscall fails, or
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
    // (matched above) and `socket_type | SOCK_CLOEXEC` is a valid type per
    // socket.2. Failure mode: an unsupported argument yields -1 plus `errno`,
    // handled just below; the call itself cannot corrupt memory.
    let raw = unsafe { libc::socket(domain, socket_type | libc::SOCK_CLOEXEC, 0) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: Invariant -- `socket` just returned a fresh descriptor owned by
    // this process alone, exactly like an accept result. Precondition: `raw` is
    // nonnegative (checked above), so it names a real descriptor with no other
    // owner. Failure mode: adopting a negative value would claim a descriptor
    // owned elsewhere and close it on drop; the sign check excludes that. No
    // pointer dereference occurs (IO-safety, not memory-safety).
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Creates an unconnected, close-on-exec stream socket for `family`.
///
/// The client counterpart of adopting an accepted descriptor: a connect needs
/// an owned socket of the peer's address family before the `io_uring` connect
/// op runs. The shared syscall path lives in `create_socket`.
///
/// # Errors
///
/// Returns the OS error when the `socket` syscall fails, or
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
/// Returns the OS error when the `socket` syscall fails, or
/// [`io::ErrorKind::Unsupported`] for `AddressFamily::Unix` (only IPv4 and IPv6
/// datagram sockets are created).
pub fn create_datagram_socket(family: AddressFamily) -> io::Result<OwnedFd> {
    create_socket(family, libc::SOCK_DGRAM)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A real `socket()` syscall is unsupported under miri's isolation, so this
    // runs off-miri; the Unix-rejection test below returns before any syscall
    // and stays miri-safe.
    #[cfg(all(target_os = "linux", not(miri)))]
    #[test]
    fn create_stream_socket_makes_an_ipv6_socket() {
        let Ok(_socket) = create_stream_socket(crate::addr::AddressFamily::Inet6) else {
            panic!("an IPv6 stream socket must be created");
        };
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
    #[cfg(all(target_os = "linux", not(miri)))]
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
}
