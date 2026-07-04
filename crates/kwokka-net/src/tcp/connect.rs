//! Connecting a socket to a peer address.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kwokka_io::{
    addr::SockAddr,
    boundary::{self, IoSeam},
    operation::{IoRequest, SubmitResult},
};

/// A future that connects socket `fd` to a peer address.
///
/// The connect counterpart of the accept future: the first poll moves the
/// address into a connect op submitted through the io seam -- addressed by
/// the polling task's identity token for the `user_data` round trip -- and
/// yields `Pending`. A later poll, woken by
/// the completion drain, returns the kernel result: `0` on success, or a
/// negative `-errno`. The address is moved out on submit, so the future
/// owns no storage the kernel could dangle on.
///
/// At most one connect may be in flight per worker. The driver packs the
/// address into its single submission scratch buffer, so a second connect
/// submitted while one is in flight overwrites the first address in place.
/// This 0.1.0 limit lifts when per-op address storage lands.
///
/// # Panics
///
/// Panics when polled with a waker that is not the runtime's task waker
/// (for example inside a combinator that wraps the waker): the
/// `user_data` round trip decodes the polling task from the waker, so
/// await it directly.
#[must_use = "futures do nothing unless polled"]
pub(super) struct ConnectFuture {
    /// Socket file descriptor to connect.
    fd: i32,
    /// Peer address; taken on submit, so `None` marks the submitted state.
    addr: Option<SockAddr>,
    /// Optional native deadline in nanoseconds. When set, the connect submits
    /// under a native kernel link-timeout, so a `-ECANCELED` result (or `-EINTR`
    /// if the connect was already in flight) means the deadline elapsed. A
    /// deadline-armed connect has no separate cancel path, so that result is
    /// unambiguous.
    deadline_ns: Option<u64>,
    /// The worker and token the connect submitted under, once in flight. `Some`
    /// gates the cancel on drop; cleared when the op resolves, so a completed
    /// connect is never cancelled.
    submitted: Option<ConnectOp>,
}

/// The worker and `user_data` token a submitted connect is cancelled by.
#[derive(Clone, Copy)]
struct ConnectOp {
    worker_id: u8,
    token: u64,
}

impl ConnectFuture {
    /// Constructs a connect future for socket `fd` toward `addr`.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "pending the public client-connect entry")
    )]
    pub(crate) const fn new(fd: i32, addr: SockAddr) -> Self {
        Self {
            fd,
            addr: Some(addr),
            deadline_ns: None,
            submitted: None,
        }
    }

    /// Constructs a connect future bounded by a native `deadline_ns` deadline.
    ///
    /// The connect submits under a native kernel link-timeout; if the deadline
    /// elapses first the kernel cancels the connect, which surfaces as
    /// `-ECANCELED` (or `-EINTR` if it was already in flight). A backend without
    /// `link_timeout` rejects the deadline path rather than dropping the bound.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "pending the public client-connect entry")
    )]
    pub(crate) const fn with_deadline(fd: i32, addr: SockAddr, deadline_ns: u64) -> Self {
        Self {
            fd,
            addr: Some(addr),
            deadline_ns: Some(deadline_ns),
            submitted: None,
        }
    }
}

impl Future for ConnectFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        // The polling task's identity is encoded in its waker; the seam
        // decoder rejects a waker the runtime did not build, the same
        // contract the accept future holds.
        let Some(binding) = boundary::decode_waker(cx.waker()) else {
            panic!("ConnectFuture requires the runtime task waker; await it directly");
        };
        let this = self.get_mut();
        // The address doubles as the submit-once gate: taking it marks the
        // op submitted, so later polls fall through to the result read.
        let Some(addr) = this.addr.take() else {
            return match IoSeam::with_current(binding.worker_id, IoSeam::completion_result) {
                Some(Some(slot)) => {
                    // The op resolved; clear the cancel guard so drop is a no-op.
                    this.submitted = None;
                    Poll::Ready(slot.result)
                }
                _ => Poll::Pending,
            };
        };
        let request = IoRequest::<()>::connect(this.fd, addr).with_user_data(binding.token);
        // A deadline arms the connect with a native LINK_TIMEOUT; without one it
        // takes the plain submit. The deadline is a Copy value read before the
        // closure moves `request`.
        let deadline = this.deadline_ns;
        let submit = IoSeam::with_current(binding.worker_id, |seam| match deadline {
            Some(deadline_ns) => seam.submit_linked_timeout_internal(&request, deadline_ns),
            None => seam.submit_internal(request),
        });
        match submit {
            Some(Some(SubmitResult::Submitted(_))) => {
                // Record the op so a drop before completion cancels it.
                this.submitted = Some(ConnectOp {
                    worker_id: binding.worker_id,
                    token: binding.token,
                });
                Poll::Pending
            }
            // No seam, no driver, or the backend rejected the op (including a
            // deadline requested where the kernel lacks link_timeout). The
            // production path runs on a real io_uring driver, so this is the
            // test-seam / unsupported path; resolve with -EINVAL rather than hang.
            _ => Poll::Ready(-22),
        }
    }
}

impl Drop for ConnectFuture {
    fn drop(&mut self) {
        if let Some(op) = self.submitted {
            // The submitted connect made the task `io_bound`, so this drop runs
            // on the owning worker and the cancel reaches the inbox single-writer.
            boundary::push_connect_cancel_for_worker(op.worker_id, op.token);
        }
    }
}

#[cfg(all(target_os = "linux", not(any(miri, loom))))]
#[cfg(test)]
mod tests {
    use std::{
        net::{SocketAddr, UdpSocket},
        os::fd::AsRawFd,
    };

    use kwokka_runtime::Runtime;

    use super::*;

    // End-to-end connect through the affine run-loop, on the real ring. UDP
    // gives std an unconnected socket fd with no extra dependency, and
    // connect on a datagram socket records the default peer, which the
    // follow-up std `send` proves: a send with no destination succeeds only
    // on a connected socket.
    #[test]
    fn connect_records_the_peer() {
        let Ok(peer) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the peer socket must succeed");
        };
        let Ok(client) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the client socket must succeed");
        };
        let Ok(SocketAddr::V4(peer_v4)) = peer.local_addr() else {
            panic!("a loopback bind must report a V4 local address");
        };

        let Ok(mut runtime) = Runtime::affine() else {
            panic!("the affine runtime must build on this host");
        };
        let result = runtime.block_on(ConnectFuture::new(
            client.as_raw_fd(),
            SockAddr::V4(peer_v4),
        ));
        assert_eq!(result, 0, "the connect completed with an error: {result}");

        // A destination-less send succeeds only on a connected socket, proving
        // the kernel recorded the peer the future submitted.
        let payload = b"kwokka connect probe";
        let Ok(sent) = client.send(payload) else {
            panic!("a send on the connected socket must succeed");
        };
        assert_eq!(sent, payload.len(), "the send delivered every byte");

        let mut buf = [0u8; 32];
        let Ok(received) = peer.recv(&mut buf) else {
            panic!("the peer recv must succeed");
        };
        assert_eq!(
            &buf[..received],
            &payload[..],
            "the peer holds the bytes sent over the connected socket",
        );
    }

    // A deadline-armed connect that finishes well inside its deadline records
    // the peer just like a plain connect. This drives the full deadline path on
    // the real ring -- the LINK_TIMEOUT submit, the connect winning the link,
    // and the paired timeout's discard CQE being dropped without disturbing the
    // result. The deadline-fires cancellation is proven at the io layer.
    #[test]
    fn deadline_connect_records_the_peer() {
        let Ok(peer) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the peer socket must succeed");
        };
        let Ok(client) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the client socket must succeed");
        };
        let Ok(SocketAddr::V4(peer_v4)) = peer.local_addr() else {
            panic!("a loopback bind must report a V4 local address");
        };

        let Ok(mut runtime) = Runtime::affine() else {
            panic!("the affine runtime must build on this host");
        };
        // Five seconds dwarfs a loopback connect, so the connect wins the link
        // and the deadline never fires.
        let result = runtime.block_on(ConnectFuture::with_deadline(
            client.as_raw_fd(),
            SockAddr::V4(peer_v4),
            5_000_000_000,
        ));
        assert_eq!(
            result, 0,
            "the deadline-armed connect completed with an error: {result}"
        );

        let payload = b"kwokka deadline connect probe";
        let Ok(sent) = client.send(payload) else {
            panic!("a send on the connected socket must succeed");
        };
        assert_eq!(sent, payload.len(), "the send delivered every byte");

        let mut buf = [0u8; 40];
        let Ok(received) = peer.recv(&mut buf) else {
            panic!("the peer recv must succeed");
        };
        assert_eq!(
            &buf[..received],
            &payload[..],
            "the peer holds the bytes sent over the deadline-armed connected socket",
        );
    }

    // Polls one connect exactly once -- submitting it and yielding `Pending` --
    // then drops it in flight, firing `ConnectFuture::drop` and its cancel.
    struct DropInFlightConnect(Option<ConnectFuture>);

    impl Future for DropInFlightConnect {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if let Some(mut connect) = self.0.take() {
                let _outcome = Pin::new(&mut connect).poll(cx);
                drop(connect);
            }
            Poll::Ready(())
        }
    }

    // Dropping an in-flight connect queues its cancel on the owning worker; a
    // fresh connect on the same runtime drains that cancel and the dropped op's
    // completion and must still succeed, proving the drop-cancel path wires
    // through without wedging or corrupting the shard. The fd-0 non-close
    // guarantee is proven at the io layer.
    #[test]
    fn dropped_connect_keeps_serving() {
        let Ok(peer) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the peer socket must succeed");
        };
        let Ok(SocketAddr::V4(peer_v4)) = peer.local_addr() else {
            panic!("a loopback bind must report a V4 local address");
        };
        let Ok(dropped_client) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the dropped client socket must succeed");
        };
        let Ok(fresh_client) = UdpSocket::bind("127.0.0.1:0") else {
            panic!("binding the fresh client socket must succeed");
        };

        let Ok(mut runtime) = Runtime::affine() else {
            panic!("the affine runtime must build on this host");
        };
        runtime.block_on(DropInFlightConnect(Some(ConnectFuture::new(
            dropped_client.as_raw_fd(),
            SockAddr::V4(peer_v4),
        ))));
        let result = runtime.block_on(ConnectFuture::new(
            fresh_client.as_raw_fd(),
            SockAddr::V4(peer_v4),
        ));
        assert_eq!(
            result, 0,
            "a fresh connect after the in-flight drop must succeed: {result}",
        );
    }
}
