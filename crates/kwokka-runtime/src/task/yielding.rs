//! Cooperative yield primitive that returns `Pending` once before completing.
//!
//! Calling `yield_now().await` lets a long-running task hand control back to
//! the executor so other ready tasks can make progress. Unlike `Pending`
//! futures that wait for an external wake (timer, I/O completion), [`YieldNow`]
//! wakes its own waker by reference on the first poll and resolves on the
//! second.
//!
//! # Examples
//!
//! ```rust
//! # async fn run() {
//! use kwokka_runtime::task::yield_now;
//!
//! for _ in 0..10_000 {
//!     // Long synchronous work in an async context.
//! }
//! yield_now().await;
//! // Other ready tasks may have run between the two polls.
//! # }
//! ```

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Cooperative yield. The first poll wakes the current waker by reference and
/// returns [`Poll::Pending`]; the second poll returns [`Poll::Ready`].
///
/// The future carries no state beyond a one-bit "have I yielded yet" flag, so
/// it is `Send + Sync` regardless of the surrounding task's mode.
///
/// # Examples
///
/// ```rust
/// # async fn run() {
/// use kwokka_runtime::task::yield_now;
/// yield_now().await;
/// # }
/// ```
#[must_use = "yield_now does nothing unless awaited"]
pub const fn yield_now() -> YieldNow {
    YieldNow { has_yielded: false }
}

/// Future returned by [`yield_now`]. See module docs for the wake handshake.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct YieldNow {
    has_yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.has_yielded {
            return Poll::Ready(());
        }
        this.has_yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use core::{
        ptr,
        sync::atomic::{AtomicUsize, Ordering},
        task::{RawWaker, RawWakerVTable, Waker},
    };

    use super::*;

    static WAKE_VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| RawWaker::new(data, &WAKE_VTABLE),
        |_| {},
        |data| {
            // SAFETY: the data pointer is derived from `&AtomicUsize` in
            // `tracking_waker`. The borrow is alive for the duration of
            // each test because the counter is stack-allocated above the
            // waker on the same frame. Dangling dereference if the counter
            // were dropped before the vtable callback fires.
            let counter = unsafe { &*data.cast::<AtomicUsize>() };
            counter.fetch_add(1, Ordering::Relaxed);
        },
        |_| {},
    );

    fn tracking_waker(counter: &AtomicUsize) -> Waker {
        let data = ptr::from_ref(counter).cast::<()>();
        // SAFETY: `WAKE_VTABLE` is `'static` and treats `data` strictly as
        // a `*const AtomicUsize`. `counter` outlives the waker because the
        // waker is dropped before the test returns. Use-after-free if the
        // waker escaped the test frame.
        unsafe { Waker::from_raw(RawWaker::new(data, &WAKE_VTABLE)) }
    }

    #[test]
    fn first_poll_returns_pending_and_wakes_by_ref() {
        let counter = AtomicUsize::new(0);
        let waker = tracking_waker(&counter);
        let mut cx = Context::from_waker(&waker);
        let mut fut = yield_now();
        let pinned = Pin::new(&mut fut);
        let result = pinned.poll(&mut cx);
        assert!(matches!(result, Poll::Pending));
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "first poll must wake the waker exactly once",
        );
    }

    #[test]
    fn second_poll_returns_ready() {
        let counter = AtomicUsize::new(0);
        let waker = tracking_waker(&counter);
        let mut cx = Context::from_waker(&waker);
        let mut fut = yield_now();
        let _ = Pin::new(&mut fut).poll(&mut cx);
        let second = Pin::new(&mut fut).poll(&mut cx);
        assert!(matches!(second, Poll::Ready(())));
    }

    #[test]
    fn second_poll_does_not_wake_again() {
        let counter = AtomicUsize::new(0);
        let waker = tracking_waker(&counter);
        let mut cx = Context::from_waker(&waker);
        let mut fut = yield_now();
        let _ = Pin::new(&mut fut).poll(&mut cx);
        let _ = Pin::new(&mut fut).poll(&mut cx);
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "the Ready arm must not wake again",
        );
    }

    #[test]
    fn yield_now_future_is_send_and_sync() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<YieldNow>();
    }
}
