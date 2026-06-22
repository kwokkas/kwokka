//! Runtime behavior through the facade: the timer resolves, a scope joins
//! its children, and `yield_now` round-trips.
//!
//! These exercise the always-present surface (`runtime`, `task`, `time`)
//! with no feature gates, complementing the net + fs composition in
//! `facade.rs`. One affine runtime per binary keeps worker 0 uncontended.

#![cfg(not(any(miri, loom)))]

use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;

use kwokka::runtime::Runtime;
use kwokka::task::{scope, yield_now};
use kwokka::time::sleep;

// Children are `'static`, so they cannot borrow a stack cell; a process-wide
// atomic is the Send + Sync channel for observing that both children ran.
static CHILDREN_RAN: AtomicUsize = AtomicUsize::new(0);

#[test]
fn sleep_resolves_scope_joins_children_and_yield_round_trips() {
    let Ok(mut runtime) = Runtime::affine() else {
        panic!("the affine runtime must build on this host");
    };

    let (slept, spawned, yielded) = runtime.block_on(async {
        // The timer resolves: the future returns control after the park, so
        // the marker after the await is reached.
        sleep(Duration::from_millis(1)).await;
        let slept = true;

        // The scope stays pending until both children settle; each child
        // bumps the counter before resolving, so the post-scope count proves
        // the scope joined them rather than resolving early.
        let spawned = scope(|scope| {
            let first = scope
                .spawn(async {
                    sleep(Duration::from_millis(1)).await;
                    CHILDREN_RAN.fetch_add(1, Ordering::Relaxed);
                })
                .is_ok();
            let second = scope
                .spawn(async {
                    sleep(Duration::from_millis(1)).await;
                    CHILDREN_RAN.fetch_add(1, Ordering::Relaxed);
                })
                .is_ok();
            first && second
        })
        .await;

        // yield_now hands the worker back once, then resolves on the next poll.
        yield_now().await;
        let yielded = true;

        (slept, spawned, yielded)
    });

    assert!(slept, "the sleep future resolved past its await");
    assert!(spawned, "the scope accepted both children");
    assert!(yielded, "yield_now resumed after handing back the worker");
    assert_eq!(
        CHILDREN_RAN.load(Ordering::Relaxed),
        2,
        "the scope resolved only after both children ran",
    );
}
