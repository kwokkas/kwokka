//! Fanning children out through a structured scope on the affine runtime.
//!
//! `scope` runs its builder with a `Scope`, spawns two children, then stays
//! pending until both settle -- structured concurrency with no free spawn.

use core::time::Duration;

use kwokka::{runtime::Runtime, task::scope, time::sleep};

fn main() -> std::io::Result<()> {
    let mut runtime = Runtime::affine()?;
    runtime.block_on(async {
        // The builder runs once and spawns two children; the scope future
        // resolves only after both have settled. spawn returns Result, so a
        // full inbox is observable rather than silent.
        scope(|scope| {
            let first = scope.spawn(async {
                sleep(Duration::from_millis(1)).await;
            });
            let second = scope.spawn(async {
                sleep(Duration::from_millis(1)).await;
            });
            core::hint::black_box((first.is_ok(), second.is_ok()));
        })
        .await;
    });
    Ok(())
}
