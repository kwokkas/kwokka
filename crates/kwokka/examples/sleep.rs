//! Awaiting `kwokka::time::sleep` inside an affine runtime.
//!
//! Builds the runtime by hand through `Runtime::affine()` rather than the
//! entry macro, then parks the task on the timer for a wall-clock span.

use core::time::Duration;

use kwokka::{runtime::Runtime, time::sleep};

fn main() -> std::io::Result<()> {
    let mut runtime = Runtime::affine()?;
    runtime.block_on(async {
        // sleep parks the task on the runtime timer rather than spinning.
        sleep(Duration::from_millis(1)).await;
    });
    Ok(())
}
