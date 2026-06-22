//! The `#[kwokka::main(affine)]` entry: an async body on one pinned worker.
//!
//! The macro builds `Runtime::affine()` and drives this body with
//! `block_on`, so the scheduler is named explicitly and never defaulted.

#[kwokka::main(affine)]
async fn main() {
    // black_box keeps the work from being optimized away without printing
    // (workspace lints deny stdout) or asserting (banned outside tests).
    core::hint::black_box(41 + 1);
}
