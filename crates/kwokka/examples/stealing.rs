//! The `#[kwokka::main(stealing)]` entry: an async body on a work-stealing crew.
//!
//! The macro builds `Runtime::stealing()` and drives this body with
//! `block_on`. Gated on the `stealing` feature, which turns on the
//! work-stealing crew in the runtime.

#[kwokka::main(stealing)]
async fn main() {
    // black_box keeps the work from being optimized away without printing
    // (workspace lints deny stdout) or asserting (banned outside tests).
    core::hint::black_box(41 + 1);
}
