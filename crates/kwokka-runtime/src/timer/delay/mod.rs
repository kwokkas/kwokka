//! Futures that wait out a wheel-tick delay.

mod sleeping;

pub use sleeping::{Sleep, sleep};
