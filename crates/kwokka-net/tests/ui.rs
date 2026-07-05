//! trybuild UI suite for the CAP-generic buffer methods. The compile-fail case
//! pins the oversized-`CAP` guard message; the boundary case proves a `CAP` at
//! the `MAX_INLINE_CAP` ceiling still compiles.

#[test]
fn ui() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/cap_at_limit.rs");
    cases.compile_fail("tests/ui/cap_over_limit.rs");
}
