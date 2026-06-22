//! trybuild UI suite for `#[kwokka::main]`: the pass cases run the
//! generated entry points on a real runtime, and every macro error path
//! pins its message.

#[test]
fn ui() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/affine.rs");
    cases.pass("tests/ui/stealing.rs");
    cases.compile_fail("tests/ui/unscheduled.rs");
    cases.compile_fail("tests/ui/unknown.rs");
    cases.compile_fail("tests/ui/legacy.rs");
    cases.compile_fail("tests/ui/unsupported.rs");
    cases.compile_fail("tests/ui/parameterized.rs");
    cases.compile_fail("tests/ui/synchronous.rs");
}
