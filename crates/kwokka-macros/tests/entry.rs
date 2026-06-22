//! The expanded entry points drive real runtimes end to end.

#[kwokka::main(affine)]
async fn run_affine() -> u32 {
    41 + 1
}

#[kwokka::main(stealing)]
async fn run_stealing() -> u32 {
    40 + 2
}

#[test]
fn the_affine_entry_runs_to_completion() {
    assert_eq!(run_affine(), 42);
}

#[test]
fn the_stealing_entry_runs_to_completion() {
    assert_eq!(run_stealing(), 42);
}
