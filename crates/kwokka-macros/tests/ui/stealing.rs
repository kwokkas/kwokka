#[kwokka::main(stealing)]
async fn main() {
    let answer = 40 + 2;
    assert_eq!(answer, 42);
}
