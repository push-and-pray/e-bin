fn main() {
    let test: usize = 1000;
    println!("{:#X?}", bincode::serialize(&test).unwrap())
}
