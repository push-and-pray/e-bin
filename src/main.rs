use e_bin::page::PageManager;

const PAGESIZE: usize = 64;

fn main() {
    let mut fm = PageManager::new("./tmp.db", PAGESIZE).unwrap();

    let test = fm.read_page(0).unwrap();

    println!("{:?}", test.read());
}
