use e_bin::btree::Node;

fn main() {
    let mut node = Node::new();
    let header = node.mutate_header().unwrap();

    header.num_keys.set(45);

    let h = node.read_header().unwrap();
    println!("{}", h.num_keys.get());
}
