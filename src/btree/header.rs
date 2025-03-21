use super::errors::BTreeError;
use super::Node;
use zerocopy::little_endian::{U16, U32};
use zerocopy::{
    try_transmute_mut, try_transmute_ref, Immutable, IntoBytes, KnownLayout, TryFromBytes,
};

#[derive(Debug, PartialEq, KnownLayout, TryFromBytes, IntoBytes, Immutable)]
#[repr(u8)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[derive(KnownLayout, TryFromBytes, IntoBytes, Immutable)]
#[repr(C)]
pub struct Header {
    pub node_type: NodeType,
    pub num_keys: U16,
    pub free_start: U16,
    pub free_end: U16,
    pub first_freeblock: U16,
    pub fragmented_bytes: u8,
    pub rightmost_child_page: U32,
}

pub const HEADER_SIZE: u16 = {
    if size_of::<Header>() > u16::MAX as usize {
        panic!("Header size does not fit into u16");
    }
    size_of::<Header>() as u16
};

impl Header {
    pub fn new(
        node_type: NodeType,
        num_keys: u16,
        free_start: u16,
        free_end: u16,
        first_freeblock: u16,
        fragmented_bytes: u8,
        rightmost_child_page: u32,
    ) -> Self {
        Header {
            node_type,
            num_keys: num_keys.into(),
            free_start: free_start.into(),
            free_end: free_end.into(),
            first_freeblock: first_freeblock.into(),
            fragmented_bytes,
            rightmost_child_page: rightmost_child_page.into(),
        }
    }
    pub fn intepret_from_bytes(bytes: &[u8; HEADER_SIZE as usize]) -> Result<&Self, BTreeError> {
        try_transmute_ref!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    pub fn intepret_mut_from_bytes(
        bytes: &mut [u8; HEADER_SIZE as usize],
    ) -> Result<&mut Self, BTreeError> {
        try_transmute_mut!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }
}

impl<'a> Node<'a> {
    pub fn read_header(&self) -> Result<&Header, BTreeError> {
        let header_bytes: &[u8; HEADER_SIZE as usize] = self
            .get_page_slice(0, HEADER_SIZE as usize)
            .try_into()
            .expect("This should never fail, as the sizes are hardcoded to be the same");
        Header::intepret_from_bytes(header_bytes)
    }

    pub fn mutate_header(&mut self) -> Result<&mut Header, BTreeError> {
        let header_bytes: &mut [u8; HEADER_SIZE as usize] = self
            .get_mut_page_slice(0, HEADER_SIZE as usize)
            .try_into()
            .expect("This should never fail, as the sizes are hardcoded to be the same");
        Header::intepret_mut_from_bytes(header_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::super::header::HEADER_SIZE;
    use super::super::{Node, PAGE_SIZE};
    use super::*;

    #[test]
    fn test_intepret_from_bytes() {
        let header = Header::new(NodeType::Leaf, 10, HEADER_SIZE, 4096, 0, 5, 1234);
        let header_bytes = header.as_bytes();
        let mut arr = [0u8; HEADER_SIZE as usize];
        arr.copy_from_slice(header_bytes);
        let header_ref = Header::intepret_from_bytes(&arr).unwrap();
        assert_eq!(header_ref.node_type, NodeType::Leaf);
        assert_eq!(header_ref.num_keys.get(), 10);
        assert_eq!(header_ref.free_start.get(), HEADER_SIZE);
        assert_eq!(header_ref.free_end.get(), 4096);
        assert_eq!(header_ref.first_freeblock.get(), 0);
        assert_eq!(header_ref.fragmented_bytes, 5);
        assert_eq!(header_ref.rightmost_child_page.get(), 1234);
    }

    #[test]
    fn test_intepret_mut_from_bytes() {
        let header = Header::new(NodeType::Internal, 0, HEADER_SIZE, 4096, 0, 0, 0);
        let header_bytes = header.as_bytes();
        let mut arr = [0u8; HEADER_SIZE as usize];
        arr.copy_from_slice(header_bytes);
        {
            let header_mut = Header::intepret_mut_from_bytes(&mut arr).unwrap();
            header_mut.num_keys = 20.into();
            header_mut.fragmented_bytes = 7;
        }
        let header_ref = Header::intepret_from_bytes(&arr).unwrap();
        assert_eq!(header_ref.num_keys.get(), 20);
        assert_eq!(header_ref.fragmented_bytes, 7);
    }

    #[test]
    fn test_node_read_and_mutate_header() {
        let mut page = [0x00; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        {
            let header_mut = node.mutate_header().unwrap();
            header_mut.node_type = NodeType::Internal;
            header_mut.num_keys.set(42);
            header_mut.free_start.set(10);
            header_mut.free_end.set(4);
            header_mut.first_freeblock.set(5);
            header_mut.fragmented_bytes = 2;
            header_mut.rightmost_child_page.set(1234);
        }

        let header = node.read_header().unwrap();

        assert_eq!(header.node_type, NodeType::Internal);
        assert_eq!(header.num_keys.get(), 42);
        assert_eq!(header.free_start.get(), 10);
        assert_eq!(header.free_end.get(), 4);
        assert_eq!(header.first_freeblock.get(), 5);
        assert_eq!(header.fragmented_bytes, 2);
        assert_eq!(header.rightmost_child_page.get(), 1234);
    }
}
