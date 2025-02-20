use errors::BTreeError;
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub mod errors;

const PAGE_SIZE: usize = 64;

#[repr(u8)]
#[derive(KnownLayout, TryFromBytes, IntoBytes, Immutable, Clone, Copy, Debug, PartialEq)]
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
const HEADER_SIZE: usize = size_of::<Header>();

#[derive(KnownLayout, TryFromBytes, IntoBytes, Immutable)]
#[repr(C)]
struct Key {
    value: U32,
    left_child_page: U32,
    offset: U16,
}

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
            num_keys: U16::new(num_keys),
            free_start: U16::new(free_start),
            free_end: U16::new(free_end),
            first_freeblock: U16::new(first_freeblock),
            fragmented_bytes,
            rightmost_child_page: U32::new(rightmost_child_page),
        }
    }
}

pub struct Node {
    pub data: [u8; PAGE_SIZE],
}

impl Node {
    pub fn new() -> Self {
        let header = Header::new(
            NodeType::Leaf,
            0,
            HEADER_SIZE.try_into().unwrap(),
            PAGE_SIZE.try_into().unwrap(),
            0,
            0,
            0,
        );
        let header_bytes = header.as_bytes();
        let mut data = [0x00; PAGE_SIZE];
        data[0..HEADER_SIZE].copy_from_slice(header_bytes);
        Node { data }
    }

    pub fn read_header(&self) -> Result<&Header, BTreeError> {
        Header::try_ref_from_bytes(&self.data[0..HEADER_SIZE])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    pub fn mutate_header(&mut self) -> Result<&mut Header, BTreeError> {
        Header::try_mut_from_bytes(&mut self.data[0..HEADER_SIZE])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    pub fn unallocated_space(&self) -> Result<usize, BTreeError> {
        let header = self.read_header()?;
        Ok((header.free_end.get() - header.free_start.get()) as usize)
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutate_and_read_header() -> Result<(), BTreeError> {
        let mut node = Node::new();

        {
            let header_mut = node.mutate_header()?;
            header_mut.node_type = NodeType::Internal;
            header_mut.num_keys.set(42);
            header_mut.free_start.set(10);
            header_mut.free_end.set(54);
            header_mut.first_freeblock.set(5);
            header_mut.fragmented_bytes = 2;
            header_mut.rightmost_child_page.set(1234);
        }

        let header = node.read_header()?;

        assert_eq!(header.node_type, NodeType::Internal);
        assert_eq!(header.num_keys.get(), 42);
        assert_eq!(header.free_start.get(), 10);
        assert_eq!(header.free_end.get(), 54);
        assert_eq!(header.first_freeblock.get(), 5);
        assert_eq!(header.fragmented_bytes, 2);
        assert_eq!(header.rightmost_child_page.get(), 1234);

        Ok(())
    }
}
