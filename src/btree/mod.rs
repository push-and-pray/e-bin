/*
*   Node Header
*   OFFSET  LEN    DESC
*   0       1       type of node (internal: 0x00, leaf: 0x01)
*   1       2       number of keys
*   3       2       free start offset
*   5       2       free end offset
*   7       2       offset to first freeblock. 0x00 if none
*   9       1       number of fragmented free bytes
*   10      4       rightmost child page number
*   14      2       key bytes size (klen)
*   16      +       key start
*
*   Key struct
*   OFFSET  LEN    DESC
*   0       2       offset to value
*   2       klen    key value
*
*   Value struct
*   OFFSET  LEN    DESC
*   0       4       left child page number
*   4       2       value len bytes (vlen)
*   6       vlen    value
*
*   Freeblock struct
*
*   OFFSET  LEN    DESC
*   0       2       len bytes
*   2       2       offset of next freeblock, 0x0000 if last
*/

use errors::{BTreeError, InvalidHeaderError};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unalign};

pub mod errors;

const PAGE_SIZE: usize = 64;

#[repr(u8)]
#[derive(KnownLayout, TryFromBytes, IntoBytes, Immutable)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[derive(KnownLayout, TryFromBytes, IntoBytes, Immutable)]
#[repr(C)]
pub struct Header {
    pub node_type: Unalign<NodeType>,
    pub num_keys: Unalign<u16>,
    pub free_start: Unalign<u16>,
    pub free_end: Unalign<u16>,
    pub first_freeblock: Unalign<u16>,
    pub fragmented_bytes: Unalign<u8>,
    pub rightmost_child_page: Unalign<u32>,
}
const HEADER_SIZE: usize = size_of::<Header>();

struct Key {
    value: u16,
    left_child_page: u32,
    offset: u16,
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
            node_type: Unalign::new(node_type),
            num_keys: Unalign::new(num_keys),
            free_start: Unalign::new(free_start),
            free_end: Unalign::new(free_end),
            first_freeblock: Unalign::new(first_freeblock),
            fragmented_bytes: Unalign::new(fragmented_bytes),
            rightmost_child_page: Unalign::new(rightmost_child_page),
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
}

impl Default for Node {
    fn default() -> Self {
        Node::new()
    }
}
