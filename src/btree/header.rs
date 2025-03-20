use super::errors::BTreeError;
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
