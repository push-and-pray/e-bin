use super::errors::BTreeError;

use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{
    try_transmute_mut, try_transmute_ref, FromBytes, Immutable, IntoBytes, KnownLayout,
};

#[derive(Clone, Debug, KnownLayout, FromBytes, IntoBytes, Immutable)]
#[repr(C)]
pub struct Key {
    pub key: U64,
    pub left_child_page: U32,
    pub value_offset: U16,
    pub value_len: U16,
}
pub const KEY_SIZE: u16 = {
    if size_of::<Key>() > u16::MAX as usize {
        panic!("Key size does not fit into u16");
    }
    size_of::<Key>() as u16
};

impl Key {
    pub fn new(key: u64, left_child_page: u32, value_offset: u16, value_len: u16) -> Self {
        Self {
            key: key.into(),
            left_child_page: left_child_page.into(),
            value_offset: value_offset.into(),
            value_len: value_len.into(),
        }
    }

    pub fn intepret_from_bytes(bytes: &[u8; KEY_SIZE as usize]) -> Result<&Self, BTreeError> {
        try_transmute_ref!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    pub fn intepret_mut_from_bytes(
        bytes: &mut [u8; KEY_SIZE as usize],
    ) -> Result<&mut Self, BTreeError> {
        try_transmute_mut!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }
}
