use errors::BTreeError;
use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub mod errors;

const PAGE_SIZE: u16 = 4096;

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

const HEADER_SIZE: u16 = {
    if size_of::<Header>() > u16::MAX as usize {
        panic!("Header size does not fit into u16");
    }
    size_of::<Header>() as u16
};

#[derive(Debug, KnownLayout, FromBytes, IntoBytes, Immutable)]
#[repr(C)]
pub struct Key {
    key: U64,
    left_child_page: U32,
    value_offset: U16,
    value_len: U16,
}
const KEY_SIZE: u16 = {
    if size_of::<Key>() > u16::MAX as usize {
        panic!("Key size does not fit into u16");
    }
    size_of::<Key>() as u16
};

impl Key {
    fn new(key: u64, left_child_page: u32, value_offset: u16, value_len: u16) -> Self {
        Self {
            key: key.into(),
            left_child_page: left_child_page.into(),
            value_offset: value_offset.into(),
            value_len: value_len.into(),
        }
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct Value {
    data: [u8],
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
            num_keys: num_keys.into(),
            free_start: free_start.into(),
            free_end: free_end.into(),
            first_freeblock: first_freeblock.into(),
            fragmented_bytes,
            rightmost_child_page: rightmost_child_page.into(),
        }
    }
}

pub struct Node<'a> {
    page: &'a mut [u8],
}

impl<'a> Node<'a> {
    pub fn new(page: &'a mut [u8]) -> Result<Self, BTreeError> {
        if page.len() != PAGE_SIZE.into() {
            return Err(BTreeError::UnexpectedData {
                expected: PAGE_SIZE.into(),
                actual: page.len(),
            });
        }

        let mut node = Self { page };

        let header = node.mutate_header()?;
        header.node_type = NodeType::Leaf;
        header.num_keys = 0.into();
        header.free_start = HEADER_SIZE.into();
        header.free_end = PAGE_SIZE.into();
        header.first_freeblock = 0.into();
        header.fragmented_bytes = 0;
        header.rightmost_child_page = 0.into();

        Ok(node)
    }

    fn read_header(&self) -> Result<&Header, BTreeError> {
        Header::try_ref_from_bytes(&self.page[0..HEADER_SIZE as usize])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    fn mutate_header(&mut self) -> Result<&mut Header, BTreeError> {
        Header::try_mut_from_bytes(&mut self.page[0..HEADER_SIZE as usize])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    fn unallocated_space(&self) -> Result<usize, BTreeError> {
        let header = self.read_header()?;
        Ok((header.free_end.get() - header.free_start.get()) as usize)
    }

    pub fn insert(&mut self, key: u64, value: &[u8]) -> Result<(), BTreeError> {
        let value_len = u16::try_from(value.len()).map_err(|_| BTreeError::UnexpectedData {
            expected: 65535,
            actual: value.len(),
        })?;

        if self.unallocated_space()? < (KEY_SIZE + value_len).into() {
            todo!("Handle overflow");
        }

        let new_free_end = {
            let header = self.mutate_header()?;
            header.free_end -= value.len() as u16;
            header.free_end.get() as usize
        };

        self.page[new_free_end..new_free_end + value.len()].copy_from_slice(value);

        let free_start: usize = self.read_header()?.free_start.get().into();

        let new_key = Key::new(key, 0, new_free_end as u16, value_len);
        let key_bytes = new_key.as_bytes();
        self.page[free_start..free_start + KEY_SIZE as usize].copy_from_slice(key_bytes);

        let header = self.mutate_header()?;
        header.free_start += KEY_SIZE;
        header.num_keys += 1;

        Ok(())
    }

    /// Returns lowest index where key < other_key is true through binary search.
    fn find_le_key(&self, key: u64) -> Result<(usize, bool), BTreeError> {
        let header = self.read_header()?;
        let num_keys = header.num_keys.get();

        if num_keys == 0 {
            return Ok((0, false));
        }

        let mut low = 0;
        let mut high = num_keys;

        while low < high {
            let mid = (low + high) / 2;
            let current_key = self.get_key_at(mid)?.key.get();

            // https://github.com/rust-lang/rust-clippy/issues/5354
            #[allow(clippy::comparison_chain)]
            if current_key == key {
                return Ok((mid.into(), true));
            } else if current_key < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        Ok((low.into(), false))
    }

    fn get_key_at(&self, index: u16) -> Result<&Key, BTreeError> {
        let key_pos = (HEADER_SIZE + KEY_SIZE * index) as usize;
        Key::ref_from_bytes(&self.page[key_pos..(key_pos + KEY_SIZE as usize)])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    fn get_mut_key_at(&mut self, index: u16) -> Result<&mut Key, BTreeError> {
        let key_pos = (HEADER_SIZE + KEY_SIZE * index) as usize;
        Key::mut_from_bytes(&mut self.page[key_pos..(key_pos + KEY_SIZE as usize)])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutate_and_read_header() -> Result<(), BTreeError> {
        let mut page = [0x00; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page)?;

        {
            let header_mut = node.mutate_header()?;
            header_mut.node_type = NodeType::Internal;
            header_mut.num_keys.set(42);
            header_mut.free_start.set(10);
            header_mut.free_end.set(4);
            header_mut.first_freeblock.set(5);
            header_mut.fragmented_bytes = 2;
            header_mut.rightmost_child_page.set(1234);
        }

        let header = node.read_header()?;

        assert_eq!(header.node_type, NodeType::Internal);
        assert_eq!(header.num_keys.get(), 42);
        assert_eq!(header.free_start.get(), 10);
        assert_eq!(header.free_end.get(), 4);
        assert_eq!(header.first_freeblock.get(), 5);
        assert_eq!(header.fragmented_bytes, 2);
        assert_eq!(header.rightmost_child_page.get(), 1234);

        Ok(())
    }

    #[test]
    fn find_le_key() -> Result<(), BTreeError> {
        let mut page = [0x00; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page)?;

        node.insert(1, b"111")?;
        node.insert(4, b"444444")?;
        node.insert(6, b"66")?;

        assert_eq!(node.find_le_key(1)?, (0, true));
        assert_eq!(node.find_le_key(4)?, (1, true));
        assert_eq!(node.find_le_key(6)?, (2, true));

        assert_eq!(node.find_le_key(0)?, (0, false));
        assert_eq!(node.find_le_key(2)?, (1, false));
        assert_eq!(node.find_le_key(3)?, (1, false));
        assert_eq!(node.find_le_key(5)?, (2, false));
        assert_eq!(node.find_le_key(7)?, (3, false));

        Ok(())
    }
}
