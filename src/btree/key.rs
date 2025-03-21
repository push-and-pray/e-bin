use super::errors::BTreeError;
use super::header::HEADER_SIZE;
use super::Node;

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

impl<'a> Node<'a> {
    pub fn insert_key_at(&mut self, key: &Key, idx: u16) -> Result<(), BTreeError> {
        debug_assert!(self.unallocated_space().unwrap() >= KEY_SIZE);

        let header = self.read_header()?;
        let keys_end = header.free_start.get() as usize;
        let pos = self.get_key_pos(idx);

        self.page
            .copy_within(pos as usize..keys_end, (pos + KEY_SIZE).into());

        self.get_mut_page_slice(pos as usize, KEY_SIZE as usize)
            .copy_from_slice(Key::as_bytes(key));

        let header = self.mutate_header()?;
        header.free_start += KEY_SIZE;
        header.num_keys += 1;

        Ok(())
    }

    pub fn pop_key_at(&mut self, idx: u16) -> Result<Key, BTreeError> {
        let key_pos = self.get_key_pos(idx);
        debug_assert!(key_pos < self.read_header().unwrap().free_start.get());

        let (key_ref, _offset) = self.read_key_at(idx)?;
        let key = key_ref.clone();
        let keys_end = self.read_header()?.free_start.get() as usize;

        self.page
            .copy_within((key_pos + KEY_SIZE) as usize..keys_end, key_pos as usize);

        let header = self.mutate_header()?;
        header.free_start -= KEY_SIZE;
        header.num_keys -= 1;

        Ok(key)
    }

    pub fn find_le_key_idx(&self, key: u64) -> Result<(usize, bool), BTreeError> {
        let header = self.read_header()?;
        let num_keys = header.num_keys.get();

        if num_keys == 0 {
            return Ok((0, false));
        }

        let mut low = 0;
        let mut high = num_keys;

        while low < high {
            let mid = (low + high) / 2;
            let (key_ptr, _offset) = self.read_key_at(mid)?;
            let current_key = key_ptr.key.get();

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

    pub fn get_key_pos(&self, index: u16) -> u16 {
        HEADER_SIZE + KEY_SIZE * index
    }

    pub fn read_key_at(&self, index: u16) -> Result<(&Key, usize), BTreeError> {
        let key_pos = self.get_key_pos(index) as usize;
        let key_bytes: &[u8; KEY_SIZE as usize] = self
            .get_page_slice(key_pos, KEY_SIZE as usize)
            .try_into()
            .expect("Shouldn't fail, hardcoded");
        Ok((Key::intepret_from_bytes(key_bytes)?, key_pos))
    }

    fn mut_key_at(&mut self, index: u16) -> Result<(&mut Key, usize), BTreeError> {
        let key_pos = self.get_key_pos(index) as usize;
        let key_bytes: &mut [u8; KEY_SIZE as usize] = self
            .get_mut_page_slice(key_pos, KEY_SIZE as usize)
            .try_into()
            .expect("Shouldn't fail, hardcoded");
        Ok((Key::intepret_mut_from_bytes(key_bytes)?, key_pos))
    }
}

#[cfg(test)]
mod tests {
    use super::super::PAGE_SIZE;
    use super::*;

    #[test]
    fn test_find_le_key_idx() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        node.insert(1, b"111").unwrap();
        node.insert(4, b"444444").unwrap();
        node.insert(6, b"66").unwrap();

        assert_eq!(node.find_le_key_idx(1).unwrap(), (0, true));
        assert_eq!(node.find_le_key_idx(4).unwrap(), (1, true));
        assert_eq!(node.find_le_key_idx(6).unwrap(), (2, true));

        assert_eq!(node.find_le_key_idx(0).unwrap(), (0, false));
        assert_eq!(node.find_le_key_idx(2).unwrap(), (1, false));
        assert_eq!(node.find_le_key_idx(3).unwrap(), (1, false));
        assert_eq!(node.find_le_key_idx(5).unwrap(), (2, false));
        assert_eq!(node.find_le_key_idx(7).unwrap(), (3, false));
    }

    #[test]
    fn test_insert_key_at() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        let key = Key::new(123, 0, 100, 5);
        node.insert_key_at(&key, 0).unwrap();

        let (stored_key, _) = node.read_key_at(0).unwrap();
        assert_eq!(stored_key.key.get(), 123);
        assert_eq!(stored_key.left_child_page.get(), 0);
        assert_eq!(stored_key.value_offset.get(), 100);
        assert_eq!(stored_key.value_len.get(), 5);

        let header = node.read_header().unwrap();
        assert_eq!(header.num_keys.get(), 1);
    }

    #[test]
    fn test_pop_key_from() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        node.insert(10, b"val10").unwrap();
        node.insert(20, b"val20").unwrap();
        node.insert(30, b"val30").unwrap();

        let popped_key = node.pop_key_at(1).unwrap();
        assert_eq!(popped_key.key.get(), 20);

        let header = node.read_header().unwrap();
        assert_eq!(header.num_keys.get(), 2);

        let (first_key, _) = node.read_key_at(0).unwrap();
        let (second_key, _) = node.read_key_at(1).unwrap();
        assert_eq!(first_key.key.get(), 10);
        assert_eq!(second_key.key.get(), 30);
    }

    #[test]
    fn test_insert_order() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        let key1 = Key::new(10, 0, 100, 3);
        let key2 = Key::new(30, 0, 200, 3);
        let key3 = Key::new(20, 0, 150, 3);

        node.insert_key_at(&key1, 0).unwrap();
        node.insert_key_at(&key2, 1).unwrap();
        node.insert_key_at(&key3, 1).unwrap();
        let (first_key, _) = node.read_key_at(0).unwrap();
        let (second_key, _) = node.read_key_at(1).unwrap();
        let (third_key, _) = node.read_key_at(2).unwrap();

        assert_eq!(first_key.key.get(), 10);
        assert_eq!(second_key.key.get(), 20);
        assert_eq!(third_key.key.get(), 30);

        let header = node.read_header().unwrap();
        assert_eq!(header.num_keys.get(), 3);
    }
}
