use errors::BTreeError;
use header::{Header, NodeType, HEADER_SIZE};
use key::{Key, KEY_SIZE};
use zerocopy::little_endian::U16;
use zerocopy::{try_transmute_ref,try_transmute_mut, FromBytes, Immutable, IntoBytes, KnownLayout,};

mod errors;
mod header;
mod key;

pub const PAGE_SIZE: u16 = 4096;


#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct Freeblock {
    next_freeblock: U16,
    size: U16,
}

const FREEBLOCK_SIZE: u16 = {
    if size_of::<Freeblock>() > u16::MAX as usize {
        panic!("Freeblock size does not fit into u16");
    }
    size_of::<Freeblock>() as u16
};

impl Freeblock {
    pub fn intepret_from_bytes(bytes: &[u8; FREEBLOCK_SIZE as usize]) -> Result<&Self, BTreeError> {
        try_transmute_ref!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    pub fn intepret_mut_from_bytes(
        bytes: &mut [u8; FREEBLOCK_SIZE as usize],
    ) -> Result<&mut Self, BTreeError> {
        try_transmute_mut!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }
}

pub struct KeyValuePair {
    pub key: u64,
    pub value: Vec<u8>,
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

    pub fn load(page: &'a mut [u8]) -> Result<Self, BTreeError> {
        if page.len() != PAGE_SIZE.into() {
            return Err(BTreeError::UnexpectedData {
                expected: PAGE_SIZE.into(),
                actual: page.len(),
            });
        }

        Ok(Self { page })
    }

    fn get_page_slice(&self, offset: usize, len: usize) -> &[u8] {
        debug_assert!(
            offset.checked_add(len).unwrap() <= self.page.len(),
            "Invalid page slice: offset {} + len {} exceeds page length {}",
            offset,
            len,
            self.page.len()
        );
        &self.page[offset..(offset + len)]
    }

    fn get_mut_page_slice(&mut self, offset: usize, len: usize) -> &mut [u8] {
        debug_assert!(
            offset.checked_add(len).unwrap() <= self.page.len(),
            "Invalid page slice: offset {} + len {} exceeds page length {}",
            offset,
            len,
            self.page.len()
        );
        &mut self.page[offset..(offset + len)]
    }

    fn read_header(&self) -> Result<&Header, BTreeError> {
        let header_bytes: &[u8; HEADER_SIZE as usize] = self
            .get_page_slice(0, HEADER_SIZE as usize)
            .try_into()
            .expect("This should never fail, as the sizes are hardcoded to be the same");
        Header::intepret_from_bytes(header_bytes)
    }

    fn mutate_header(&mut self) -> Result<&mut Header, BTreeError> {
        let header_bytes: &mut [u8; HEADER_SIZE as usize] = self
            .get_mut_page_slice(0, HEADER_SIZE as usize)
            .try_into()
            .expect("This should never fail, as the sizes are hardcoded to be the same");
        Header::intepret_mut_from_bytes(header_bytes)
    }

    fn unallocated_space(&self) -> Result<u16, BTreeError> {
        let header = self.read_header()?;
        Ok(header.free_end.get() - header.free_start.get())
    }

    pub fn insert(&mut self, key: u64, value: &[u8]) -> Result<(), BTreeError> {
        debug_assert!(value.len() < u16::MAX.into());
        let value_len = value.len() as u16;

        if self.unallocated_space()? < (KEY_SIZE + value_len) {
            todo!("Handle overflow and defrag");
        }

        let (key_idx, exists) = self.find_le_key_idx(key)?;

        if exists {
            todo!();
        } else {
            let offset = self.prepend_value(value)?;
            let new_key = Key::new(key, 0, offset, value_len);
            self.insert_key_at(&new_key, key_idx.try_into().unwrap())?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: u64) -> Result<Option<KeyValuePair>, BTreeError> {
        let (key_idx, found) = self.find_le_key_idx(key)?;
        if !found {
            return Ok(None);
        }

        let deleted_key = self.pop_key_from(key_idx as u16)?;

        let deleted_val = self
            .get_page_slice(
                deleted_key.value_offset.get() as usize,
                deleted_key.value_len.get() as usize,
            )
            .to_owned();

        // Value is at border. We dont have to care about freeblocks
        if deleted_key.value_offset == self.read_header()?.free_end {
            self.mutate_header()?.free_end += deleted_key.value_len.get();
            return Ok(Some(KeyValuePair {
                key: deleted_key.key.get(),
                value: deleted_val,
            }));
        }

        // Initialize first freeblock
        if self.read_header()?.first_freeblock == 0 {
            let new_freeblock = Freeblock {
                next_freeblock: 0.into(),
                size: deleted_key.value_len,
            };

            self.get_mut_page_slice(
                deleted_key.value_offset.get() as usize,
                FREEBLOCK_SIZE as usize,
            )
            .copy_from_slice(new_freeblock.as_bytes());

            self.mutate_header()?
                .first_freeblock
                .set(deleted_key.value_offset.get());

            return Ok(Some(KeyValuePair {
                key: deleted_key.key.get(),
                value: deleted_val,
            }));
        }

        todo!("Insert freeblock into linked list of freeblocks")
    }


    fn prepend_value(&mut self, value: &[u8]) -> Result<u16, BTreeError> {
        debug_assert!(self.unallocated_space()? as usize >= value.len());
        debug_assert!(value.len() < u16::MAX as usize);

        let header = self.read_header()?;
        let free_end = header.free_end.get() as usize;
        let new_free_end = free_end - value.len();

        self.get_mut_page_slice(new_free_end, value.len())
            .copy_from_slice(value);

        let mut_header = self.mutate_header()?;
        mut_header.free_end.set(new_free_end.try_into().unwrap());
        Ok(new_free_end as u16)
    }

    // Inserts key obj at given idx, shifting others to the right. Assumes that there is space
    // available in node
    fn insert_key_at(&mut self, key: &Key, idx: u16) -> Result<(), BTreeError> {
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

    fn pop_key_from(&mut self, idx: u16) -> Result<Key, BTreeError> {
        let key_pos = self.get_key_pos(idx);
        debug_assert!(key_pos < self.read_header().unwrap().free_start.get());

        let (key_ref, _offset) = self.get_key_at(idx)?;
        let key = key_ref.clone();
        let keys_end = self.read_header()?.free_start.get() as usize;

        self.page.copy_within(
            (key_pos + KEY_SIZE) as usize..keys_end,
            key_pos as usize,
        );

        let header = self.mutate_header()?;
        header.free_start -= KEY_SIZE;
        header.num_keys -= 1;

        Ok(key)
    }

    // Returns lowest index where key < other_key is true through binary search. Bool indicates if
    // found index has key is equal to the key we are looking for
    fn find_le_key_idx(&self, key: u64) -> Result<(usize, bool), BTreeError> {
        let header = self.read_header()?;
        let num_keys = header.num_keys.get();

        if num_keys == 0 {
            return Ok((0, false));
        }

        let mut low = 0;
        let mut high = num_keys;

        while low < high {
            let mid = (low + high) / 2;
            let (key_ptr, _offset) = self.get_key_at(mid)?;
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

    fn get_key_pos(&self, index: u16) -> u16 {
        HEADER_SIZE + KEY_SIZE * index
    }

    fn get_key_at(&self, index: u16) -> Result<(&Key, usize), BTreeError> {
        let key_pos = self.get_key_pos(index) as usize;
        let key_bytes: &[u8; KEY_SIZE as usize] = self.get_page_slice(key_pos, KEY_SIZE as usize).try_into().expect("Shouldn't fail, hardcoded");
        Ok((
            Key::intepret_from_bytes(key_bytes)?,
            key_pos,
        ))
    }

    fn get_mut_key_at(&mut self, index: u16) -> Result<(&mut Key, usize), BTreeError> {
        let key_pos = self.get_key_pos(index) as usize;
        let key_bytes: &mut [u8; KEY_SIZE as usize] = self.get_mut_page_slice(key_pos, KEY_SIZE as usize).try_into().expect("Shouldn't fail, hardcoded");
        Ok((
            Key::intepret_mut_from_bytes(key_bytes)?,
            key_pos,
        ))
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

        assert_eq!(node.find_le_key_idx(1)?, (0, true));
        assert_eq!(node.find_le_key_idx(4)?, (1, true));
        assert_eq!(node.find_le_key_idx(6)?, (2, true));

        assert_eq!(node.find_le_key_idx(0)?, (0, false));
        assert_eq!(node.find_le_key_idx(2)?, (1, false));
        assert_eq!(node.find_le_key_idx(3)?, (1, false));
        assert_eq!(node.find_le_key_idx(5)?, (2, false));
        assert_eq!(node.find_le_key_idx(7)?, (3, false));

        Ok(())
    }

    #[test]
    fn insert_at() -> Result<(), BTreeError> {
        let mut page = [0x00; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page)?;

        let key = Key::new(0, 0, 0, 0);
        node.insert_key_at(&key, 0)?;

        Ok(())
    }
}
