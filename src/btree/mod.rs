use errors::BTreeError;
use freeblock::{Freeblock, FREEBLOCK_SIZE};
use header::{NodeType, HEADER_SIZE};
use key::{Key, KEY_SIZE};

use zerocopy::IntoBytes;

mod errors;
mod freeblock;
mod header;
mod key;

pub const PAGE_SIZE: u16 = 4096;

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

        // Traverse freeblock to find suitable spot for new freeblock
        let next_offset: u16 = self.read_header()?.first_freeblock.into();
        let next_freeblock_bytes: &mut [u8; FREEBLOCK_SIZE as usize] = self
            .get_mut_page_slice(next_offset.into(), FREEBLOCK_SIZE.into())
            .try_into()
            .expect("Shouldn't fail, sizes are hardcoded equal");
        let next_freeblock = Freeblock::intepret_mut_from_bytes(next_freeblock_bytes)?;

        todo!("");
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
