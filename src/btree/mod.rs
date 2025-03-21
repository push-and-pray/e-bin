use errors::BTreeError;
use freeblock::FREEBLOCK_SIZE;
use header::{NodeType, HEADER_SIZE};
use key::{Key, KEY_SIZE};

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
        debug_assert_eq!(page.len(), PAGE_SIZE.into());

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
        debug_assert_eq!(page.len(), PAGE_SIZE.into());

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

    fn free_space(&self) -> Result<u16, BTreeError> {
        let mut total_space = self.unallocated_space()?;
        total_space += self.read_header()?.fragmented_bytes as u16;

        let mut freeblock_offset = self.read_header()?.first_freeblock.get();
        while freeblock_offset != 0 {
            let freeblock = self.read_freeblock(freeblock_offset.into())?;
            total_space += freeblock.size.get();
            freeblock_offset = freeblock.next_freeblock.get();
        }

        Ok(total_space)
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
            self.insert_key_at(key_idx.try_into().unwrap(), key, 0, offset, value_len)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: u64) -> Result<Option<KeyValuePair>, BTreeError> {
        let (key_idx, found) = self.find_le_key_idx(key)?;
        if !found {
            return Ok(None);
        }

        let deleted_key = self.pop_key_at(key_idx as u16)?;
        let deleted_val = self
            .get_page_slice(
                deleted_key.value_offset.get() as usize,
                deleted_key.value_len.get() as usize,
            )
            .to_owned();

        // Value is at border. We dont have to care about freeblocks and just reclaim space
        if deleted_key.value_offset == self.read_header()?.free_end {
            self.mutate_header()?.free_end += deleted_key.value_len.get();
            return Ok(Some(KeyValuePair {
                key: deleted_key.key.get(),
                value: deleted_val,
            }));
        }

        if deleted_val.len() < FREEBLOCK_SIZE.into() {
            let header = self.mutate_header()?;
            header.fragmented_bytes = header
                .fragmented_bytes
                .saturating_add(deleted_val.len() as u8);
            return Ok(Some(KeyValuePair {
                key: deleted_key.key.get(),
                value: deleted_val,
            }));
        }

        // Traverse freeblock chain until suitable location is found
        let mut prev_offset: Option<u16> = None;
        let mut curr_offset: u16 = self.read_header()?.first_freeblock.get();

        while curr_offset != 0 && curr_offset < deleted_key.value_offset.get() {
            prev_offset = Some(curr_offset);
            let freeblock = self.read_freeblock(curr_offset.into())?;
            curr_offset = freeblock.next_freeblock.get();
        }

        self.write_freeblock(
            deleted_key.value_offset.get().into(),
            curr_offset,
            deleted_key.value_len.get(),
        );

        if let Some(prev) = prev_offset {
            let prev_freeblock = self.mut_freeblock(prev.into())?;
            prev_freeblock.next_freeblock = deleted_key.value_offset;
        } else {
            self.mutate_header()?.first_freeblock = deleted_key.value_offset;
        }

        Ok(Some(KeyValuePair {
            key: deleted_key.key.get(),
            value: deleted_val,
        }))
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
    use pretty_assertions::assert_eq;

    #[test]
    fn test_freespace_tracking() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();
        let initial_free = PAGE_SIZE - HEADER_SIZE;
        let mut expected_unalloc = initial_free;
        let mut expected_free_space = initial_free;

        assert_eq!(node.unallocated_space().unwrap(), expected_unalloc);
        assert_eq!(node.free_space().unwrap(), expected_free_space);

        for i in 1..=10 {
            let value = i.to_string().repeat(i as usize);
            let value_bytes = value.as_bytes();
            let value_len: u16 = value_bytes.len().try_into().unwrap();
            node.insert(i, value_bytes).unwrap();
            expected_unalloc -= KEY_SIZE + value_len;
            expected_free_space -= KEY_SIZE + value_len;
            assert_eq!(node.unallocated_space().unwrap(), expected_unalloc);
            assert_eq!(node.free_space().unwrap(), expected_free_space);
        }

        for i in 1..=10 {
            let deleted = node.delete(i).unwrap().unwrap();
            assert_eq!(deleted.key, i);
            let expected_value = i.to_string().repeat(i as usize);
            let expected_bytes = expected_value.as_bytes();
            let value_len: u16 = expected_bytes.len().try_into().unwrap();
            assert_eq!(deleted.value, expected_bytes);
            expected_free_space += KEY_SIZE + value_len;
            assert_eq!(node.free_space().unwrap(), expected_free_space);
        }
        assert_eq!(node.unallocated_space().unwrap(), 4037);
        assert_eq!(node.free_space().unwrap(), initial_free);
    }

    #[test]
    fn test_delete_nonexistent() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();
        node.insert(1, b"test").unwrap();
        assert!(node.delete(2).unwrap().is_none());
    }

    #[test]
    fn test_delete_small_value_fragmentation() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        node.insert(42, b"ab").unwrap();
        node.insert(43, b"largevalue").unwrap();

        let frag_before = node.read_header().unwrap().fragmented_bytes;
        node.delete(42).unwrap().unwrap();
        let frag_after = node.read_header().unwrap().fragmented_bytes;

        assert_eq!(frag_after, frag_before.saturating_add(2));
    }

    #[test]
    fn test_delete_border_value() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();
        node.insert(100, b"border").unwrap();
        let free_end_before = node.read_header().unwrap().free_end.get();
        let deleted = node.delete(100).unwrap().unwrap();
        let free_end_after = node.read_header().unwrap().free_end.get();
        assert_eq!(free_end_after, free_end_before + 6);
        assert_eq!(deleted.value, b"border");
    }
}
