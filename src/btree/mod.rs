use errors::BTreeError;
use freeblock::FREEBLOCK_SIZE;
use header::{NodeType, HEADER_SIZE};
use key::KEY_SIZE;

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

    pub fn get(&self, key: u64) -> Result<Option<&[u8]>, BTreeError> {
        let (key_idx, exists) = self.find_le_key_idx(key)?;
        if !exists {
            return Ok(None);
        }

        let key = self.read_key_at(key_idx.try_into().unwrap())?;
        Ok(Some(self.get_page_slice(
            key.value_offset.get().into(),
            key.value_len.get().into(),
        )))
    }

    pub fn defrag(&mut self) -> Result<(), BTreeError> {
        let num_keys = { self.read_header()?.num_keys.get() };

        let mut total_used = 0;
        let mut key_infos = Vec::with_capacity(num_keys.into());
        for i in 0..num_keys {
            let key_record = self.read_key_at(i)?;
            let val_len = key_record.value_len.get() as usize;
            let old_offset = key_record.value_offset.get() as usize;
            key_infos.push((i, old_offset, val_len));
            total_used += val_len;
        }

        let mut buffer = vec![0u8; total_used];
        let mut pos = 0;
        for &(_idx, old_offset, val_len) in &key_infos {
            let src_slice = self.get_page_slice(old_offset, val_len);
            buffer[pos..pos + val_len].copy_from_slice(src_slice);
            pos += val_len;
        }

        let new_free_end = PAGE_SIZE as usize - total_used;

        self.get_mut_page_slice(new_free_end, total_used)
            .copy_from_slice(&buffer);

        pos = 0;
        for &(idx, _old_offset, val_len) in &key_infos {
            let key_record = self.mut_key_at(idx)?;
            key_record.value_offset.set((new_free_end + pos) as u16);
            pos += val_len;
        }

        let header = self.mutate_header()?;
        header.free_end.set(new_free_end.try_into().unwrap());
        header.first_freeblock.set(0);
        header.fragmented_bytes = 0;

        Ok(())
    }

    pub fn insert(&mut self, key: u64, value: &[u8]) -> Result<Option<KeyValuePair>, BTreeError> {
        debug_assert!(value.len() < u16::MAX.into());
        let value_len = value.len() as u16;

        let (key_idx, exists) = self.find_le_key_idx(key)?;

        if exists {
            todo!("If exists, replace. Remember to check if there is enough space, if old val was removed")
        }

        if self.unallocated_space()? > KEY_SIZE + value_len {
            let offset = self.prepend_value(value)?;
            self.insert_key_at(key_idx.try_into().unwrap(), key, 0, offset, value_len)?;
            return Ok(None);
        }

        if self.free_space()? < KEY_SIZE + value_len {
            return Err(BTreeError::NotEnoughSpace {
                required: (KEY_SIZE + value_len).into(),
                actual: self.free_space()?.into(),
            });
        }

        let mut prev_freeblock_offset: Option<u16> = None;
        let mut current_freeblock_offset = self.read_header()?.first_freeblock.get();

        while current_freeblock_offset != 0 {
            let (freeblock_size, freeblock_next) = {
                let freeblock = self.read_freeblock(current_freeblock_offset.into())?;
                (freeblock.size.get(), freeblock.next_freeblock.get())
            };

            if freeblock_size < value_len {
                prev_freeblock_offset = Some(current_freeblock_offset);
                current_freeblock_offset = freeblock_next;
                continue;
            }
            let chosen_offset = current_freeblock_offset;

            if freeblock_size == value_len {
                if let Some(prev) = prev_freeblock_offset {
                    let prev_fb = self.mut_freeblock(prev.into())?;
                    prev_fb.next_freeblock.set(freeblock_next);
                } else {
                    let header = self.mutate_header()?;
                    header.first_freeblock.set(freeblock_next);
                }
            } else {
                let remaining_size = freeblock_size - value_len;
                if remaining_size < FREEBLOCK_SIZE {
                    {
                        let header = self.mutate_header()?;
                        header.fragmented_bytes =
                            header.fragmented_bytes.saturating_add(remaining_size as u8);
                    }
                    if let Some(prev) = prev_freeblock_offset {
                        let prev_fb = self.mut_freeblock(prev.into())?;
                        prev_fb.next_freeblock.set(freeblock_next);
                    } else {
                        let header = self.mutate_header()?;
                        header.first_freeblock.set(freeblock_next);
                    }
                } else {
                    let new_freeblock_offset = current_freeblock_offset + value_len;
                    self.write_freeblock(
                        new_freeblock_offset.into(),
                        freeblock_next,
                        remaining_size,
                    );
                    if let Some(prev) = prev_freeblock_offset {
                        let prev_fb = self.mut_freeblock(prev.into())?;
                        prev_fb.next_freeblock.set(new_freeblock_offset);
                    } else {
                        let header = self.mutate_header()?;
                        header.first_freeblock.set(new_freeblock_offset);
                    }
                }
            }

            // Use the chosen freeblock space for the value.
            self.get_mut_page_slice(chosen_offset as usize, value.len())
                .copy_from_slice(value);
            self.insert_key_at(
                key_idx.try_into().unwrap(),
                key,
                0,
                chosen_offset,
                value_len,
            )?;
            return Ok(None);
        }

        self.defrag()?;

        if self.unallocated_space()? > KEY_SIZE + value_len {
            let offset = self.prepend_value(value)?;
            self.insert_key_at(key_idx.try_into().unwrap(), key, 0, offset, value_len)?;
            Ok(None)
        } else {
            panic!("Defragging didn't give back the required space. This should have been the case, as there was enough free space just before")
        }
    }

    pub fn delete(&mut self, key: u64) -> Result<Option<KeyValuePair>, BTreeError> {
        let (key_idx, found) = self.find_le_key_idx(key)?;
        if !found {
            return Ok(None);
        }
        Ok(Some(self.delete_at_idx(key_idx)?))
    }

    fn delete_at_idx(&mut self, idx: usize) -> Result<KeyValuePair, BTreeError> {
        let deleted_key = self.pop_key_at(idx as u16)?;
        let deleted_val = self
            .get_page_slice(
                deleted_key.value_offset.get() as usize,
                deleted_key.value_len.get() as usize,
            )
            .to_owned();

        // Value is at border. We dont have to care about freeblocks and just reclaim space
        if deleted_key.value_offset == self.read_header()?.free_end {
            self.mutate_header()?.free_end += deleted_key.value_len.get();
            return Ok(KeyValuePair {
                key: deleted_key.key.get(),
                value: deleted_val,
            });
        }

        if deleted_val.len() < FREEBLOCK_SIZE.into() {
            let header = self.mutate_header()?;
            header.fragmented_bytes = header
                .fragmented_bytes
                .saturating_add(deleted_val.len() as u8);
            return Ok(KeyValuePair {
                key: deleted_key.key.get(),
                value: deleted_val,
            });
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

        Ok(KeyValuePair {
            key: deleted_key.key.get(),
            value: deleted_val,
        })
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
    fn test_load_node() {
        let mut page = [0u8; PAGE_SIZE as usize];
        {
            let _node = Node::new(&mut page).unwrap();
        }
        let node = Node::load(&mut page).unwrap();
        let header = node.read_header().unwrap();
        assert_eq!(header.node_type, NodeType::Leaf);
    }

    #[test]
    fn test_defrag_functionality() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        node.insert(10, b"value10").unwrap();
        node.insert(20, b"value20").unwrap();
        node.insert(30, b"value30").unwrap();

        node.delete(20).unwrap();

        let header_before = node.read_header().unwrap();
        assert!(header_before.fragmented_bytes > 0 || header_before.first_freeblock.get() != 0);

        node.defrag().unwrap();

        let header_after = node.read_header().unwrap();
        assert_eq!(header_after.fragmented_bytes, 0);
        assert_eq!(header_after.first_freeblock.get(), 0);

        assert_eq!(node.get(10).unwrap().unwrap(), b"value10");
        assert_eq!(node.get(30).unwrap().unwrap(), b"value30");
    }

    #[test]
    fn test_freeblock_reuse_in_insert() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        let large_value = vec![1u8; (FREEBLOCK_SIZE as usize) + 10];
        node.insert(1, &large_value).unwrap();

        let _ = node.delete(1).unwrap().unwrap();

        let small_value = vec![2u8; 5];
        node.insert(2, &small_value).unwrap();

        let retrieved = node.get(2).unwrap().unwrap();
        assert_eq!(retrieved, small_value.as_slice());
    }

    #[test]
    fn test_out_of_order_insertion_and_deletion() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        node.insert(50, b"fifty").unwrap();
        node.insert(20, b"twenty").unwrap();
        node.insert(70, b"seventy").unwrap();
        node.insert(10, b"ten").unwrap();
        node.insert(40, b"forty").unwrap();

        assert_eq!(node.get(10).unwrap().unwrap(), b"ten");
        assert_eq!(node.get(20).unwrap().unwrap(), b"twenty");
        assert_eq!(node.get(40).unwrap().unwrap(), b"forty");
        assert_eq!(node.get(50).unwrap().unwrap(), b"fifty");
        assert_eq!(node.get(70).unwrap().unwrap(), b"seventy");

        let _ = node.delete(20).unwrap().unwrap();
        let _ = node.delete(50).unwrap().unwrap();

        assert!(node.get(20).unwrap().is_none());
        assert!(node.get(50).unwrap().is_none());
        assert_eq!(node.get(10).unwrap().unwrap(), b"ten");
        assert_eq!(node.get(40).unwrap().unwrap(), b"forty");
        assert_eq!(node.get(70).unwrap().unwrap(), b"seventy");
    }

    #[test]
    fn test_complex_inserts_deletes() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        for key in 1..=20 {
            let value = key.to_string().repeat((key % 5 + 3) as usize);
            node.insert(key, value.as_bytes()).unwrap();
        }

        for key in (1..=20).filter(|k| k % 3 == 0) {
            node.delete(key).unwrap();
        }

        for key in 21..=25 {
            let value = format!("key{}", key);
            node.insert(key, value.as_bytes()).unwrap();
        }

        for key in 1..=25 {
            if key <= 20 && key % 3 == 0 {
                assert!(node.get(key).unwrap().is_none());
            } else if key <= 20 {
                let expected = key.to_string().repeat((key % 5 + 3) as usize);
                assert_eq!(node.get(key).unwrap().unwrap(), expected.as_bytes());
            } else {
                let expected = format!("key{}", key);
                assert_eq!(node.get(key).unwrap().unwrap(), expected.as_bytes());
            }
        }
    }

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

    #[test]
    fn test_get() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();
        node.insert(1, b"abekat").unwrap();
        assert_eq!(node.get(1).unwrap().unwrap(), b"abekat");
        assert_eq!(node.get(2).unwrap(), None);
    }

    #[test]
    fn test_defrag_with_multiple_freeblocks() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        // Insert and delete to create fragmentation
        for i in 1..=5 {
            let val: Vec<u8> = vec![i; 500];
            node.insert(i.into(), &val).unwrap();
        }
        node.delete(2).unwrap();
        node.delete(4).unwrap();

        let pre_defrag_space = node.free_space().unwrap();
        node.defrag().unwrap();
        let post_defrag_space = node.free_space().unwrap();

        assert_eq!(pre_defrag_space, post_defrag_space);
        assert_eq!(node.read_header().unwrap().first_freeblock.get(), 0);
    }

    #[test]
    fn test_reverse_order_insertion() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        for key in (1..=100).rev() {
            node.insert(key, &key.to_le_bytes()).unwrap();
        }

        for key in 1u64..=100 {
            let expected = key.to_le_bytes();
            assert_eq!(node.get(key).unwrap().unwrap(), expected);
        }
    }

    #[test]
    fn test_multiple_small_deletions_fragmented_bytes() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        node.insert(1, b"ab").unwrap(); // 2 bytes
        node.insert(2, b"cd").unwrap(); // 2 bytes
        node.insert(3, b"ef").unwrap(); // 2 bytes

        let _ = node.delete(1).unwrap();
        let _ = node.delete(2).unwrap();
        let _ = node.delete(3).unwrap();

        let header = node.read_header().unwrap();
        assert_eq!(header.fragmented_bytes, 4);
    }

    #[test]
    fn test_defrag_clears_fragmentation() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        node.insert(10, b"small").unwrap();
        node.insert(20, b"tiny").unwrap();
        let _ = node.delete(10).unwrap();
        let _ = node.delete(20).unwrap();

        let header_before = node.read_header().unwrap();
        assert!(header_before.fragmented_bytes > 0 || header_before.first_freeblock.get() != 0);

        node.defrag().unwrap();
        let header_after = node.read_header().unwrap();
        assert_eq!(header_after.fragmented_bytes, 0);
        assert_eq!(header_after.first_freeblock.get(), 0);
    }

    #[test]
    fn test_insert_using_freeblock_with_fragmentation() {
        let mut page = [0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).unwrap();

        {
            let header = node.mutate_header().unwrap();
            header.free_end.set(header.free_start.get() + 16);
        }

        let freeblock_offset = HEADER_SIZE + 50; // an arbitrary offset above free_start
        let freeblock_size: u16 = 12;
        {
            let header = node.mutate_header().unwrap();
            header.first_freeblock.set(freeblock_offset);
        }
        node.write_freeblock(freeblock_offset as usize, 0, freeblock_size);

        let value = vec![b'a'; 10];
        node.insert(101, &value).unwrap();

        let key_record = node.read_key_at(0).unwrap();
        assert_eq!(key_record.value_offset.get(), freeblock_offset);
        assert_eq!(key_record.value_len.get(), 10);

        let header = node.read_header().unwrap();
        assert_eq!(header.fragmented_bytes, 2);
        assert_eq!(header.first_freeblock.get(), 0);

        let stored_value = node.get(101).unwrap().unwrap();
        assert_eq!(stored_value, value.as_slice());
    }
}
