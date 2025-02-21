use errors::BTreeError;
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub mod errors;

const PAGE_SIZE: usize = 4096;

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

#[derive(Debug, KnownLayout, FromBytes, IntoBytes, Immutable)]
#[repr(C)]
pub struct Key {
    value: U32,
    left_child_page: U32,
    offset: U16,
    len: U16,
}
const KEY_SIZE: usize = size_of::<Key>();

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

pub struct Node {
    data: [u8; PAGE_SIZE],
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

    fn read_header(&self) -> Result<&Header, BTreeError> {
        Header::try_ref_from_bytes(&self.data[0..HEADER_SIZE])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    fn mutate_header(&mut self) -> Result<&mut Header, BTreeError> {
        Header::try_mut_from_bytes(&mut self.data[0..HEADER_SIZE])
            .map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    fn unallocated_space(&self) -> Result<usize, BTreeError> {
        let header = self.read_header()?;
        Ok((header.free_end.get() - header.free_start.get()) as usize)
    }

    pub fn insert(&mut self, key: u32, value: &[u8]) -> Result<(), BTreeError> {
        let value_len = u16::try_from(value.len()).map_err(|_| BTreeError::UnexpectedData {
            expected: 65535,
            actual: value.len(),
        })?;

        if self.unallocated_space()? < KEY_SIZE + value_len as usize {
            todo!("Handle overflow");
        }

        let new_free_end = {
            let header = self.mutate_header()?;
            header.free_end -= value.len() as u16;
            header.free_end.get() as usize
        };

        self.data[new_free_end..new_free_end + value.len()].copy_from_slice(value);

        let free_start: usize = self.read_header()?.free_start.get().into();

        let new_key = Key {
            value: key.into(),
            left_child_page: 0.into(),
            offset: (new_free_end as u16).into(),
            len: value_len.into(),
        };
        let key_bytes = new_key.as_bytes();
        self.data[free_start..free_start + KEY_SIZE].copy_from_slice(key_bytes);

        let header = self.mutate_header()?;
        header.free_start += KEY_SIZE as u16;
        header.num_keys += 1;

        Ok(())
    }

    pub fn find_key(&self, key: u32) -> Result<Option<&Key>, BTreeError> {
        let mut key_cursor = HEADER_SIZE;
        let mut found_key = None;
        while key_cursor < self.read_header()?.free_start.into() {
            let key_obj = Key::ref_from_bytes(&self.data[key_cursor..key_cursor + KEY_SIZE])
                .map_err(|err| BTreeError::SerializationError(err.to_string()))?;
            if key_obj.value.get() == key {
                found_key = Some(key_obj);
                break;
            }

            key_cursor += KEY_SIZE;
        }

        Ok(found_key)
    }

    pub fn find_value(&self, key: u32) -> Result<Option<&[u8]>, BTreeError> {
        let key_ref = self.find_key(key)?;

        match key_ref {
            None => Ok(None),
            Some(k) => Ok(Some(
                &self.data[usize::from(k.offset)..usize::from(k.offset) + usize::from(k.len)],
            )),
        }
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
    fn insert_and_get() -> Result<(), BTreeError> {
        let mut node = Node::new();

        node.insert(0, b"abekat")?;

        Ok(())
    }
}
