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

#![allow(dead_code, unused)]

// node header magic numbers
const NODE_TYPE_OFFSET: usize = 0;
const NODE_TYPE_LEN: usize = 1;

const NUM_KEYS_OFFSET: usize = 1;
const NUM_KEYS_LEN: usize = 2;

const FREE_START_OFFSET: usize = 3;
const FREE_START_LEN: usize = 2;

const FREE_END_OFFSET: usize = 5;
const FREE_END_LEN: usize = 2;

const FIRST_FREEBLOCK_OFFSET: usize = 7;
const FIRST_FREEBLOCK_LEN: usize = 2;

const FRAGMENTED_BYTES_OFFSET: usize = 9;
const FRAGMENTED_BYTES_LEN: usize = 1;

const RIGHTMOST_CHILD_OFFSET: usize = 10;
const RIGHTMOST_CHILD_LEN: usize = 4;

const KEY_LEN_OFFSET: usize = 14;
const KEY_LEN_SIZE: usize = 2;

const HEADER_LEN: usize = 16;

// key struct magic numbers
const KEY_OFFSET_TO_VAL_OFFSET: usize = 0;
const KEY_OFFSET_TO_VAL_LEN: usize = 2;

const KEY_HEADER_LEN: usize = 2;

// value struct magic numbers
const VALUE_LEFT_CHILD_OFFSET: usize = 0;
const VALUE_LEFT_CHILD_LEN: usize = 4;

const VALUE_LEN_OFFSET: usize = 4;
const VALUE_LEN_LEN: usize = 2;

const VALUE_HEADER_LEN: usize = 6;

// freeblock magic numbers
const FREEBLOCK_LEN_OFFSET: usize = 0;
const FREEBLOCK_LEN_SIZE: usize = 2;

const FREEBLOCK_NEXT_OFFSET: usize = 2;
const FREEBLOCK_NEXT_SIZE: usize = 2;

mod errors;
use errors::{BTreeError, InvalidHeaderError};

use bincode;
use serde::{Deserialize, Serialize};

pub struct BTreeNode<'a, K, V> {
    data: &'a mut [u8],
    _marker: std::marker::PhantomData<(K, V)>,
}

#[derive(Debug)]
enum NodeType {
    Internal,
    Leaf,
}

impl<'a, K, V> BTreeNode<'a, K, V>
where
    K: Ord + Serialize + for<'de> Deserialize<'de>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    fn new(data: &'a mut [u8]) -> Result<Self, BTreeError> {
        let node = Self {
            data,
            _marker: std::marker::PhantomData,
        };

        if node.data.len() < HEADER_LEN {
            return Err(BTreeError::InvalidHeader(
                InvalidHeaderError::UnexpectedData {
                    expected: HEADER_LEN,
                    actual: node.data.len(),
                },
            ));
        }
        Ok(node)
    }

    fn get_node_type(&self) -> Result<NodeType, BTreeError> {
        let type_byte = self.get_byte(0)?;
        match type_byte {
            0x00 => Ok(NodeType::Internal),
            0x01 => Ok(NodeType::Leaf),
            _ => Err(BTreeError::InvalidHeader(
                InvalidHeaderError::InvalidNodeType(type_byte),
            )),
        }
    }

    fn set_node_type(&mut self, node_type: NodeType) -> Result<(), BTreeError> {
        let type_byte = match node_type {
            NodeType::Internal => [0x00],
            NodeType::Leaf => [0x01],
        };
        debug_assert_eq!(type_byte.len(), NODE_TYPE_LEN);
        self.set_bytes(NODE_TYPE_OFFSET, &type_byte)
    }

    fn get_n_keys(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(NUM_KEYS_OFFSET)?) as usize)
    }

    fn set_n_keys(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), NUM_KEYS_LEN);
        self.set_bytes(NUM_KEYS_OFFSET, &bytes)
    }

    fn get_free_start(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(FREE_START_OFFSET)?) as usize)
    }

    fn set_free_start(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), FREE_START_LEN);
        self.set_bytes(FREE_START_OFFSET, &bytes)
    }

    fn get_free_end(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(FREE_END_OFFSET)?) as usize)
    }

    fn set_free_end(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), FREE_END_LEN);
        self.set_bytes(FREE_END_OFFSET, &bytes)
    }

    fn get_first_freeblock_offset(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(FIRST_FREEBLOCK_OFFSET)?) as usize)
    }

    fn set_first_freeblock_offset(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), FIRST_FREEBLOCK_LEN);
        self.set_bytes(FIRST_FREEBLOCK_OFFSET, &bytes)
    }

    fn get_n_fragmented_bytes(&self) -> Result<usize, BTreeError> {
        Ok(self.get_byte(FRAGMENTED_BYTES_OFFSET)? as usize)
    }

    fn set_n_fragmented_bytes(&mut self, value: u8) -> Result<(), BTreeError> {
        let bytes = u8::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), FRAGMENTED_BYTES_LEN);
        self.set_bytes(FRAGMENTED_BYTES_OFFSET, &bytes)
    }

    fn get_rightmost_child(&self) -> Result<usize, BTreeError> {
        Ok(u32::from_be_bytes(self.get_bytes(RIGHTMOST_CHILD_OFFSET)?) as usize)
    }

    fn set_rightmost_child(&mut self, value: u32) -> Result<(), BTreeError> {
        let bytes = u32::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), RIGHTMOST_CHILD_LEN);
        self.set_bytes(RIGHTMOST_CHILD_OFFSET, &bytes)
    }

    fn get_key_len(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(KEY_LEN_OFFSET)?) as usize)
    }

    fn set_key_len(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), KEY_LEN_SIZE);
        self.set_bytes(KEY_LEN_OFFSET, &bytes)
    }

    fn free_space(&self) -> Result<usize, BTreeError> {
        let fragmented_bytes = self.get_n_fragmented_bytes()?;
        let unallocated_bytes = self.unallocated_space()?;

        let mut free_block_total = 0;
        let mut next_free_block = self.get_first_freeblock_offset()?;

        while next_free_block != 0 {
            free_block_total +=
                u16::from_be_bytes(self.get_bytes(next_free_block + FREEBLOCK_LEN_OFFSET)?)
                    as usize;
            next_free_block =
                u16::from_be_bytes(self.get_bytes(next_free_block + FREEBLOCK_NEXT_OFFSET)?)
                    as usize;
        }

        Ok(fragmented_bytes + unallocated_bytes + free_block_total)
    }

    fn unallocated_space(&self) -> Result<usize, BTreeError> {
        Ok(self.get_free_end()? - self.get_free_start()?)
    }

    fn insert_cell(&mut self, key: K, value: V) -> Result<(), BTreeError> {
        todo!();
        let key_bytes =
            bincode::serialize(&key).map_err(|e| BTreeError::SerializationError(e.to_string()))?;
        if key_bytes.len() != self.get_key_len()? {
            return Err(BTreeError::UnexpectedData {
                expected: self.get_key_len()?,
                actual: key_bytes.len(),
            });
        }
        let value_bytes = bincode::serialize(&value)
            .map_err(|e| BTreeError::SerializationError(e.to_string()))?;

        let required_space =
            key_bytes.len() + value_bytes.len() + KEY_HEADER_LEN + VALUE_HEADER_LEN;

        if required_space < self.unallocated_space() {
            todo!("Handle space missing in");
        }

    }

    fn delete_cell(key: &[u8]) {}

    fn set_bytes(&mut self, offset: usize, data: &[u8]) -> Result<(), BTreeError> {
        self.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn get_bytes<const N: usize>(&self, offset: usize) -> Result<[u8; N], BTreeError> {
        let end = offset + N;
        if end > self.data.len() {
            return Err(BTreeError::InvalidHeader(
                InvalidHeaderError::UnexpectedData {
                    expected: end,
                    actual: self.data.len(),
                },
            ));
        }

        let bytes: [u8; N] = self.data[offset..end]
            .try_into()
            .expect("Unreachable. Everything is checked above");
        Ok(bytes)
    }

    fn get_byte(&self, offset: usize) -> Result<u8, BTreeError> {
        let byte_array: [u8; 1] = self.get_bytes(offset)?;
        Ok(byte_array[0])
    }
}

#[cfg(test)]
mod tests {
    const PAGE_SIZE: usize = 4096;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_invalid_header_insufficient_data() {
        let mut data = vec![0u8; HEADER_LEN - 1];
        let result = BTreeNode::<i32, String>::new(&mut data);
        assert!(
            result.is_err(),
            "Expected error for insufficient header data"
        );
    }

    #[test]
    fn node_type() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_node_type(NodeType::Leaf).unwrap();
        match node.get_node_type().unwrap() {
            NodeType::Leaf => Ok(()),
            _ => Err("Read error"),
        };

        node.set_node_type(NodeType::Internal).unwrap();
        match node.get_node_type().unwrap() {
            NodeType::Internal => Ok(()),
            _ => Err("Read error"),
        };

        Ok(())
    }

    #[test]
    fn num_cells() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_n_keys(1);
        assert_eq!(node.get_n_keys().unwrap(), 1);

        node.set_n_keys(10);
        assert_eq!(node.get_n_keys().unwrap(), 10);

        node.set_n_keys(0);
        assert_eq!(node.get_n_keys().unwrap(), 0);

        node.set_n_keys(65535);
        assert_eq!(node.get_n_keys().unwrap(), 65535);

        Ok(())
    }

    #[test]
    fn free_start() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_free_start(1);
        assert_eq!(node.get_free_start().unwrap(), 1);

        node.set_free_start(10);
        assert_eq!(node.get_free_start().unwrap(), 10);

        node.set_free_start(0);
        assert_eq!(node.get_free_start().unwrap(), 0);

        node.set_free_start(65535);
        assert_eq!(node.get_free_start().unwrap(), 65535);

        Ok(())
    }

    #[test]
    fn free_end() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_free_end(1);
        assert_eq!(node.get_free_end().unwrap(), 1);

        node.set_free_end(10);
        assert_eq!(node.get_free_end().unwrap(), 10);

        node.set_free_end(0);
        assert_eq!(node.get_free_end().unwrap(), 0);

        node.set_free_end(65535);
        assert_eq!(node.get_free_end().unwrap(), 65535);

        Ok(())
    }
    #[test]
    fn first_freeblock_offset() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_first_freeblock_offset(1);
        assert_eq!(node.get_first_freeblock_offset().unwrap(), 1);

        node.set_first_freeblock_offset(10);
        assert_eq!(node.get_first_freeblock_offset().unwrap(), 10);

        node.set_first_freeblock_offset(0);
        assert_eq!(node.get_first_freeblock_offset().unwrap(), 0);

        node.set_first_freeblock_offset(65535);
        assert_eq!(node.get_first_freeblock_offset().unwrap(), 65535);

        Ok(())
    }

    #[test]
    fn n_fragmented_bytes() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_n_fragmented_bytes(1);
        assert_eq!(node.get_n_fragmented_bytes().unwrap(), 1);

        node.set_n_fragmented_bytes(10);
        assert_eq!(node.get_n_fragmented_bytes().unwrap(), 10);

        node.set_n_fragmented_bytes(0);
        assert_eq!(node.get_n_fragmented_bytes().unwrap(), 0);

        node.set_n_fragmented_bytes(255);
        assert_eq!(node.get_n_fragmented_bytes().unwrap(), 255);

        Ok(())
    }

    #[test]
    fn rightmost_child() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_rightmost_child(1);
        assert_eq!(node.get_rightmost_child().unwrap(), 1);

        node.set_rightmost_child(10);
        assert_eq!(node.get_rightmost_child().unwrap(), 10);

        node.set_rightmost_child(0);
        assert_eq!(node.get_rightmost_child().unwrap(), 0);

        node.set_rightmost_child(65535);
        assert_eq!(node.get_rightmost_child().unwrap(), 65535);

        Ok(())
    }

    #[test]
    fn key_len() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_key_len(1);
        assert_eq!(node.get_key_len().unwrap(), 1);

        node.set_key_len(10);
        assert_eq!(node.get_key_len().unwrap(), 10);

        node.set_key_len(0);
        assert_eq!(node.get_key_len().unwrap(), 0);

        node.set_key_len(65535);
        assert_eq!(node.get_key_len().unwrap(), 65535);

        Ok(())
    }

    #[test]
    fn all_header_properties() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::<i32, String>::new(&mut data).unwrap();

        node.set_node_type(NodeType::Leaf).unwrap();

        node.set_n_keys(65535);
        node.set_free_start(65534);
        node.set_free_end(65533);
        node.set_first_freeblock_offset(65532);
        node.set_n_fragmented_bytes(200);
        node.set_rightmost_child(65521);
        node.set_key_len(64535);

        match node.get_node_type().unwrap() {
            NodeType::Leaf => Ok(()),
            _ => Err("Read error"),
        };
        assert_eq!(node.get_n_keys().unwrap(), 65535);
        assert_eq!(node.get_free_start().unwrap(), 65534);
        assert_eq!(node.get_free_end().unwrap(), 65533);
        assert_eq!(node.get_first_freeblock_offset().unwrap(), 65532);
        assert_eq!(node.get_n_fragmented_bytes().unwrap(), 200);
        assert_eq!(node.get_rightmost_child().unwrap(), 65521);
        assert_eq!(node.get_key_len().unwrap(), 64535);
        Ok(())
    }
}
