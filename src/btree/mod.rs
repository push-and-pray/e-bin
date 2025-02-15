/*
*   Node Header
*
*   OFFSET  SIZE    DESC
*   0       1       type of node (internal: 0x00, leaf: 0x01)
*   1       2       number of cells
*   3       2       offset to cells
*   5       2       offset to first freeblock. 0x00 if none
*   7       1       number of fragmented free bytes
*   8       4       rightmost child page number
*
*   Cell struct
*
*   OFFSET  SIZE    DESC
*   0       4       left child page number
*   4       2       key len bytes (klen)
*   6       2       value len bytes (vlen)
*   8       klen    key
*   8+klen  vlen    value
*
*   Freeblock struct
*
*   OFFSET  SIZE    DESC
*   0       2       len bytes
*   2       2       offset of next freeblock, 0x0000 if last
*/

#![allow(dead_code, unused)]

const HEADER_SIZE: usize = 12;

// node header magic numbers
const NODE_TYPE_OFFSET: usize = 0;
const NODE_TYPE_SIZE: usize = 1;

const NUM_CELLS_OFFSET: usize = 1;
const NUM_CELLS_SIZE: usize = 2;

const CELL_OFFSET_OFFSET: usize = 3;
const CELL_OFFSET_SIZE: usize = 2;

const FIRST_FREEBLOCK_OFFSET: usize = 5;
const FIRST_FREEBLOCK_SIZE: usize = 2;

const FRAGMENTED_BYTES_OFFSET: usize = 7;
const FRAGMENTED_BYTES_SIZE: usize = 1;

const RIGHTMOST_CHILD_OFFSET: usize = 8;
const RIGHTMOST_CHILD_SIZE: usize = 4;

// cell magic numbers
const CELL_LEFT_CHILD_OFFSET: usize = 0;
const CELL_LEFT_CHILD_SIZE: usize = 4;

const CELL_KEY_LEN_OFFSET: usize = 4;
const CELL_KEY_LEN_SIZE: usize = 2;

const CELL_VALUE_LEN_OFFSET: usize = 6;
const CELL_VALUE_LEN_SIZE: usize = 2;

const CELL_KEY_OFFSET: usize = 8;

// freeblock magic numbers
const FREEBLOCK_LEN_OFFSET: usize = 0;
const FREEBLOCK_LEN_SIZE: usize = 2;

const FREEBLOCK_NEXT_OFFSET: usize = 2;
const FREEBLOCK_NEXT_SIZE: usize = 2;

mod errors;
use errors::{BTreeError, InvalidHeaderError};

pub struct BTreeNode<'a> {
    data: &'a mut [u8],
}

enum NodeType {
    Internal,
    Leaf,
}

impl<'a> BTreeNode<'a> {
    fn new(data: &'a mut [u8]) -> Result<Self, BTreeError> {
        let node = Self { data };

        if node.data.len() < HEADER_SIZE {
            return Err(BTreeError::InvalidHeader(
                InvalidHeaderError::InsufficientData {
                    expected: HEADER_SIZE,
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
        debug_assert_eq!(type_byte.len(), NODE_TYPE_SIZE);
        self.set_bytes(NODE_TYPE_OFFSET, &type_byte)
    }

    fn get_n_cells(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(NUM_CELLS_OFFSET)?) as usize)
    }

    fn set_n_cells(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), NUM_CELLS_SIZE);
        self.set_bytes(NUM_CELLS_OFFSET, &bytes)
    }

    fn get_cell_offset(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(CELL_OFFSET_OFFSET)?) as usize)
    }

    fn set_cell_offset(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), CELL_OFFSET_SIZE);
        self.set_bytes(CELL_OFFSET_OFFSET, &bytes)
    }

    fn get_first_freeblock_offset(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(FIRST_FREEBLOCK_OFFSET)?) as usize)
    }

    fn set_first_freeblock_offset(&mut self, value: u16) -> Result<(), BTreeError> {
        let bytes = u16::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), FIRST_FREEBLOCK_SIZE);
        self.set_bytes(FIRST_FREEBLOCK_OFFSET, &bytes)
    }

    fn get_n_fragmented_bytes(&self) -> Result<usize, BTreeError> {
        Ok(self.get_byte(FRAGMENTED_BYTES_OFFSET)? as usize)
    }

    fn set_n_fragmented_bytes(&mut self, value: u8) -> Result<(), BTreeError> {
        let bytes = u8::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), FRAGMENTED_BYTES_SIZE);
        self.set_bytes(FRAGMENTED_BYTES_OFFSET, &bytes)
    }

    fn get_rightmost_child(&self) -> Result<usize, BTreeError> {
        Ok(u32::from_be_bytes(self.get_bytes(RIGHTMOST_CHILD_OFFSET)?) as usize)
    }

    fn set_rightmost_child(&mut self, value: u32) -> Result<(), BTreeError> {
        let bytes = u32::to_be_bytes(value);
        debug_assert_eq!(bytes.len(), RIGHTMOST_CHILD_SIZE);
        self.set_bytes(RIGHTMOST_CHILD_OFFSET, &bytes)
    }

    fn free_space(&self) -> Result<usize, BTreeError> {
        let fragmented_bytes = self.get_n_fragmented_bytes()?;
        let unallocated_bytes = self.get_cell_offset()? - HEADER_SIZE;

        let mut free_block_total = 0;
        let mut next_free_block = self.get_first_freeblock_offset()?;

        while next_free_block != 0 {
            free_block_total +=
                usize::from_be_bytes(self.get_bytes(next_free_block + FREEBLOCK_LEN_OFFSET)?);
            next_free_block =
                usize::from_be_bytes(self.get_bytes(next_free_block + FREEBLOCK_NEXT_OFFSET)?);
        }

        Ok(fragmented_bytes + unallocated_bytes + free_block_total)
    }

    fn insert_cell(key: &[u8], value: &[u8]) {}

    fn delete_cell(key: &[u8]) {}

    fn set_bytes(&mut self, offset: usize, data: &[u8]) -> Result<(), BTreeError> {
        self.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn get_bytes<const N: usize>(&self, offset: usize) -> Result<[u8; N], BTreeError> {
        let end = offset + N;
        if end > self.data.len() {
            return Err(BTreeError::InvalidHeader(
                InvalidHeaderError::InsufficientData {
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
        let mut data = vec![0u8; HEADER_SIZE - 1];
        let result = BTreeNode::new(&mut data);
        assert!(
            result.is_err(),
            "Expected error for insufficient header data"
        );
    }

    #[test]
    fn get_node_type() -> Result<(), String> {
        let mut data = vec![
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let node = BTreeNode::new(&mut data).unwrap();
        match node.get_node_type().unwrap() {
            NodeType::Leaf => Ok(()),
            NodeType::Internal => Err("This is not a leaf".to_string()),
        }
    }

    #[test]
    fn num_cells() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::new(&mut data).unwrap();

        node.set_n_cells(1);
        assert_eq!(node.get_n_cells().unwrap(), 1);

        node.set_n_cells(10);
        assert_eq!(node.get_n_cells().unwrap(), 10);

        node.set_n_cells(0);
        assert_eq!(node.get_n_cells().unwrap(), 0);

        node.set_n_cells(65535);
        assert_eq!(node.get_n_cells().unwrap(), 65535);

        Ok(())
    }

    #[test]
    fn cell_offset() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::new(&mut data).unwrap();

        node.set_cell_offset(1);
        assert_eq!(node.get_cell_offset().unwrap(), 1);

        node.set_cell_offset(10);
        assert_eq!(node.get_cell_offset().unwrap(), 10);

        node.set_cell_offset(0);
        assert_eq!(node.get_cell_offset().unwrap(), 0);

        node.set_cell_offset(65535);
        assert_eq!(node.get_cell_offset().unwrap(), 65535);

        Ok(())
    }

    #[test]
    fn first_freeblock_offset() -> Result<(), String> {
        let mut data = vec![0x00; PAGE_SIZE];
        let mut node = BTreeNode::new(&mut data).unwrap();

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
        let mut node = BTreeNode::new(&mut data).unwrap();

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
        let mut node = BTreeNode::new(&mut data).unwrap();

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
}
