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
    page_size: usize,
}

enum NodeType {
    Internal,
    Leaf,
}

impl<'a> BTreeNode<'a> {
    fn new(data: &'a mut [u8], page_size: usize) -> Result<Self, BTreeError> {
        let node = Self { data, page_size };
        let node_type = node.node_type()?;

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

    fn node_type(&self) -> Result<NodeType, BTreeError> {
        let type_byte = self.get_byte(0)?;
        match type_byte {
            0x00 => Ok(NodeType::Internal),
            0x01 => Ok(NodeType::Leaf),
            _ => Err(BTreeError::InvalidHeader(
                InvalidHeaderError::InvalidNodeType(type_byte),
            )),
        }
    }

    fn n_cells(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(1)?) as usize)
    }

    fn cell_offset(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(3)?) as usize)
    }

    fn first_freeblock_offset(&self) -> Result<usize, BTreeError> {
        Ok(u16::from_be_bytes(self.get_bytes(5)?) as usize)
    }

    fn n_fragmented_bytes(&self) -> Result<usize, BTreeError> {
        Ok(self.get_byte(7)? as usize)
    }

    fn rightmost_child(&self) -> Result<usize, BTreeError> {
        Ok(u32::from_be_bytes(self.get_bytes(8)?) as usize)
    }

    fn free_space(&self) -> Result<usize, BTreeError> {
        let fragmented_bytes = self.n_fragmented_bytes()?;
        let unallocated_bytes = self.cell_offset()? - HEADER_SIZE;

        let free_block_total = 0;
        let next_free_block = self.first_freeblock_offset()?;
        while next_free_block != 0 {
            todo!()
        }

        Ok(fragmented_bytes + unallocated_bytes + free_block_total)
    }

    fn insert_cell(key: &[u8], value: &[u8]) {}

    fn delete_cell(key: &[u8]) {}

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
    use super::*;

    #[test]
    fn get_node_type() -> Result<(), String> {
        let mut data = vec![
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let node = BTreeNode::new(&mut data, PAGE_SIZE).unwrap();
        match node.node_type().unwrap() {
            NodeType::Leaf => Ok(()),
            NodeType::Internal => Err("This is not a leaf".to_string()),
        }
    }
}
