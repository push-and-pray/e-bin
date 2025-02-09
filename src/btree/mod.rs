/*
*   OFFSET  SIZE    DESC
*   0       1       type of node (internal: 0x00, leaf: 0x01)
*   1       2       number of cells
*   3       2       offset to cells
*   5       2       offset to first freeblock. 0x00 if none
*   7       1       number of fragmented free bytes
*   8       4       rightmost child page number
*/

mod errors;
use errors::{BTreeError, InvalidHeaderError};

pub struct BTreeNode {
    data: Vec<u8>,
}

enum NodeType {
    Internal,
    Leaf,
}

impl BTreeNode {
    fn new(data: Vec<u8>) -> Result<Self, BTreeError> {
        let node = Self { data };
        let node_type = node.node_type()?;
        let required_len = match node_type {
            NodeType::Internal => 12,
            NodeType::Leaf => 8,
        };

        if node.data.len() < required_len {
            return Err(BTreeError::InvalidHeader(
                InvalidHeaderError::InsufficientData {
                    expected: required_len,
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
            .expect("This shouldn't fail because of the check above");
        Ok(bytes)
    }

    fn get_byte(&self, offset: usize) -> Result<u8, BTreeError> {
        let byte_array: [u8; 1] = self.get_bytes(offset)?;
        Ok(byte_array[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_node_type() -> Result<(), String> {
        let data = vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let node = BTreeNode::new(data).unwrap();
        match node.node_type().unwrap() {
            NodeType::Leaf => Ok(()),
            NodeType::Internal => Err("This is not a leaf".to_string()),
        }
    }
}
