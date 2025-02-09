#[derive(Debug)]
pub enum BTreeError {
    InvalidHeader(InvalidHeaderError),
}

#[derive(Debug)]
pub enum InvalidHeaderError {
    InvalidNodeType(u8),
    InsufficientData { expected: usize, actual: usize },
}
