#[derive(Debug)]
pub enum BTreeError {
    InvalidHeader(InvalidHeaderError),
    SerializationError(String),
    UnexpectedData { expected: usize, actual: usize },
    NotEnoughSpace { required: usize, actual: usize },
}

#[derive(Debug)]
pub enum InvalidHeaderError {
    InvalidNodeType(u8),
    UnexpectedData { expected: usize, actual: usize },
}
