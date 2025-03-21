use super::errors::BTreeError;
use super::Node;

use zerocopy::little_endian::U16;
use zerocopy::{
    try_transmute_mut, try_transmute_ref, FromBytes, Immutable, IntoBytes, KnownLayout,
};

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct Freeblock {
    pub next_freeblock: U16,
    pub size: U16,
}

pub const FREEBLOCK_SIZE: u16 = {
    if size_of::<Freeblock>() > u16::MAX as usize {
        panic!("Freeblock size does not fit into u16");
    }
    size_of::<Freeblock>() as u16
};

impl Freeblock {
    pub fn intepret_from_bytes(bytes: &[u8; FREEBLOCK_SIZE as usize]) -> Result<&Self, BTreeError> {
        try_transmute_ref!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }

    pub fn intepret_mut_from_bytes(
        bytes: &mut [u8; FREEBLOCK_SIZE as usize],
    ) -> Result<&mut Self, BTreeError> {
        try_transmute_mut!(bytes).map_err(|err| BTreeError::SerializationError(err.to_string()))
    }
}

impl<'a> Node<'a> {
    pub fn read_freeblock(&self, offset: usize) -> Result<&Freeblock, BTreeError> {
        let freeblock_bytes: &[u8; FREEBLOCK_SIZE as usize] = self
            .get_page_slice(offset, FREEBLOCK_SIZE.into())
            .try_into()
            .expect("Shouldn't fail, sizes are hardcoded equal");
        Freeblock::intepret_from_bytes(freeblock_bytes)
    }

    pub fn mut_freeblock(&mut self, offset: usize) -> Result<&mut Freeblock, BTreeError> {
        let freeblock_bytes: &mut [u8; FREEBLOCK_SIZE as usize] = self
            .get_mut_page_slice(offset, FREEBLOCK_SIZE.into())
            .try_into()
            .expect("Shouldn't fail, sizes are hardcoded equal");
        Freeblock::intepret_mut_from_bytes(freeblock_bytes)
    }

    pub fn write_freeblock(&mut self, size: u16, next_freeblock: u16, offset: usize) {
        debug_assert!(
            offset >= self.read_header().unwrap().free_start.get().into(),
            "Tried writing freeblock before free space start"
        );
        let new_freeblock = Freeblock {
            next_freeblock: next_freeblock.into(),
            size: size.into(),
        };

        self.get_mut_page_slice(offset, FREEBLOCK_SIZE as usize)
            .copy_from_slice(new_freeblock.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::super::HEADER_SIZE;
    use super::super::PAGE_SIZE;
    use super::*;

    #[test]
    fn interpret_freeblock_from_bytes() {
        let freeblock = Freeblock {
            next_freeblock: 100.into(),
            size: 50.into(),
        };

        let fb_bytes = freeblock.as_bytes();
        assert_eq!(fb_bytes.len(), size_of::<Freeblock>());

        let mut arr = [0u8; FREEBLOCK_SIZE as usize];
        arr.copy_from_slice(fb_bytes);

        let fb_ref = Freeblock::intepret_from_bytes(&arr).expect("Failed to interpret bytes");
        assert_eq!(fb_ref.next_freeblock.get(), 100);
        assert_eq!(fb_ref.size.get(), 50);
    }
    #[test]
    fn interpret_mut_freeblock_from_bytes() {
        let freeblock = Freeblock {
            next_freeblock: 200.into(),
            size: 75.into(),
        };

        let fb_bytes = freeblock.as_bytes();
        let mut arr = [0u8; FREEBLOCK_SIZE as usize];
        arr.copy_from_slice(fb_bytes);

        {
            let fb_mut = Freeblock::intepret_mut_from_bytes(&mut arr)
                .expect("Failed to interpret mutable bytes");
            fb_mut.next_freeblock = 300.into();
            fb_mut.size = 150.into();
        }

        let fb_ref = Freeblock::intepret_from_bytes(&arr).expect("Failed to reinterpret bytes");
        assert_eq!(fb_ref.next_freeblock.get(), 300);
        assert_eq!(fb_ref.size.get(), 150);
    }

    #[test]
    fn node_read_and_mut_freeblock() {
        let mut page = vec![0u8; PAGE_SIZE as usize];
        let mut node = Node::new(&mut page).expect("Failed to load node");

        let valid_offset = HEADER_SIZE as usize + 100;
        let expected_size = 123;
        let expected_next = 456;

        node.write_freeblock(expected_size, expected_next, valid_offset);
        let freeblock = node
            .read_freeblock(valid_offset)
            .expect("Failed to read freeblock");

        assert_eq!(freeblock.size.get(), expected_size);
        assert_eq!(freeblock.next_freeblock.get(), expected_next);
    }
}
