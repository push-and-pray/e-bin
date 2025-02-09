/*
The Log Manager appends binary data to a file. The file consists of multiple pages which have the following format
--------------------------------------------------
| offset (4 bytes) |      free space      | data |
--------------------------------------------------

Data grows from left to right. The offset points to the end of the free data. This makes it easy for readers to read newests log first
*/

use std::io;

use crate::page::{Page, PageManager};

pub struct LogManager {
    log: PageManager,
    tail: Page,
    tail_index: usize,
    latest_lsn: i32,
    latest_flushed_lsn: i32,
}

impl Page {
    fn set_offset<T>(&mut self, offset: T)
    where
        T: TryInto<u16>,
        T::Error: std::fmt::Debug,
    {
        let offset: u16 = offset
            .try_into()
            .expect("Offset couldnt be converted to u16");
        self.mutate()[..2].copy_from_slice(&offset.to_be_bytes())
    }

    fn get_offset(&self) -> u16 {
        u16::from_be_bytes(self.read()[..2].try_into().expect("Slice is too small"))
    }
}

impl LogManager {
    pub fn new(path: &str, page_size: usize) -> Result<Self, io::Error> {
        let mut pm = PageManager::new(path, page_size)?;
        let logsize = pm.file.metadata()?.len();

        // Generate new tail if log hasnt been initialized. Else, load tail from last page
        let (tail, tail_index) = if logsize == 0 {
            let mut page = Page::new(page_size);
            page.set_offset(page_size);
            (page, 0)
        } else {
            let tail_index = pm.n_pages()? - 1;
            (pm.read_page(tail_index)?, tail_index)
        };

        Ok(Self {
            log: pm,
            tail,
            tail_index,
            latest_lsn: 0,
            latest_flushed_lsn: 0,
        })
    }

    pub fn flush_since_lsn(&mut self, lsn: i32) -> Result<(), io::Error> {
        if lsn >= self.latest_flushed_lsn {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        let result = self.log.write_page(self.tail_index, &self.tail);
        self.latest_flushed_lsn = self.latest_lsn;
        result
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), io::Error> {
        let mut offset = self.tail.get_offset() as usize;
        let freespace = offset - size_of::<u16>();

        if data.len() > (self.log.page_size - size_of::<u16>()) {
            panic!(
                "Tried writing log entry of size {} with page size {}",
                data.len(),
                self.log.page_size
            );
        };

        if freespace < data.len() {
            self.flush()?;
            self.tail = Page::new(self.log.page_size);
            self.tail_index += 1;
            self.tail.set_offset(self.log.page_size);
            offset = self.log.page_size;
        }
        let new_offset = offset - data.len();
        self.tail.mutate()[new_offset..offset].copy_from_slice(data);
        self.tail.set_offset(new_offset);
        self.latest_lsn += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;
    const PAGESIZE: usize = 8;

    #[test]
    fn offset_setter_getter() {
        let mut page = Page::new(PAGESIZE);
        page.set_offset(PAGESIZE);
        assert_eq!(page.get_offset(), PAGESIZE as u16);
    }

    #[test]
    fn init_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("logfile.bin");
        let manager = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        assert_eq!(manager.tail_index, 0);
        assert_eq!(manager.tail.get_offset(), PAGESIZE as u16);
        assert_eq!(manager.latest_lsn, 0);
        assert_eq!(manager.latest_flushed_lsn, 0);
    }

    #[test]
    fn single_write() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("logfile.bin");
        let mut lm = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        let log_data = b"A";
        lm.append(log_data).unwrap();
        assert_eq!(lm.tail.read(), &vec![0, 7, 0, 0, 0, 0, 0, 65]);
        lm.flush().unwrap();
        assert_eq!(lm.tail.read(), &vec![0, 7, 0, 0, 0, 0, 0, 65]);

        let data = lm.log.read_page(0).unwrap();
        assert_eq!(data.read(), &vec![0, 7, 0, 0, 0, 0, 0, 65]);
    }

    #[test]
    fn multiple_writes() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("logfile.bin");
        let mut lm = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        lm.append(b"A").unwrap();
        lm.append(b"B").unwrap();
        lm.append(b"C").unwrap();

        assert_eq!(lm.tail.read(), &vec![0, 5, 0, 0, 0, 67, 66, 65]);
        lm.flush().unwrap();
        assert_eq!(lm.tail.read(), &vec![0, 5, 0, 0, 0, 67, 66, 65]);
        let data = lm.log.read_page(0).unwrap();
        assert_eq!(data.read(), &vec![0, 5, 0, 0, 0, 67, 66, 65]);
    }

    #[test]
    fn rollback() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("logfile.bin");
        let mut lm = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        lm.append(b"AA").unwrap();
        lm.append(b"BB").unwrap();
        lm.append(b"CC").unwrap();
        lm.append(b"D").unwrap();

        assert_eq!(lm.tail.read(), &vec![0, 7, 0, 0, 0, 0, 0, 68]);

        let data = lm.log.read_page(0).unwrap();
        assert_eq!(data.read(), &vec![0, 2, 67, 67, 66, 66, 65, 65]);
    }

    #[test]
    fn init_non_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("logfile.bin");
        let mut lm_old = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        lm_old.append(b"AA").unwrap();
        lm_old.append(b"BB").unwrap();
        lm_old.append(b"CC").unwrap();
        lm_old.append(b"D").unwrap();
        lm_old.flush().unwrap();

        let lm_new = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();
        assert_eq!(lm_new.tail.read(), &vec![0, 7, 0, 0, 0, 0, 0, 68]);
        assert_eq!(lm_new.tail_index, 1);
    }

    #[test]
    #[should_panic]
    fn valid_append_input_size() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("logfile.bin");
        let mut lm_old = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        assert!(lm_old.append(&[65; PAGESIZE - 1]).is_err());
    }

    #[test]
    fn append_exact_allowed_size() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("logfile.bin");
        let mut lm = LogManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        lm.append(b"AAAAAA").unwrap();
        assert_eq!(lm.tail.read(), &vec![0, 2, 65, 65, 65, 65, 65, 65]);

        lm.append(b"BBBBBB").unwrap();
        assert_eq!(lm.tail.read(), &vec![0, 2, 66, 66, 66, 66, 66, 66]);
        let data = lm.log.read_page(0).unwrap();
        assert_eq!(data.read(), &vec![0, 2, 65, 65, 65, 65, 65, 65]);
    }
}
