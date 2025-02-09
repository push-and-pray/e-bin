use core::panic;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, Read, Seek, SeekFrom};

pub struct Page {
    data: Vec<u8>,
}

impl Page {
    pub fn new(page_size: usize) -> Self {
        Self {
            data: vec![0; page_size],
        }
    }

    pub fn from_vec(data: Vec<u8>, page_size: usize) -> Self {
        if data.len() != page_size {
            panic!(
                "Tried initializing page with data size {} when page size is set to {}",
                data.len(),
                page_size
            );
        }
        Self { data }
    }

    pub fn read(&self) -> &[u8] {
        &self.data
    }

    pub fn mutate(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

pub struct PageManager {
    pub file: File,
    pub page_size: usize,
}

impl PageManager {
    pub fn new(path: &str, page_size: usize) -> Result<Self, io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)?;
        Ok(Self { file, page_size })
    }
}

impl PageManager {
    pub fn read_page(&mut self, index: usize) -> Result<Page, io::Error> {
        let mut buf = vec![0; self.page_size];
        let offset = (index * self.page_size)
            .try_into()
            .expect("usize couldn't be converted into u64");

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;

        Ok(Page::from_vec(buf, self.page_size))
    }

    pub fn write_page(&mut self, index: usize, page: &Page) -> Result<(), io::Error> {
        if page.read().len() != self.page_size {
            panic!(
                "Tried write page with size {} when page size is set to {}",
                page.read().len(),
                self.page_size
            );
        }
        let offset = (index * self.page_size)
            .try_into()
            .expect("usize couldn't be converted into u64");
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page.read())
    }

    pub fn append_page(&mut self, page: &Page) -> Result<usize, io::Error> {
        if page.read().len() != self.page_size {
            panic!(
                "Tried appending page with size {} when page size is set to {}",
                page.read().len(),
                self.page_size
            );
        }
        let filesize = self.file.metadata()?.len() as usize;
        let new_page_index = filesize / self.page_size;

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(page.read())?;

        Ok(new_page_index)
    }

    pub fn n_pages(&self) -> Result<usize, io::Error> {
        let filesize = self.file.metadata()?.len();

        assert!(filesize as usize % self.page_size == 0);
        Ok(filesize as usize / self.page_size)
    }
}

#[cfg(test)]
mod test {
    const PAGESIZE: usize = 32;
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn page_init() {
        let page = Page::new(PAGESIZE);
        assert!(page.data.iter().all(|&byte| byte == 0));
    }

    #[test]
    fn page_from_vec() {
        let page = Page::from_vec(vec![1; PAGESIZE], PAGESIZE);
        assert!(page.data.iter().all(|&byte| byte == 1));
    }

    #[test]
    #[should_panic]
    fn page_from_wrong_vec() {
        let _page = Page::from_vec(vec![1; PAGESIZE * 2], PAGESIZE);
    }

    #[test]
    fn page_read() {
        let page = Page::from_vec(vec![2; PAGESIZE], PAGESIZE);
        assert!(page.read().iter().all(|&byte| byte == 2));
    }

    #[test]
    fn page_mutate() {
        let mut mutable_page = Page::new(PAGESIZE);
        mutable_page.mutate().fill(2);
        assert!(mutable_page.read().iter().all(|&byte| byte == 2));
    }

    #[test]
    fn page_manager_read_write() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile.bin");
        let mut manager = PageManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        let page = Page::from_vec(vec![3; PAGESIZE], PAGESIZE);
        manager.write_page(0, &page).unwrap();

        let page = manager.read_page(0).unwrap();
        assert!(page.read().iter().all(|&byte| byte == 3));
    }

    #[test]
    fn page_manager_append() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile.bin");
        let mut manager = PageManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        for i in 0..=3 {
            let page = Page::from_vec(vec![i as u8; PAGESIZE], PAGESIZE);
            manager.append_page(&page).unwrap();
        }

        for i in 0..=3 {
            let page = manager.read_page(i).unwrap();
            assert!(page.read().iter().all(|&byte| byte == (i as u8)));
        }
    }

    #[test]
    fn page_manager_read_write_index() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile.bin");
        let mut manager = PageManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        for i in 0..=10 {
            let page = Page::from_vec(vec![i as u8; PAGESIZE], PAGESIZE);
            manager.write_page(i, &page).unwrap();
        }

        for i in (0..=10).rev() {
            let page = manager.read_page(i).unwrap();
            assert!(page.read().iter().all(|&byte| byte == (i as u8)));
        }
    }

    #[test]
    fn page_manager_read_empty_page() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile.bin");
        let mut manager = PageManager::new(file_path.to_str().unwrap(), PAGESIZE).unwrap();

        assert!(manager.read_page(0).is_err());

        for i in 0..3 {
            let page = Page::from_vec(vec![i as u8; PAGESIZE], PAGESIZE);
            manager.append_page(&page).unwrap();
        }

        assert!(manager.read_page(3).is_err());
    }
}
