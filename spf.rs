use crate::*;
use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// Simple implementation of PageFile based directly on system file.
pub struct SimplePagedFile
{
  file: fs::File,
  page_count: u64,
  pub is_new: bool,
}

impl PagedFile for SimplePagedFile
{
  fn read_page(&mut self, pnum: u64, data: &mut [u8])
  {
    let off = pnum * PAGE_SIZE as u64;
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.read_exact(data);
  }

  fn write_page(&mut self, pnum: u64, data: &[u8], size: usize)
  {
    let off = pnum * (PAGE_SIZE as u64);
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.write(&data[0..size]);
  }

  fn alloc_page(&mut self) -> u64
  {
    let result = self.page_count;
    self.page_count = result + 1;
    result
  }

  fn free_page(&mut self, _pnum: u64) {}

  fn is_new(&self) -> bool
  {
    self.is_new
  }

  fn compress(&self, _size: usize, _saving: usize) -> bool
  {
    false
  }
}

impl SimplePagedFile
{
  pub fn new(filename: &str) -> Self
  {
    let mut file = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
    let fsize = file.seek(SeekFrom::End(0)).unwrap();
    let page_count = (fsize + (PAGE_SIZE as u64) - 1) / (PAGE_SIZE as u64);
    let is_new = page_count == 0;
    Self { file, page_count, is_new }
  }
}
