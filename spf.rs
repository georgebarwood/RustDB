use crate::*;
use std::{ fs, fs::OpenOptions, io::SeekFrom, io::Seek, io::Read, io::Write };

/// Simple implementation of PageFile based directly on system file.
pub struct SimplePagedFile
{
  file: RefCell<fs::File>,
  page_count: Cell<u64>,
  pub is_new: bool,
}

impl PagedFile for SimplePagedFile
{
  fn read_page( &self, pnum: u64, data: &mut [u8] )
  {
    let off = pnum * PAGE_SIZE as u64;
    let mut f = self.file.borrow_mut();
    f.seek( SeekFrom::Start(off) ).unwrap();
    let _x = f.read_exact( data );
  }

  fn write_page( &self, pnum: u64, data: &[u8] )
  {
    let off = pnum * ( PAGE_SIZE as u64 );
    let mut f = self.file.borrow_mut();
    f.seek( SeekFrom::Start(off) ).unwrap();
    let _x = f.write( data );
   }

  fn alloc_page( &self ) -> u64
  {
    let result = self.page_count.get();
    self.page_count.set( result + 1 );
    result
  }

  fn is_new( &self ) -> bool
  {
    self.is_new
  }
}

impl SimplePagedFile
{
  pub fn new( filename: &str ) -> Self
  {
    let mut file = OpenOptions::new().read(true).write(true).create(true).open( filename ).unwrap();
    let fsize = file.seek(SeekFrom::End(0)).unwrap();
    let mut page_count = ( fsize + (PAGE_SIZE as u64) - 1 ) / (PAGE_SIZE as u64);
    let is_new = page_count == 0;
    if is_new { page_count = 1; }
    Self{ file: RefCell::new(file), page_count: Cell::new(page_count), is_new }
  }
}
