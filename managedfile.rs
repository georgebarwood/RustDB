/*

Idea:

Each page is 0 to 15 "sectors".
A page map entry for a logical page is 1..15 sector numbers.
There is a stored reverse page map [physical sector] => [logical page:ix].
When a sector is freed, we can relocate the last physical sector to the freed sector.

Initial reverse page map could be

1 -> 0:0
2 -> 1:0
3 -> 2:0

If page 2 needs two sectors, we have also
4 -> 2:1

Objection: we have to scan the entire reverse page map on startup.
*/

use crate::*;
use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

pub struct ManagedFile
{
  file: fs::File,
  logical_page_count: u64,
  first_free_logical_page: u64,
  physical_page_count: u64,
  is_new: bool,
  to_be_freed: Vec<u64>,
  to_be_allocated: Vec<u64>,
}

/* Primitives are

   alloc_page
   free_page

   read_page
   write_page
*/

impl ManagedFile
{
  pub fn new(filename: &str) -> Self
  {
    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(filename)
      .unwrap();
    let fsize = file.seek(SeekFrom::End(0)).unwrap();
    let mut physical_page_count = (fsize + (PAGE_SIZE as u64) - 1) / (PAGE_SIZE as u64);
    let is_new = physical_page_count == 0;
    if is_new
    {
      physical_page_count = 1;
    }

    let mut x = Self {
      file,
      first_free_logical_page: u64::MAX,
      logical_page_count: 0,
      physical_page_count,
      is_new,
      to_be_freed: Vec::new(),
      to_be_allocated: Vec::new(),
    };
    if !is_new
    {
      x.logical_page_count = x.readu64(0);
      x.first_free_logical_page = x.readu64(8);
    }
    x
  }

  pub fn allocate(&mut self)
  {
    for _lp in &self.to_be_allocated
    {
      // Use to_be_freed if possible.
      // Otherwise allocate new page from end of file.
    }
    for _ in &self.to_be_freed
    {}
  }

  pub fn save(&mut self)
  {
    self.writeu64(0, self.logical_page_count);
    self.writeu64(8, self.first_free_logical_page);
  }

  fn readu64(&mut self, offset: u64) -> u64
  {
    self.file.seek(SeekFrom::Start(offset)).unwrap();
    let mut bytes = [0; 8];
    let _x = self.file.read_exact(&mut bytes);
    u64::from_le_bytes(bytes)
  }

  fn writeu64(&mut self, offset: u64, x: u64)
  {
    let bytes = x.to_le_bytes();
    self.file.seek(SeekFrom::Start(offset)).unwrap();
    let _ = self.file.write(&bytes);
  }

  pub fn get_pp(&mut self, x: u64) -> u64
  {
    if x >= self.logical_page_count
    {
      // This can occur during database initialisation.
      x + 1
    }
    else
    {
      self.readu64(16 + x * 16)
    }
  }

  fn set_pp(&mut self, x: u64, to: u64)
  {
    self.writeu64(16 + x * 16, to);
  }

  fn get_lp(&mut self, x: u64) -> u64
  {
    self.readu64(24 + x * 16)
  }

  fn set_lp(&mut self, x: u64, to: u64)
  {
    self.writeu64(24 + x * 16, to);
  }

  fn move_page(&mut self, _from: u64, _to: u64) {}

  pub fn do_free(&mut self, lp: u64)
  {
    let pp = self.get_pp(lp);

    // move_pp is last physical page in file, gets relocated.
    let move_pp = self.physical_page_count - 1;
    self.physical_page_count = move_pp;

    let move_lp = self.get_lp(move_pp);
    self.move_page(move_pp, pp);

    // Update location of move_lp
    self.set_pp(move_lp, pp);
    self.set_lp(pp, move_lp);

    // Update logical page free chain.
    self.set_pp(lp, self.first_free_logical_page);
    self.first_free_logical_page = lp;
  }
}

impl PagedFile for ManagedFile
{
  fn read_page(&mut self, pnum: u64, data: &mut [u8])
  {
    let pp = self.get_pp(pnum);

    // println!( "reading page pnum={} pp={}", pnum, pp );

    let off = pp * PAGE_SIZE as u64;
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.read_exact(data);
  }

  fn write_page(&mut self, pnum: u64, data: &[u8])
  {
    self.allocate();

    let pp = self.get_pp(pnum);

    // println!( "writing page pnum={} pp={}", pnum, pp );

    let off = pp * (PAGE_SIZE as u64);
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.write(data);
  }

  fn alloc_page(&mut self) -> u64
  {
    if let Some(p) = self.to_be_freed.pop()
    {
      return p;
    }
    let lp;
    if self.first_free_logical_page == u64::MAX
    {
      lp = self.logical_page_count;
      self.logical_page_count = lp + 1;
    }
    else
    {
      lp = self.first_free_logical_page;
      self.first_free_logical_page = self.get_pp(lp);
    }
    self.to_be_allocated.push(lp);
    lp
  }

  /// Free a logical page number.
  fn free_page(&mut self, lp: u64)
  {
    self.to_be_freed.push(lp);
  }

  fn is_new(&self) -> bool
  {
    self.is_new
  }

  fn rollback(&mut self)
  {
    self.to_be_freed.clear();
    self.to_be_allocated.clear();
  }
}
