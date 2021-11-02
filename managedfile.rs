//! ManagedFile implements PagedFile toring logical pages in smaller regions of backing storage.
//!
//! Each logical page has a fixed size "starter page".
//!
//! A logical page that does not fit in the "starter page" has 1 or more "extension pages".
//!
//! Each extension page starts with it's containing logical page number ( to allow extension pages to be relocated as required ).
//!
//! When a new extension page is needed, it is allocated from the end of the file.
//!
//! When an extension page is freed, the last extension page in the file is relocated to fill it
//!( using the lpnum stored at the start of the extension page ).
//!
//!If a new logical page is needed, the first extension page is relocated to the end of the file.
//!
//![ A list of free logical page numbers is kept in a database file ]
//!
//!File layout: file header | starter pages | extension pages.
//!
//!Layout of starter page: 2 byte logical page size | array of 8 byte page numbers | user data | unused data.
//!
//!Layout of extension page: 8 byte logical page number | user data | unused data.

use crate::*;
use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// = 24. Size of file header.
const OVERHEAD: u64 = 24;

/// = 400. Size of starter page.
const SPSIZE: usize = 400;

/// = 1024. Size of extension page.
const EPSIZE: usize = 1024;

pub struct ManagedFile
{
  file: fs::File,
  lp_count: u64, // Number of logical pages in use.
  lp_alloc: u64, // Allocator for logical pages.
  pp_resvd: u64, // Number of pages reserved for logical page area.
  pp_count: u64, // Number of pages allocated.
  is_new: bool,
}

impl ManagedFile
{
  pub fn new(filename: &str) -> Self
  {
    let mut file = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
    let fsize = file.seek(SeekFrom::End(0)).unwrap();
    let pp_count = (fsize + (EPSIZE as u64) - 1) / (EPSIZE as u64);

    let is_new = pp_count == 0;
    let mut x = Self { file, lp_count: 0, lp_alloc: 0, pp_resvd: 0, pp_count, is_new };
    if is_new
    {
      x.pp_count = 50;
      x.pp_resvd = 50;
      x.save();
    }
    else
    {
      x.lp_count = x.readu64(0);
      x.lp_alloc = x.readu64(8);
      x.pp_resvd = x.readu64(16);
    }
    if x.pp_count < x.pp_resvd
    {
      x.pp_count = x.pp_resvd;
    }
    println!(
      "lp_count={} pp_count={} pp_resvd={}",
      x.lp_count, x.pp_count, x.pp_resvd
    );
    x
  }

  pub fn save(&mut self)
  {
    self.writeu64(0, self.lp_count);
    self.writeu64(8, self.lp_alloc);
    self.writeu64(16, self.pp_resvd);
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

  fn read(&mut self, off: u64, data: &mut [u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.read_exact(data);
  }

  fn write(&mut self, off: u64, data: &[u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.write(data);
  }

  /// Calculate the number of extension pages needed to store a page of given size.
  fn calc_ext(size: usize) -> usize
  {
    let mut n = 0;
    if size > SPSIZE - 2
    {
      n = (size - SPSIZE) / (EPSIZE - 8);
      if size + (2 + n * 16) > SPSIZE + n * EPSIZE
      {
        n += 1;
      }
    }
    n
  }
}

impl PagedFile for ManagedFile
{
  fn read_page(&mut self, pnum: u64, data: &mut [u8])
  {
    if pnum >= self.lp_count
    {
      return;
    }
    let off: u64 = OVERHEAD + (SPSIZE as u64) * pnum;
    let mut starter = vec![0_u8; SPSIZE];
    self.read(off, &mut starter);
    let size = util::get(&starter, 0, 2) as usize; // Number of bytes in page.
    let ext = Self::calc_ext(size); // Number of extension pages.

    println!("read_page pnum={} size={} ext={}", pnum, size, ext);

    let off = 2 + ext * 8;
    let mut done = size;
    if done > SPSIZE - off
    {
      done = SPSIZE - off;
    }
    data[0..done].copy_from_slice(&starter[off..off + done]);

    // Read the extension pages.
    for i in 0..ext
    {
      let mut amount = size - done;
      if amount > EPSIZE - 8
      {
        amount = EPSIZE - 8;
      }
      let page = util::getu64(&starter, 2 + i * 8);
      let roff = page * (EPSIZE as u64);

      // println!( "read_page page={} done={} amount={}", page, done, amount );

      self.read(roff + 8, &mut data[done..done + amount]);
      done += amount;
    }
  }

  fn write_page(&mut self, pnum: u64, data: &[u8], size: usize)
  {
    if OVERHEAD + pnum * (SPSIZE as u64) >= self.pp_resvd * (EPSIZE as u64)
    {
      // Need to relocate page at offset self.lp_resvd.
      println!("resvd={}", self.pp_resvd);
      panic!()
    }

    if pnum >= self.lp_count
    {
      self.lp_count = pnum + 1;
      println!("lp_count={}", self.lp_count);
      self.save();
    }

    // Calculate number of extension pages needed.
    let ext = Self::calc_ext(size);

    println!("write_page pnum={} size={} ext={}", pnum, size, ext);

    // Read the starter info.
    let off: u64 = OVERHEAD + (SPSIZE as u64) * pnum;
    let mut starter = vec![0_u8; SPSIZE];
    self.read(off, &mut starter);
    let old_size = util::get(&starter, 0, 2) as usize;
    let mut old_ext = Self::calc_ext(old_size);
    util::set(&mut starter, 0, size as u64, 2);

    if ext != old_ext
    {
      if old_ext > ext
      {
        panic!()
      }
      // Need to allocate or free extension pages.
      while old_ext < ext
      {
        util::set(&mut starter, 2 + old_ext * 8, self.pp_count, 8);
        self.pp_count += 1;
        old_ext += 1;
      }
    }

    let off = 2 + ext * 8;
    let mut done = SPSIZE - off;
    if done > size
    {
      done = size;
    }
    starter[off..off + done].copy_from_slice(&data[0..done]);

    // Save the starter data.
    let woff: u64 = OVERHEAD + (SPSIZE as u64) * pnum;
    self.write(woff, &starter[0..off + done]);

    // Write the extension pages.
    for i in 0..ext
    {
      let mut amount = size - done;
      if amount > EPSIZE - 8
      {
        amount = EPSIZE - 8;
      }
      let page = util::getu64(&starter, 2 + i * 8) as u64;
      // println!( "write_page page={} done={} amount={}", page, done, amount );
      let woff = page * (EPSIZE as u64);
      self.writeu64(woff, page);
      self.write(woff + 8, &data[done..done + amount]);
      done += amount;
    }
  }

  fn alloc_page(&mut self) -> u64
  {
    let result = self.lp_alloc;
    self.lp_alloc += 1;
    self.save(); // Probably want to sasve later.
    result
  }

  /// Free a logical page number.
  fn free_page(&mut self, _lp: u64) {}

  fn is_new(&self) -> bool
  {
    self.is_new
  }

  fn rollback(&mut self) {}
}
