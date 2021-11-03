//! `ManagedFile` implements `PagedFile` by storing logical pages in smaller regions of backing storage.
//!
//! Each logical page has a fixed size "starter page".
//!
//! A logical page that does not fit in the "starter page" has 1 or more "extension pages".
//!
//! Each extension page starts with it's containing logical page number ( to allow extension pages to be relocated as required ).
//!
//! When a new extension page is needed, it is allocated from the end of the file.
//!
//! When an extension page is freed, the last extension page in the file is relocated to fill it.
//!
//! If the starter page array needs to be enlarged, the first extension page is relocated to the end of the file.
//!
//! File layout: file header | starter pages | extension pages.
//!
//! Layout of starter page: 2 byte logical page size | array of 8 byte page numbers | user data | unused data.
//!
//! Layout of extension page: 8 byte logical page number | user data | unused data.

use crate::*;
use std::cmp::min;
use std::collections::BTreeSet;
use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// = 24. Size of file header.
const HSIZE: u64 = 24;

/// = 400. Size of starter page.
const SPSIZE: usize = 400;

/// = 1024. Size of extension page.
const EPSIZE: usize = 1024;

/// Maximum logical page size.
pub const LPMAX: usize = ((SPSIZE - 2) / 8) * EPSIZE;

/// Good maximum logical page size ( 20 extension pages ).
pub const LPGOODSIZE: usize = SPSIZE - 2 + 20 * (EPSIZE - 16);

pub struct ManagedFile
{
  file: fs::File,         // Underlying file (may want to use a trait instead to make module more general).
  lp_alloc: u64,          // Allocator for logical pages.
  ep_resvd: u64,          // Number of extension pages reserved for starter pages.
  ep_count: u64,          // Number of extension pages allocated.
  lp_first: u64,          // Start of linked list of free logical pages.
  is_new: bool,           // File is newly created.
  dirty: bool,            // Header needs to be saved ( alternative would be to keep copy of current saved header ).
  ep_free: BTreeSet<u64>, // Temporary set of free extension pages.
  lp_free: BTreeSet<u64>, // Temporary set of free logical pages.
}

impl ManagedFile
{
  /// Construct a new ManagedFile.
  pub fn new(filename: &str) -> Self
  {
    let mut file = OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap();
    let fsize = file.seek(SeekFrom::End(0)).unwrap();
    let ep_count = (fsize + (EPSIZE as u64) - 1) / (EPSIZE as u64);

    let is_new = ep_count == 0;
    let mut x = Self {
      file,
      lp_alloc: 0,
      ep_resvd: 0,
      ep_count,
      lp_first: 0,
      is_new,
      dirty: false,
      lp_free: BTreeSet::new(),
      ep_free: BTreeSet::new(),
    };
    if is_new
    {
      x.ep_count = 20; // About 100 starter pages ( 20 x 1k / 400 ).
      x.ep_resvd = 20;
      x.lp_first = u64::MAX;
    }
    else
    {
      x.init();
    }
    if x.ep_count < x.ep_resvd
    {
      x.ep_count = x.ep_resvd;
    }
    x
  }

  /// Initialise from file header.
  fn init(&mut self)
  {
    self.lp_alloc = self.readu64(0);
    self.ep_resvd = self.readu64(8);
    self.lp_first = self.readu64(16);
  }

  /// Read a u64 from the underlying file.
  fn readu64(&mut self, offset: u64) -> u64
  {
    let mut bytes = [0; 8];
    self.read(offset, &mut bytes);
    u64::from_le_bytes(bytes)
  }

  /// Read a u16 from the underlying file.
  fn readu16(&mut self, offset: u64) -> usize
  {
    let mut bytes = [0; 2];
    self.read(offset, &mut bytes);
    u16::from_le_bytes(bytes) as usize
  }

  /// Write a u64 to the underlying file.
  fn writeu64(&mut self, offset: u64, x: u64)
  {
    let bytes = x.to_le_bytes();
    self.write(offset, &bytes);
  }

  /// Read bytes from the underlying file.
  fn read(&mut self, off: u64, bytes: &mut [u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.read_exact(bytes);
  }

  /// Write bytes to the underlying file.
  fn write(&mut self, off: u64, bytes: &[u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.write(bytes);
  }

  /// Relocate extension page to a new location.
  fn relocate(&mut self, from: u64, to: u64)
  {
    if from == to
    {
      return;
    }

    let mut buffer = vec![0; EPSIZE];
    self.read(from * EPSIZE as u64, &mut buffer);
    self.write(to * EPSIZE as u64, &buffer);
    let lpnum = util::getu64(&buffer, 0);

    // Compute location and length of the array of extension page numbers.
    let mut off = HSIZE + lpnum * SPSIZE as u64;
    let size = self.readu16(off);
    let mut ext = calc_ext(size);
    off += 2;

    // Update the matching extension page number.
    loop
    {
      if ext == 0
      {
        panic!("Failed to find matching ep page");
      }
      let x = self.readu64(off);
      if x == from
      {
        self.writeu64(off, to);
        break;
      }
      off += 8;
      ext -= 1;
    }
  }

  /// Clear extension page.
  fn clear(&mut self, epnum: u64)
  {
    let buf = vec![0; EPSIZE];
    self.write(epnum * EPSIZE as u64, &buf);
  }

  fn lp_valid(&mut self, lpnum: u64) -> bool
  {
    HSIZE + (lpnum + 1) * (SPSIZE as u64) <= self.ep_resvd * (EPSIZE as u64)
  }

  /// Extend the starter page array so that lpnum is valid.
  fn extend_starter_pages(&mut self, lpnum: u64)
  {
    // Check if the end of the starter page array exceeds the reserved amount.
    // While it does, relocate the first extended page to the end of the file.
    while !self.lp_valid(lpnum)
    {
      self.relocate(self.ep_resvd, self.ep_count);
      self.clear(self.ep_resvd);
      self.ep_resvd += 1;
      self.ep_count += 1;
      self.dirty = true;
    }
  }

  /// Allocate an extension page.
  fn ep_alloc(&mut self) -> u64
  {
    if let Some(pp) = self.ep_free.iter().next()
    {
      let p = *pp;
      self.ep_free.remove(&p);
      p
    }
    else
    {
      let p = self.ep_count;
      self.dirty = true;
      self.ep_count += 1;
      p
    }
  }

  fn _trace(&self)
  {
    println!(
      "lp_alloc={} ep_count={} ep_resvd={}",
      self.lp_alloc, self.ep_count, self.ep_resvd
    );
  }
}

impl PagedFile for ManagedFile
{
  fn save(&mut self)
  {
    // Free the temporary set of free logical pages.
    for p in &std::mem::take(&mut self.lp_free)
    {
      let p = *p;
      self.write_page( p, &[], 0 ); // Frees any associated extension pages.
      self.writeu64(HSIZE + p * SPSIZE as u64, self.lp_first);
      self.lp_first = p;
    }

    // Relocate pages to fill any free extension pages.
    while !self.ep_free.is_empty()
    {
      self.ep_count -= 1;
      let from = self.ep_count;
      // If the last page is not a free page, relocate it using a free page.
      if !self.ep_free.remove(&from)
      {
        let to = self.ep_alloc();
        self.relocate(from, to);
      }
    }

    // Save the header and set the file size.
    if self.dirty
    {
      self.writeu64(0, self.lp_alloc);
      self.writeu64(8, self.ep_resvd);
      self.writeu64(16, self.lp_first);
      self.file.set_len(self.ep_count * EPSIZE as u64).unwrap();
      self.dirty = false;
      self._trace();
    }
  }

  /// Write size bytes of data to the specified logical page.
  fn write_page(&mut self, lpnum: u64, data: &[u8], size: usize)
  {
    assert!(size <= LPMAX);
    self.extend_starter_pages(lpnum);
    // Calculate number of extension pages needed.
    let ext = calc_ext(size);

    // Read the current starter info.
    let off: u64 = HSIZE + (SPSIZE as u64) * lpnum;
    let mut starter = vec![0_u8; SPSIZE];
    self.read(off, &mut starter);
    let old_size = util::get(&starter, 0, 2) as usize;
    let mut old_ext = calc_ext(old_size);
    util::set(&mut starter, 0, size as u64, 2);

    if ext != old_ext
    {
      // Note freed pages.
      while old_ext > ext
      {
        old_ext -= 1;
        let fp = util::getu64(&starter, 2 + old_ext * 8);
        self.ep_free.insert(fp);
      }

      // Allocate new pages.
      while old_ext < ext
      {
        let np = self.ep_alloc();
        util::setu64(&mut starter[2 + old_ext * 8..], np);
        old_ext += 1;
      }
    }

    let off = 2 + ext * 8;
    let mut done = min(SPSIZE - off, size);
    starter[off..off + done].copy_from_slice(&data[0..done]);

    // Save the starter data.
    let woff = HSIZE + (SPSIZE as u64) * lpnum;
    self.write(woff, &starter[0..off + done]);

    // Write the extension pages.
    for i in 0..ext
    {
      let amount = min(size - done, EPSIZE - 8);
      let page = util::getu64(&starter, 2 + i * 8) as u64;
      let woff = page * (EPSIZE as u64);
      self.writeu64(woff, lpnum);
      self.write(woff + 8, &data[done..done + amount]);
      done += amount;
    }
    debug_assert!(done == size);
  }

  /// Read bytes from logical page into data.
  fn read_page(&mut self, lpnum: u64, data: &mut [u8])
  {
    if !self.lp_valid(lpnum)
    {
      return;
    }

    let off = HSIZE + (SPSIZE as u64) * lpnum;
    let mut starter = vec![0_u8; SPSIZE];
    self.read(off, &mut starter);
    let size = util::get(&starter, 0, 2) as usize; // Number of bytes in logical page.
    let ext = calc_ext(size); // Number of extension pages.
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

      debug_assert!(self.readu64(roff) == lpnum);

      self.read(roff + 8, &mut data[done..done + amount]);
      done += amount;
    }
    debug_assert!(done == size);
  }

  /// Allocate logical page number.
  fn alloc_page(&mut self) -> u64
  {
    if let Some(p) = self.lp_free.iter().next()
    {
      *p
    }
    else
    {
      self.dirty = true;
      if self.lp_first != u64::MAX
      {
        let p = self.lp_first;
        self.lp_first = self.readu64(HSIZE + p * SPSIZE as u64);
        p
      }
      else
      {
        let p = self.lp_alloc;
        self.lp_alloc += 1;
        p
      }
    }
  }

  /// Free a logical page number.
  fn free_page(&mut self, pnum: u64)
  {
    self.lp_free.insert(pnum);
  }

  /// Is this a new file?
  fn is_new(&self) -> bool
  {
    self.is_new
  }

  /// Restore state back to previous save ( cannot be used once write_page has been called ).
  fn rollback(&mut self)
  {
    self.lp_free.clear();
    self.ep_free.clear();
    self.init();
    self.dirty = false;
  }

  /// Check whether compressing a page is worthwhile.
  fn compress(&self, size: usize, saving: usize) -> bool
  {
    calc_ext(size - saving) < calc_ext(size)
  }
}

/// Calculate the number of extension pages needed to store a page of given size.
fn calc_ext(size: usize) -> usize
{
  let mut n = 0;
  if size > (SPSIZE - 2)
  {
    n = ((size - (SPSIZE - 2)) + (EPSIZE - 16 - 1)) / (EPSIZE - 16);
  }
  debug_assert!(2 + 16 * n + size <= SPSIZE + n * EPSIZE);
  n
}
