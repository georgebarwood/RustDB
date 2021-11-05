//! CompactFile stores logical pages in smaller regions of backing storage.

use crate::*;
use std::cmp::min; //
use std::collections::BTreeSet;

/// = 24. Size of file header.
const HSIZE: u64 = 24;

/// = 400. Size of starter page.
pub const SPSIZE: usize = 400;

/// = 1024. Size of extension page.
pub const EPSIZE: usize = 1024;

/// Maximum logical page size.
pub const LPMAX: usize = ((SPSIZE - 2) / 8) * EPSIZE;

/// Good maximum logical page size ( 20 extension pages ).
pub const LPGOODSIZE: usize = SPSIZE - 2 + 20 * (EPSIZE - 16);

/// CompactFile stores logical pages in smaller regions of backing storage.
///
/// Each logical page has a fixed size "starter page".
///
/// A logical page that does not fit in the "starter page" has 1 or more "extension pages".
///
/// Each extension page starts with it's containing logical page number ( to allow extension pages to be relocated as required ).
///
/// When a new extension page is needed, it is allocated from the end of the file.
///
/// When an extension page is freed, the last extension page in the file is relocated to fill it.
///
/// If the starter page array needs to be enlarged, the first extension page is relocated to the end of the file.
///
/// File layout: file header | starter pages | extension pages.
///
/// Layout of starter page: 2 byte logical page size | array of 8 byte page numbers | user data | unused data.
///
/// Note: for a free logical page, the link to the next free page is stored after the page size ( 0 ).
///
/// Layout of extension page: 8 byte logical page number | user data | unused data.
pub struct CompactFile
{
  /// Underlying storage.
  stg: Box<dyn Storage>,
  /// Number of extension pages reserved for starter pages.      
  ep_resvd: u64,
  /// Number of extension pages allocated.       
  ep_count: u64,
  /// Temporary set of free extension pages.         
  ep_free: BTreeSet<u64>,
  /// Allocator for logical pages.        
  lp_alloc: u64,
  /// Start of linked list of free logical pages.        
  lp_first: u64,
  /// lp allocation fields updated.
  lp_alloc_dirty: bool,
  /// Temporary set of free logical pages.
  lp_free: BTreeSet<u64>,
  /// File is newly created.         
  is_new: bool,
}

impl CompactFile
{
  /// Construct a new CompactFile.
  pub fn new(mut stg: Box<dyn Storage>) -> Self
  {
    let fsize = stg.size();
    let ep_count = (fsize + (EPSIZE as u64) - 1) / (EPSIZE as u64);

    let is_new = ep_count == 0;
    let mut x = Self {
      stg,
      ep_resvd: 12,
      ep_count,
      ep_free: BTreeSet::new(),
      lp_alloc: 0,
      lp_first: u64::MAX,
      lp_alloc_dirty: false,
      lp_free: BTreeSet::new(),
      is_new,
    };
    if x.ep_count < x.ep_resvd
    {
      x.ep_count = x.ep_resvd;
    }
    if is_new
    {
      x.writeu64(0, x.ep_resvd);
      x.lp_alloc_dirty = true;
      x.save();
    }
    else
    {
      x.ep_resvd = x.readu64(0);
      x.lp_alloc = x.readu64(8);
      x.lp_first = x.readu64(16);
      x._trace();
    }
    x
  }

  /// Set the contents of the page.
  pub fn set_page(&mut self, lpnum: u64, data: &[u8], size: usize)
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

  /// Get the current size of the specified logical page.
  pub fn page_size(&mut self, lpnum: u64) -> usize
  {
    if self.lp_valid(lpnum)
    {
      self.readu16(HSIZE + (SPSIZE as u64) * lpnum)
    }
    else
    {
      0
    }
  }

  /// Get logical page contents. Returns the page size.
  pub fn get_page(&mut self, lpnum: u64, data: &mut [u8]) -> usize
  {
    if !self.lp_valid(lpnum)
    {
      return 0;
    }

    let off = HSIZE + (SPSIZE as u64) * lpnum;
    let mut starter = vec![0_u8; SPSIZE];
    self.read(off, &mut starter);
    let size = util::get(&starter, 0, 2) as usize; // Number of bytes in logical page.

    // println!( "read_page lpnum={} size={}", lpnum, size );

    let ext = calc_ext(size); // Number of extension pages.
    let off = 2 + ext * 8;
    let mut done = min(size, SPSIZE - off);
    data[0..done].copy_from_slice(&starter[off..off + done]);

    // Read the extension pages.
    for i in 0..ext
    {
      let amount = min(size - done, EPSIZE - 8);
      let page = util::getu64(&starter, 2 + i * 8);
      let roff = page * (EPSIZE as u64);

      debug_assert!(self.readu64(roff) == lpnum);

      self.read(roff + 8, &mut data[done..done + amount]);
      done += amount;
    }
    debug_assert!(done == size);
    size
  }

  /// Allocate logical page number. Page are numbered 0,1,2... Pages can be used before they are allocated, but freed pages must not be used until they are re-allocated.
  pub fn alloc_page(&mut self) -> u64
  {
    if let Some(p) = self.lp_free.iter().next()
    {
      *p
    }
    else
    {
      self.lp_alloc_dirty = true;
      if self.lp_first != u64::MAX
      {
        let p = self.lp_first;
        self.lp_first = self.readu64(HSIZE + p * SPSIZE as u64 + 2);
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
  pub fn free_page(&mut self, pnum: u64)
  {
    self.lp_free.insert(pnum);
  }

  /// Is this a new file?
  pub fn is_new(&self) -> bool
  {
    self.is_new
  }

  /// Resets logical page allocation to last save.
  pub fn rollback(&mut self)
  {
    self.lp_free.clear();
    if self.lp_alloc_dirty
    {
      self.lp_alloc_dirty = false;
      self.lp_alloc = self.readu64(8);
      self.lp_first = self.readu64(16);
    }
  }

  /// Process the temporary sets of free pages and write the file header.
  pub fn save(&mut self)
  {
    // Free the temporary set of free logical pages.
    for p in &std::mem::take(&mut self.lp_free)
    {
      let p = *p;
      // Set the pagee size to zero, frees any associated extension pages.
      self.set_page(p, &[], 0);

      // Store link to old lp_first after size field.
      self.writeu64(HSIZE + p * SPSIZE as u64 + 2, self.lp_first);
      self.lp_first = p;
      self.lp_alloc_dirty = true;
    }

    let mut compacted = false;
    // Relocate pages to fill any free extension pages.
    while !self.ep_free.is_empty()
    {
      compacted = true;
      self.ep_count -= 1;
      let from = self.ep_count;
      // If the last page is not a free page, relocate it using a free page.
      if !self.ep_free.remove(&from)
      {
        let to = self.ep_alloc();
        self.relocate(from, to);
      }
    }

    if self.lp_alloc_dirty || compacted
    {
      self._trace();
    }

    // Save the lp alloc values and file size.
    if self.lp_alloc_dirty
    {
      self.lp_alloc_dirty = false;
      self.writeu64(8, self.lp_alloc);
      self.writeu64(16, self.lp_first);
    }
    if compacted
    {
      self.stg.truncate(self.ep_count * EPSIZE as u64);
    }
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
    self.write(offset, &x.to_le_bytes());
  }

  /// Read bytes from the underlying file.
  fn read(&mut self, off: u64, bytes: &mut [u8])
  {
    self.stg.read(off, bytes);
  }

  /// Write bytes to the underlying file.
  fn write(&mut self, off: u64, bytes: &[u8])
  {
    self.stg.write(off, bytes);
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
  fn ep_clear(&mut self, epnum: u64)
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
    let mut save = false;
    while !self.lp_valid(lpnum)
    {
      self.relocate(self.ep_resvd, self.ep_count);
      self.ep_clear(self.ep_resvd);
      self.ep_resvd += 1;
      self.ep_count += 1;
      save = true;
    }
    if save
    {
      self.writeu64(0, self.ep_resvd);
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
      self.ep_count += 1;
      p
    }
  }

  /// Print debug info.
  fn _trace(&self)
  {
    println!(
      "ep_count={} ep_resvd={} lp_alloc={} lp_first={}",
      self.ep_count, self.ep_resvd, self.lp_alloc, self.lp_first
    );
  }
} // end impl CompactFile

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

/// Check whether compressing a page is worthwhile.
pub fn compress(size: usize, saving: usize) -> bool
{
  calc_ext(size - saving) < calc_ext(size)
}

/// Underlying storage for CompactFile.
pub trait Storage
{
  fn read(&mut self, off: u64, bytes: &mut [u8]);
  fn write(&mut self, off: u64, bytes: &[u8]);
  fn truncate(&mut self, off: u64);
  fn size(&mut self) -> u64;
}

use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// Simple implementation of Storage using std::fs::File.
pub struct SimpleFileStorage
{
  pub file: fs::File,
}

impl SimpleFileStorage
{
  pub fn new(filename: &str) -> Self
  {
    Self { file: OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap() }
  }
}

impl Storage for SimpleFileStorage
{
  fn read(&mut self, off: u64, bytes: &mut [u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.read_exact(bytes);
  }

  fn write(&mut self, off: u64, bytes: &[u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.write(bytes);
  }

  fn size(&mut self) -> u64
  {
    self.file.seek(SeekFrom::End(0)).unwrap()
  }

  fn truncate(&mut self, off: u64)
  {
    self.file.set_len(off).unwrap();
  }
}
