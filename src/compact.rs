use crate::stg::Storage;
use crate::{nd, page, util, Arc, Data, HashSet};
use std::cmp::min;
use std::collections::BTreeSet;

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
/// Note: for a free logical page, a special value is stored after the page size, then a link to the next free logical page.
///
/// Layout of extension page: 8 byte logical page number | user data | unused data.

pub struct CompactFile {
    /// Underlying storage.
    pub stg: Box<dyn Storage>,

    /// Size of starter page
    pub sp_size: usize,

    /// Size of extension page
    pub ep_size: usize,

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

impl CompactFile {
    /// = 28. Size of file header.
    const HSIZE: u64 = 28;
    /// Enable tracing.
    const TRACE: bool = false;
    // Special value used to validate free chain entries.
    const SPECIAL_VALUE: u64 = 0xf1e2d3c4b5a697;

    /// Get the list of free logical pages ( also verifies free chain is ok ).
    pub fn get_info(&self) -> (HashSet<u64>,u64) {
        let mut free = HashSet::default();
        let mut p = self.lp_first;
        while p != u64::MAX {
            assert!(free.insert(p));
            let lpoff = Self::HSIZE + p * self.sp_size as u64;
            assert!(self.read_u16(lpoff) == 0);
            assert!(self.stg.read_u64(lpoff + 2) == Self::SPECIAL_VALUE);
            p = self.stg.read_u64(lpoff + 10);
        }
        (free,self.lp_alloc)
    }

    fn trace(&self, msg: &str) {
        if !Self::TRACE {
            return;
        }
        print!(
            "Compactfile trace {} ep_resvd={} ep_count={} ep_free={:?} lp_alloc={} lp_free={:?} free=[",
            msg, self.ep_resvd, self.ep_count, self.ep_free, self.lp_alloc, self.lp_free
        );
        let mut p = self.lp_first;
        while p != u64::MAX {
            print!(" {}", p);
            p = self.stg.read_u64(Self::HSIZE + p * self.sp_size as u64 + 2);
        }
        println!("]");
    }

    /// Construct a new CompactFile.
    pub fn new(stg: Box<dyn Storage>, sp_size: usize, ep_size: usize) -> Self {
        let fsize = stg.size();
        let is_new = fsize == 0;
        let mut x = Self {
            sp_size,
            ep_size,
            stg,
            ep_resvd: 20,
            ep_count: 20,
            ep_free: BTreeSet::new(),
            lp_alloc: 0,
            lp_first: u64::MAX,
            lp_alloc_dirty: false,
            lp_free: BTreeSet::new(),
            is_new,
        };
        if is_new {
            x.stg.write_u64(0, x.ep_resvd);
            x.writeu16(24, x.sp_size as u16);
            x.writeu16(26, x.ep_size as u16);
            x.lp_alloc_dirty = true;
        } else {
            x.ep_resvd = x.stg.read_u64(0);
            x.lp_alloc = x.stg.read_u64(8);
            x.lp_first = x.stg.read_u64(16);
            x.sp_size = x.read_u16(24) as usize;
            x.ep_size = x.read_u16(26) as usize;
        }
        x.ep_count = (fsize + (x.ep_size as u64) - 1) / (x.ep_size as u64);
        if x.ep_count < x.ep_resvd {
            x.ep_count = x.ep_resvd;
        }
        if is_new {
            x.save();
        }
        x
    }

    /// Set the contents of the page.
    pub fn set_page(&mut self, lpnum: u64, data: Data) {
        debug_assert!(!self.lp_free.contains(&lpnum));

        self.extend_starter_pages(lpnum);
        // Calculate number of extension pages needed.
        let size = data.len();
        let ext = self.ext(size);

        // Read the current starter info.
        let foff = Self::HSIZE + (self.sp_size as u64) * lpnum;
        let old_size = self.read_u16(foff);
        assert!(old_size <= page::PAGE_SIZE);
        let mut old_ext = self.ext(old_size);

        let mut info = vec![0_u8; 2 + old_ext * 8];
        self.stg.read(foff, &mut info);

        util::set(&mut info, 0, size as u64, 2);

        if ext != old_ext {
            // Note freed pages.
            while old_ext > ext {
                old_ext -= 1;
                let fp = util::getu64(&info, 2 + old_ext * 8);
                info.resize(info.len() - 8, 0); // Important or info could over-write data later.
                self.ep_free.insert(fp);
            }
            // Allocate new pages.
            while old_ext < ext {
                let np = self.ep_alloc();
                info.resize(info.len() + 8, 0);
                util::setu64(&mut info[2 + old_ext * 8..], np);
                old_ext += 1;
            }
        }
        let off = 2 + ext * 8;
        let mut done = min(self.sp_size - off, size);
        self.stg
            .write_data(foff + off as u64, data.clone(), 0, done);

        // Write the extension pages.
        for i in 0..ext {
            let amount = min(size - done, self.ep_size - 8);
            let page = util::getu64(&info, 2 + i * 8) as u64;
            let foff = page * (self.ep_size as u64);
            self.stg.write_u64(foff, lpnum);
            self.stg.write_data(foff + 8, data.clone(), done, amount);
            done += amount;
        }
        debug_assert!(done == size);

        // Write the info.
        debug_assert!(info.len() == 2 + ext * 8);
        self.stg.write_vec(foff, info);
    }

    /// Get the current size of the specified logical page.
    pub fn page_size(&self, lpnum: u64) -> usize {
        if self.lp_valid(lpnum) {
            self.read_u16(Self::HSIZE + (self.sp_size as u64) * lpnum)
        } else {
            0
        }
    }

    /// Get logical page contents. Returns the page size.
    pub fn get_page(&self, lpnum: u64) -> Data {
        if !self.lp_valid(lpnum) {
            return nd();
        }

        let off = Self::HSIZE + (self.sp_size as u64) * lpnum;
        let mut starter = vec![0_u8; self.sp_size];
        self.stg.read(off, &mut starter);
        let size = util::get(&starter, 0, 2) as usize; // Number of bytes in logical page.
        let mut data = vec![0u8; size];
        let ext = self.ext(size); // Number of extension pages.

        // Read the starter data.
        let off = 2 + ext * 8;
        let mut done = min(size, self.sp_size - off);
        data[0..done].copy_from_slice(&starter[off..off + done]);

        // Read the extension pages.
        for i in 0..ext {
            let amount = min(size - done, self.ep_size - 8);
            let page = util::getu64(&starter, 2 + i * 8);
            let roff = page * (self.ep_size as u64);
            debug_assert!(self.stg.read_u64(roff) == lpnum);
            self.stg.read(roff + 8, &mut data[done..done + amount]);
            done += amount;
        }
        debug_assert!(done == size);
        Arc::new(data)
    }

    /// Allocate logical page number. Pages are numbered 0,1,2...
    pub fn alloc_page(&mut self) -> u64 {
        self.trace("alloc_page");
        if let Some(&p) = self.lp_free.iter().next() {
            p
        } else {
            self.lp_alloc_dirty = true;
            if self.lp_first != u64::MAX {
                let p = self.lp_first;
                let lpoff = Self::HSIZE + p * self.sp_size as u64;
                assert!(self.read_u16(lpoff) == 0);
                assert!(self.stg.read_u64(lpoff + 2) == Self::SPECIAL_VALUE);
                self.lp_first = self.stg.read_u64(lpoff + 10);
                p
            } else {
                let p = self.lp_alloc;
                self.lp_alloc += 1;
                p
            }
        }
    }

    /// Free a logical page number.
    pub fn free_page(&mut self, pnum: u64) {
        self.lp_free.insert(pnum);
        self.trace("free_page");
    }

    /// Is this a new file?
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// Resets logical page allocation to last save.
    pub fn rollback(&mut self) {
        self.trace("rollback before");
        self.lp_free.clear();
        if self.lp_alloc_dirty {
            self.lp_alloc_dirty = false;
            self.lp_alloc = self.stg.read_u64(8);
            self.lp_first = self.stg.read_u64(16);
        }
        self.trace("rollback after");
    }

    /// Process the temporary sets of free pages and write the file header.
    pub fn save(&mut self) {
        self.trace("save before");
        // Free the temporary set of free logical pages.
        for p in &std::mem::take(&mut self.lp_free) {
            let p = *p;
            // Set the page size to zero, frees any associated extension pages.
            self.set_page(p, nd());
            // Store link to old lp_first after size field.
            let lpoff = Self::HSIZE + p * self.sp_size as u64;
            self.stg.write_u64(lpoff + 2, Self::SPECIAL_VALUE); // Used to validate freee chain entries.
            self.stg.write_u64(lpoff + 10, self.lp_first);

            self.lp_first = p;
            self.lp_alloc_dirty = true;
        }

        // Relocate pages to fill any free extension pages.
        while !self.ep_free.is_empty() {
            self.ep_count -= 1;
            let from = self.ep_count;
            // If the last page is not a free page, relocate it using a free page.
            if !self.ep_free.remove(&from) {
                let to = self.ep_alloc();
                self.relocate(from, to);
            }
        }
        // Save the lp alloc values and file size.
        if self.lp_alloc_dirty {
            self.lp_alloc_dirty = false;
            self.stg.write_u64(8, self.lp_alloc);
            self.stg.write_u64(16, self.lp_first);
        }
        self.stg.commit(self.ep_count * self.ep_size as u64);
        self.trace("save after");
    }

    /// Read a u16 from the underlying file.
    fn read_u16(&self, offset: u64) -> usize {
        let mut bytes = [0; 2];
        self.stg.read(offset, &mut bytes);
        u16::from_le_bytes(bytes) as usize
    }

    /// Write a u16 to the underlying file.
    fn writeu16(&mut self, offset: u64, x: u16) {
        self.stg.write(offset, &x.to_le_bytes());
    }

    /// Relocate extension page to a new location.
    fn relocate(&mut self, from: u64, to: u64) {
        if from == to {
            return;
        }
        let mut buffer = vec![0; self.ep_size];
        self.stg.read(from * self.ep_size as u64, &mut buffer);
        self.stg.write(to * self.ep_size as u64, &buffer);
        let lpnum = util::getu64(&buffer, 0);
        // Compute location and length of the array of extension page numbers.
        let mut off = Self::HSIZE + lpnum * self.sp_size as u64;
        let size = self.read_u16(off);
        let mut ext = self.ext(size);
        off += 2;
        // Update the matching extension page number.
        loop {
            debug_assert!(ext != 0);
            let x = self.stg.read_u64(off);
            if x == from {
                self.stg.write_u64(off, to);
                break;
            }
            off += 8;
            ext -= 1;
        }
    }

    /// Clear extension page.
    fn ep_clear(&mut self, epnum: u64) {
        let buf = vec![0; self.ep_size];
        self.stg.write(epnum * self.ep_size as u64, &buf);
    }

    /// Check if logical page number is within reserved region.
    fn lp_valid(&self, lpnum: u64) -> bool {
        Self::HSIZE + (lpnum + 1) * (self.sp_size as u64) <= self.ep_resvd * (self.ep_size as u64)
    }

    /// Extend the starter page array so that lpnum is valid.
    fn extend_starter_pages(&mut self, lpnum: u64) {
        let mut save = false;
        while !self.lp_valid(lpnum) {
            self.relocate(self.ep_resvd, self.ep_count);
            self.ep_clear(self.ep_resvd);
            self.ep_resvd += 1;
            self.ep_count += 1;
            save = true;
        }
        if save {
            self.stg.write_u64(0, self.ep_resvd);
        }
    }

    /// Allocate an extension page.
    fn ep_alloc(&mut self) -> u64 {
        if let Some(pp) = self.ep_free.iter().next() {
            let p = *pp;
            self.ep_free.remove(&p);
            p
        } else {
            let p = self.ep_count;
            self.ep_count += 1;
            p
        }
    }

    /// Calculate the number of extension pages needed to store a page of given size.
    fn ext(&self, size: usize) -> usize {
        Self::ext_pages(self.sp_size, self.ep_size, size)
    }

    /// Calculate the number of extension pages needed to store a page of given size.
    pub fn ext_pages(sp_size: usize, ep_size: usize, size: usize) -> usize {
        let mut n = 0;
        if size > (sp_size - 2) {
            n = ((size - (sp_size - 2)) + (ep_size - 16 - 1)) / (ep_size - 16);
        }
        debug_assert!(2 + 16 * n + size <= sp_size + n * ep_size);
        assert!(2 + n * 8 <= sp_size);
        n
    }

    /// Check whether compressing a page is worthwhile.
    pub fn compress(sp_size: usize, ep_size: usize, size: usize, saving: usize) -> bool {
        Self::ext_pages(sp_size, ep_size, size - saving) < Self::ext_pages(sp_size, ep_size, size)
    }
} // end impl CompactFile

#[test]
pub fn test() {
    use crate::stg::MemFile;
    use rand::Rng;
    /* Idea of test is to check two CompactFiles with different parameters behave the same */

    let mut rng = rand::thread_rng();

    let s0 = Box::new(MemFile::default());
    let s1 = Box::new(MemFile::default());

    let mut cf0 = CompactFile::new(s0, 200, 1024);
    let mut cf1 = CompactFile::new(s1, 136, 1024);

    for _ in 0..1000000 {
        let n: usize = rng.gen::<usize>() % 5000;
        let p: u64 = rng.gen::<u64>() % 100;
        let b: u8 = rng.gen::<u8>();

        let d = vec![b; n];
        let d = Arc::new(d);
        cf0.set_page(p, d.clone());
        cf1.set_page(p, d.clone());

        let p: u64 = rng.gen::<u64>() % 100;
        let x = cf0.get_page(p);
        let y = cf1.get_page(p);
        assert!(x == y);

        cf0.save();
        cf1.save();
    }
}
