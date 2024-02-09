use crate::{nd, util, Arc, Data, Storage};
use std::cmp::min;
use std::collections::BTreeSet;

/// CompactFile stores logical pages in smaller regions of backing storage.
///
/// Each logical page has a fixed size "starter page".
///
/// A logical page that does not fit in the "starter page" has 1 or more "extension pages".
///
/// Each extension page starts with its logical page number, to allow extension pages to be relocated as required.
///
/// When a new extension page is needed, it is allocated from the end of the file.
///
/// When an extension page is freed, the last extension page in the file is relocated to fill it.
///
/// The starter page section is extended as required when a logical page is written by relocating the first extension page to the end of the file.
///
/// File layout: file header | starter pages | extension pages.
///
/// Layout of starter page: 2 byte logical page size | array of 8 byte page numbers | user data | unused data.
///
/// Layout of extension page: 8 byte logical page number | user data | unused data.
///
/// All pages ( whether allocated or not ) initially have size zero.
///
/// Pages are allocated by simply incrementing lp_alloc, so sizes in the starter page section must be pre-initialised to zero when it is extended or after a renumber operation.

pub struct CompactFile {
    /// Underlying storage.
    pub stg: Box<dyn Storage>,

    /// Size of starter page
    pub(crate) sp_size: usize,

    /// Size of extension page
    pub(crate) ep_size: usize,

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

    /// Temporary set of free logical pages.
    lp_free: BTreeSet<u64>,

    /// Starter page with list of free logical pages.
    fsp: FreeStarterPage,

    /// File is newly created.         
    is_new: bool,

    /// Header fields (ep_count, lp_alloc, lp_first) modified.
    header_dirty: bool,
}

/// = 44. Size of file header.
const HSIZE: u64 = 44;

impl CompactFile {
    // Magic value to ensure file is correct format.
    const MAGIC_VALUE: [u8; 8] = *b"RDBF1001";

    /// Construct a new CompactFile.
    pub fn new(stg: Box<dyn Storage>, sp_size: usize, ep_size: usize) -> Self {
        let fsize = stg.size();
        let is_new = fsize == 0;
        let mut x = Self {
            sp_size,
            ep_size,
            stg,
            ep_resvd: 10,
            ep_count: 10,
            ep_free: BTreeSet::new(),
            lp_alloc: 0,
            lp_first: u64::MAX,
            lp_free: BTreeSet::new(),
            is_new,
            header_dirty: false,
            fsp: FreeStarterPage::new(),
        };
        let magic: u64 = crate::util::getu64(&Self::MAGIC_VALUE, 0);
        if is_new {
            x.stg.write_u64(0, magic);
            x.write_header();
            x.write_ep_resvd();
            x.write_u16(40, x.sp_size as u16);
            x.write_u16(42, x.ep_size as u16);
        } else {
            assert!(
                x.stg.read_u64(0) == magic,
                "Database File Invalid (maybe wrong version)"
            );
            x.read_header();
            x.ep_resvd = x.stg.read_u64(32);
            x.sp_size = x.read_u16(40);
            x.ep_size = x.read_u16(42);
        }
        if is_new {
            x.save();
        }
        x
    }

    fn read_header(&mut self) {
        self.ep_count = self.stg.read_u64(8);
        self.lp_alloc = self.stg.read_u64(16);
        self.lp_first = self.stg.read_u64(24);
    }

    fn write_header(&mut self) {
        self.stg.write_u64(8, self.ep_count);
        self.stg.write_u64(16, self.lp_alloc);
        self.stg.write_u64(24, self.lp_first);
    }

    fn write_ep_resvd(&mut self) {
        self.stg.write_u64(32, self.ep_resvd);
    }

    /// Get the current size of the specified logical page. Note: not valid for a newly allocated page until it is first written.
    pub fn lp_size(&self, lpnum: u64) -> usize {
        let off = self.lp_off(lpnum);
        if off != 0 {
            self.read_u16(off)
        } else {
            0
        }
    }

    /// Set the contents of the page.
    pub fn set_page(&mut self, lpnum: u64, data: Data) {
        debug_assert!(!self.lp_free.contains(&lpnum));

        self.extend_starter_pages(lpnum);
        // Calculate number of extension pages needed.
        let size = data.len();
        let ext = self.ext(size);

        // Read the current starter info.
        let foff = HSIZE + (self.sp_size as u64) * lpnum;
        let old_size = self.read_u16(foff);
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

        // Write the extension pages.
        let mut done = 0;
        for i in 0..ext {
            let amount = min(size - done, self.ep_size - 8);
            let page = util::getu64(&info, 2 + i * 8);
            let foff = page * (self.ep_size as u64);
            self.stg.write_u64(foff, lpnum);
            self.stg.write_data(foff + 8, data.clone(), done, amount);
            done += amount;
        }

        info.resize(self.sp_size, 0);

        // Save any remaining data using unused portion of starter page.
        let amount = size - done;
        if amount > 0 {
            let off = 2 + ext * 8;
            info[off..off + amount].copy_from_slice(&data[done..size]);
        }

        // Write the info.
        self.stg.write_vec(foff, info);
    }

    /// Get logical page contents.
    pub fn get_page(&self, lpnum: u64) -> Data {
        let foff = self.lp_off(lpnum);
        if foff == 0 {
            return nd();
        }
        let mut starter = vec![0_u8; self.sp_size];
        self.stg.read(foff, &mut starter);
        let size = util::get(&starter, 0, 2) as usize; // Number of bytes in logical page.
        let mut data = vec![0u8; size];
        let ext = self.ext(size); // Number of extension pages.

        // Read the extension pages.
        let mut done = 0;
        for i in 0..ext {
            let amount = min(size - done, self.ep_size - 8);
            let page = util::getu64(&starter, 2 + i * 8);
            let roff = page * (self.ep_size as u64);
            debug_assert!(self.stg.read_u64(roff) == lpnum);
            self.stg.read(roff + 8, &mut data[done..done + amount]);
            done += amount;
        }

        let amount = size - done;
        if amount > 0 {
            let off = 2 + ext * 8;
            data[done..size].copy_from_slice(&starter[off..off + amount]);
        }

        Arc::new(data)
    }

    /// Allocate logical page number. Pages are numbered 0,1,2...
    pub fn alloc_page(&mut self) -> u64 {
        if let Some(p) = self.lp_free.pop_first() {
            p
        } else {
            let mut p = self.lp_first;
            if p != u64::MAX {
                self.load_fsp(p);
                if self.fsp.count > 1 {
                    p = self.fsp.pop();
                } else {
                    self.lp_first = self.fsp.pop();
                    self.header_dirty = true;
                    self.fsp.dirty = false;
                }
            } else {
                p = self.lp_alloc;
                self.lp_alloc += 1;
                self.header_dirty = true;
            }
            p
        }
    }

    /// Free a logical page number.
    pub fn free_page(&mut self, pnum: u64) {
        self.lp_free.insert(pnum);
    }

    /// Is this a new file?
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// Resets logical page allocation to last save.
    pub fn rollback(&mut self) {
        self.lp_free.clear();
        self.read_header();
        self.fsp.clear(u64::MAX);
    }

    fn perm_free(&mut self, p: u64) {
        if self.lp_first == u64::MAX {
            self.fsp.clear(p);
            self.fsp.push(u64::MAX);
            self.lp_first = p;
            self.header_dirty = true;
        } else {
            self.load_fsp(self.lp_first);
            if !self.fsp.full() {
                self.fsp.push(p);
            } else {
                self.save_fsp();
                self.fsp.clear(p);
                self.fsp.push(self.lp_first);
                self.lp_first = p;
                self.header_dirty = true;
            }
        }
    }

    /// Process the temporary sets of free pages and write the file header.
    pub fn save(&mut self) {
        // Free the temporary set of free logical pages.
        let flist = std::mem::take(&mut self.lp_free);
        for p in flist.iter().rev() {
            let p = *p;
            // Set the page size to zero, frees any associated extension pages.
            self.set_page(p, nd());
            self.perm_free(p);
        }
        // Relocate pages to fill any free extension pages.
        while !self.ep_free.is_empty() {
            self.ep_count -= 1;
            self.header_dirty = true;
            let from = self.ep_count;
            // If the last page is not a free page, relocate it using a free page.
            if !self.ep_free.remove(&from) {
                let to = self.ep_alloc();
                self.relocate(from, to);
            }
        }
        self.save_fsp();
        if self.header_dirty {
            self.write_header();
            self.header_dirty = false;
        }
        self.stg.commit(self.ep_count * self.ep_size as u64);
    }

    /// Read a u16 from the underlying file.
    fn read_u16(&self, offset: u64) -> usize {
        let mut bytes = [0; 2];
        self.stg.read(offset, &mut bytes);
        u16::from_le_bytes(bytes) as usize
    }

    /// Write a u16 to the underlying file.
    fn write_u16(&mut self, offset: u64, x: u16) {
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
        assert!(lpnum < self.lp_alloc);
        // Compute location and length of the array of extension page numbers.
        let mut off = HSIZE + lpnum * self.sp_size as u64;
        let size = self.read_u16(off);
        let mut ext = self.ext(size);
        off += 2;
        // Update the matching extension page number.
        loop {
            if ext == 0 {
                panic!("relocate failed to find matching extension page lpnum={lpnum} from={from}");
            }
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

    /// Get offset of starter page ( returns zero if not in reserved region ).
    fn lp_off(&self, lpnum: u64) -> u64 {
        let sp_size = self.sp_size as u64;
        let mut off = HSIZE + lpnum * sp_size;
        if off + sp_size > self.ep_resvd * (self.ep_size as u64) {
            off = 0;
        }
        off
    }

    /// Extend the starter page array so that lpnum is valid.
    fn extend_starter_pages(&mut self, lpnum: u64) {
        let mut save = false;
        while self.lp_off(lpnum) == 0 {
            if !self.ep_free.remove(&self.ep_resvd)
            // Do not relocate a free extended page.
            {
                self.relocate(self.ep_resvd, self.ep_count);
                self.ep_count += 1;
                self.header_dirty = true;
            }

            self.ep_clear(self.ep_resvd);
            self.ep_resvd += 1;

            save = true;
        }
        if save {
            self.write_ep_resvd();
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
            self.header_dirty = true;
            p
        }
    }

    /// Calculate the number of extension pages needed to store a page of given size.
    fn ext(&self, size: usize) -> usize {
        Self::ext_pages(self.sp_size, self.ep_size, size)
    }

    /// Calculate the number of extension pages needed to store a page of given size.
    fn ext_pages(sp_size: usize, ep_size: usize, size: usize) -> usize {
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

    #[cfg(feature = "verify")]
    /// Get the set of free logical pages ( also verifies free chain is ok ).
    pub fn get_info(&mut self) -> (crate::HashSet<u64>, u64) {
        let mut free = crate::HashSet::default();
        let mut p = self.lp_first;
        while p != u64::MAX {
            assert!(free.insert(p));
            self.load_fsp(p);
            for i in 1..self.fsp.count {
                let p = self.fsp.get(i);
                assert!(free.insert(p));
            }
            p = self.fsp.get(0);
        }
        (free, self.lp_alloc)
    }

    #[cfg(feature = "renumber")]
    /// Load free pages into lp_free, preparation for page renumbering. Returns number of used pages or None if there are no free pages.
    pub fn load_free_pages(&mut self) -> Option<u64> {
        assert!(self.ep_free.is_empty());
        let mut p = self.lp_first;
        if p == u64::MAX {
            return None;
        }
        while p != u64::MAX {
            self.load_fsp(p);
            for i in 1..self.fsp.count {
                let p = self.fsp.get(i);
                self.free_page(p);
            }
            self.free_page(p);
            p = self.fsp.get(0);
        }
        self.lp_first = u64::MAX;
        self.header_dirty = true;
        Some(self.lp_alloc - self.lp_free.len() as u64)
    }

    #[cfg(feature = "renumber")]
    /// Efficiently move the data associated with lpnum to new logical page.
    pub fn renumber(&mut self, lpnum: u64) -> u64 {
        let lpnum2 = self.alloc_page();
        let foff = self.lp_off(lpnum);
        if foff != 0 {
            let mut starter = vec![0_u8; self.sp_size];
            self.stg.read(foff, &mut starter);
            let size = util::get(&starter, 0, 2) as usize; // Number of bytes in logical page.
            let ext = self.ext(size); // Number of extension pages.

            // Modify the extension pages.
            for i in 0..ext {
                let page = util::getu64(&starter, 2 + i * 8);
                let woff = page * (self.ep_size as u64);
                debug_assert!(self.stg.read_u64(woff) == lpnum);
                self.stg.write_u64(woff, lpnum2);
            }

            // Write the starter data.
            let foff2 = HSIZE + (self.sp_size as u64) * lpnum2;
            self.stg.write_vec(foff2, starter);
        }
        lpnum2
    }

    #[cfg(feature = "renumber")]
    fn reduce_starter_pages(&mut self, target: u64) {
        let resvd = HSIZE + target * self.sp_size as u64;
        let resvd = (resvd + self.ep_size as u64 - 1) / self.ep_size as u64;
        while self.ep_resvd > resvd {
            self.ep_count -= 1;
            self.header_dirty = true;
            let from = self.ep_count;
            self.ep_resvd -= 1;
            self.relocate(from, self.ep_resvd);
        }
        self.write_ep_resvd();
    }

    #[cfg(feature = "renumber")]
    /// All lpnums >= target must have been renumbered to be < target at this point.
    pub fn set_lpalloc(&mut self, target: u64) {
        assert!(self.lp_first == u64::MAX);
        assert!(self.ep_free.is_empty());
        self.reduce_starter_pages(target);
        self.lp_alloc = target;
        self.header_dirty = true;
        self.lp_free.clear();
        self.clear_lp();
    }

    #[cfg(feature = "renumber")]
    /// Set size of renumbered pages >= lp_alloc to zero.
    fn clear_lp(&mut self) {
        let start = HSIZE + (self.sp_size as u64) * self.lp_alloc;
        let end = self.ep_resvd * self.ep_size as u64;
        if end > start {
            let buf = vec![0; (end - start) as usize];
            self.stg.write(start, &buf);
        }
    }

    fn load_fsp(&mut self, lpnum: u64) {
        if lpnum != self.fsp.current {
            self.save_fsp();
            let off = HSIZE + 2 + lpnum * self.sp_size as u64;
            self.stg.read(off, &mut self.fsp.data);
            self.fsp.init();
            self.fsp.current = lpnum;
        }
    }

    fn save_fsp(&mut self) {
        if self.fsp.dirty {
            self.fsp.terminate();
            let off = HSIZE + 2 + self.fsp.current * self.sp_size as u64;
            self.stg.write(off, &self.fsp.data);
            self.fsp.dirty = false;
        }
    }
} // end impl CompactFile

struct FreeStarterPage {
    current: u64,
    count: usize,
    data: [u8; 64],
    dirty: bool,
}

impl FreeStarterPage {
    fn new() -> Self {
        Self {
            count: 0,
            data: [0; 64],
            dirty: false,
            current: u64::MAX,
        }
    }

    fn full(&self) -> bool {
        self.count == 8
    }

    fn push(&mut self, lpnum: u64) {
        assert!(self.count < 8);
        self.set(self.count, lpnum);
        self.count += 1;
        self.dirty = true;
    }

    fn pop(&mut self) -> u64 {
        assert!(self.count > 0);
        self.count -= 1;
        self.dirty = true;
        self.get(self.count)
    }

    fn terminate(&mut self) {
        if self.count < 8 {
            self.set(self.count, u64::MAX - 2);
        }
    }

    fn init(&mut self) {
        self.count = 0;
        while self.count < 8 && self.get(self.count) != u64::MAX - 2 {
            self.count += 1;
        }
        self.dirty = false;
    }

    fn get(&self, ix: usize) -> u64 {
        let off = ix * 8;
        util::getu64(&self.data, off)
    }

    fn set(&mut self, ix: usize, lpnum: u64) {
        let off = ix * 8;
        util::setu64(&mut self.data[off..off + 8], lpnum);
    }

    fn clear(&mut self, current: u64) {
        self.data.fill(0);
        self.current = current;
        self.count = 0;
        self.dirty = false;
    }
}

#[test]
pub fn test() {
    use crate::{AtomicFile, MemFile};
    use rand::Rng;
    /* Idea of test is to check two CompactFiles with different parameters behave the same */

    let mut rng = rand::thread_rng();

    let s0 = AtomicFile::new(MemFile::new(), MemFile::new());
    let s1 = AtomicFile::new(MemFile::new(), MemFile::new());

    let mut cf0 = CompactFile::new(s0, 200, 512);
    let mut cf1 = CompactFile::new(s1, 136, 1024);
    for _ in 0..100 {
        cf0.alloc_page();
        cf1.alloc_page();
    }

    for _ in 0..100000 {
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
