use crate::{
    dividedstg::{DividedStg, FD, FD_SIZE},
    nd, util, Arc, BTreeSet, Data, Limits, PageStorage, PageStorageInfo, Storage,
};

/// Implementation of [PageStorage] using [DividedStg].

/*

File 0 (PN_FILE) has a header ( allocation info and FDs ) then info for each numbered page, a 16-bit size and index into sub-file.

First word of allocated page is 64-bit page number ( to allow relocation ).

*/

pub struct BlockPageStg {
    /// Underlying Divided Storage.
    pub ds: DividedStg,
    alloc_pn: u64,
    first_free_pn: u64,
    fd: Vec<FD>,
    free_pn: BTreeSet<u64>, // Temporary set of free page numbers.
    header_dirty: bool,
    is_new: bool,
    psi: SizeInfo,
    header_size: u64,
    zbytes: Data,
}

const PN_FILE: usize = 0; // Page number sub-file, has header and info (size,index) for each numbered page.
const NOT_PN: u64 = u64::MAX >> 16; // Special value to denote end of list of free page numbers.
const PAGE_HSIZE: usize = 8; // Space for 64-bit page number to allow page to be relocated.
const HEADER_SIZE: usize = 24; // Space in PN_FILE for storing alloc_pn, first_free_pn, max_div, sizes.

impl BlockPageStg {
    /// Construct from specified Storage and limits.
    pub fn new(stg: Box<dyn Storage>, lim: &Limits) -> Box<Self> {
        let is_new = stg.size() == 0;

        let sizes = lim.page_sizes;
        let max_div = lim.max_div;
        let ds = DividedStg::new(stg, lim.blk_cap);
        let blk_cap = ds.blk_cap as usize;

        let mut s = Self {
            ds,
            alloc_pn: 0,
            first_free_pn: NOT_PN,
            fd: Vec::new(),
            free_pn: BTreeSet::default(),
            header_dirty: true,
            is_new,
            psi: SizeInfo {
                blk_cap,
                max_div,
                sizes,
            },
            header_size: 0,
            zbytes: nd(),
        };

        if is_new {
            for _i in 0..sizes + 1 {
                s.fd.push(s.ds.new_file());
            }
            s.ds.set_root(&s.fd[0]);
        } else {
            s.read_header();
        }

        // Page sizes are assumed to fit in u16.
        assert!(
            s.psi.max_size_page() <= u16::MAX as usize,
            "Max page size is 65535"
        );

        s.header_size = (HEADER_SIZE + s.psi.sizes * FD_SIZE) as u64;
        s.zbytes = Arc::new(vec![0; s.psi.max_size_page()]);

        #[cfg(feature = "log")]
        println!("bps new alloc={:?}", &s.allocs());

        Box::new(s)
    }

    /// Read page number file header.
    fn read_header(&mut self) {
        self.fd.clear();
        self.fd.push(self.ds.get_root());

        let mut buf = [0; HEADER_SIZE];
        self.read(PN_FILE, 0, &mut buf);
        self.alloc_pn = util::getu64(&buf, 0);
        self.first_free_pn = util::getu64(&buf, 8);

        self.psi.max_div = util::get(&buf, 16, 4) as usize;
        self.psi.sizes = util::get(&buf, 20, 4) as usize;

        let sizes = self.psi.sizes;
        let mut buf = vec![0; FD_SIZE * sizes];
        self.read(PN_FILE, HEADER_SIZE as u64, &mut buf);

        for fx in 0..sizes {
            let off = fx * FD_SIZE;
            self.fd.push(self.ds.load_fd(&buf[off..]));
        }
        self.header_dirty = false;
    }

    /// Write page number file header.
    fn write_header(&mut self) {
        let mut buf = vec![0; self.header_size as usize];
        util::setu64(&mut buf, self.alloc_pn);
        util::setu64(&mut buf[8..], self.first_free_pn);
        util::set(&mut buf, 16, self.psi.max_div as u64, 4);
        util::set(&mut buf, 20, self.psi.sizes as u64, 4);

        for fx in 0..self.psi.sizes {
            let off = HEADER_SIZE + fx * FD_SIZE;
            self.ds.save_fd(&self.fd[fx + 1], &mut buf[off..]);
        }
        self.write(PN_FILE, 0, &buf);
        self.header_dirty = false;

        #[cfg(feature = "log")]
        println!("bps write_header allocs={:?}", &self.allocs());
    }

    #[cfg(feature = "log")]
    fn allocs(&self) -> Vec<u64> {
        (0..self.psi.sizes() + 1).map(|x| self.alloc(x)).collect()
    }

    /// Get page size of sub-file ( fx > 0 ).
    fn page_size(&self, fx: usize) -> u64 {
        (self.psi.size(fx) + PAGE_HSIZE) as u64
    }

    /// Get sub-file size.
    fn fsize(&self, fx: usize) -> u64 {
        let size = self.fd[fx].size();
        if fx == 0 && size < self.header_size {
            self.header_size
        } else {
            size
        }
    }

    /// Use sub-file size to calculate allocation index.
    fn alloc(&self, fx: usize) -> u64 {
        let size = self.fsize(fx);
        if fx == 0 {
            (size - self.header_size) / 8
        } else {
            let ps = self.page_size(fx);
            (size + ps - 1) / ps
        }
    }

    /// Free page by relocating last page in sub-file to fill gap and truncating.
    fn free_page(&mut self, fx: usize, ix: u64) {
        if fx != 0 {
            let last = self.alloc(fx) - 1;
            let ps = self.page_size(fx);
            if last != ix {            
                let mut buf = vec![0; ps as usize];
                self.read(fx, last * ps, &mut buf);
                let pn = util::getu64(&buf, 0);
                let (fx1, _size, ix1) = self.get_pn_info(pn);
                assert!(fx1 == fx && ix1 == last);
                self.update_ix(pn, ix);
                self.write_data(fx, ix * ps, Arc::new(buf));
            }
            self.truncate(fx, last * ps);
        }
    }

    /// Set numbered page info.
    fn set_pn_info(&mut self, pn: u64, size: usize, ix: u64) {
        let off = self.header_size + pn * 8;
        let eof = self.fsize(PN_FILE);
        if off > eof {
            self.clear(PN_FILE, eof, off - eof);
        }
        let mut buf = [0; 8];
        util::set(&mut buf, 0, ix, 6);
        util::set(&mut buf, 6, size as u64, 2);
        self.write(PN_FILE, off, &buf);
    }

    /// Get info about numbered page ( file index, size, index ).
    fn get_pn_info(&self, pn: u64) -> (usize, usize, u64) {
        let off = self.header_size + pn * 8;
        if off >= self.fsize(0) {
            return (0, 0, 0);
        }
        let mut buf = [0; 8];
        self.read(PN_FILE, off, &mut buf);
        let ix = util::get(&buf, 0, 6);
        let size = util::get(&buf, 6, 2) as usize;
        let fx = if size == 0 { 0 } else { self.psi.index(size) };
        (fx, size, ix)
    }

    /// Update ix for numbered page ( for relocation ).
    fn update_ix(&mut self, pn: u64, ix: u64) {
        let off = self.header_size + pn * 8;
        self.write(PN_FILE, off, &ix.to_le_bytes()[0..6]);
    }

    /// Clear sub-file region.
    fn clear(&mut self, fx: usize, off: u64, n: u64) {
        let z = Arc::new(vec![0; n as usize]);
        self.write_data(fx, off, z);
    }

    /// Write sub-file.
    fn write(&mut self, fx: usize, off: u64, data: &[u8]) {
        let data = Arc::new(data.to_vec());
        self.write_data(fx, off, data);
    }

    /// Write sub-file Data.
    fn write_data(&mut self, fx: usize, off: u64, data: Data) {
        let n = data.len();
        self.write_data_n(fx, off, data, n);
    }

    /// Write sub-file Data up to n bytes.
    fn write_data_n(&mut self, fx: usize, off: u64, data: Data, n: usize) {
        self.ds.write_data(&mut self.fd[fx], off, data, n);
        self.save_fd(fx);
    }

    /// Truncate sub-file.
    fn truncate(&mut self, fx: usize, off: u64) {
        self.ds.truncate(&mut self.fd[fx], off);
        self.save_fd(fx);
    }

    /// Save sub-file descriptor after write or truncate operation.
    fn save_fd(&mut self, fx: usize) {
        let fd = &mut self.fd[fx];
        if fd.changed {
            fd.changed = false;
            self.header_dirty = true;
            if fx == 0 {
                self.ds.set_root(fd);
            }
        }
    }

    /// Read sub-file.
    fn read(&self, fx: usize, off: u64, data: &mut [u8]) {
        self.ds.read(&self.fd[fx], off, data);
    }
}

impl PageStorage for BlockPageStg {
    fn is_new(&self) -> bool {
        self.is_new
    }

    fn new_page(&mut self) -> u64 {
        if let Some(pn) = self.free_pn.pop_first() {
            pn
        } else {
            self.header_dirty = true;
            let pn = self.first_free_pn;
            if pn != NOT_PN {
                let (_fx, _size, next) = self.get_pn_info(pn);
                self.first_free_pn = next;
                pn
            } else {
                let pn = self.alloc_pn;
                self.alloc_pn += 1;
                pn
            }
        }
    }

    fn drop_page(&mut self, pn: u64) {
        self.free_pn.insert(pn);
    }

    fn info(&self) -> Box<dyn PageStorageInfo> {
        Box::new(self.psi.clone())
    }

    fn set_page(&mut self, pn: u64, data: Data) {
        let size = data.len();
        let fx = self.psi.index(size);
        let ps = self.page_size(fx);
        let (old_fx, mut old_size, mut ix) = self.get_pn_info(pn);
        if size != old_size {
            if fx != old_fx {
                self.free_page(old_fx, ix);
                ix = self.alloc(fx);
                old_size = ps as usize - PAGE_HSIZE;
                self.write(fx, ix * ps, &pn.to_le_bytes());
                self.set_pn_info(pn, size, ix);
            }
            self.set_pn_info(pn, size, ix);
        }

        let off = PAGE_HSIZE as u64 + ix * ps;
        self.write_data_n(fx, off, data, size);

        // Clear unused space in page.
        if old_size > size {
            self.write_data_n(fx, off + size as u64, self.zbytes.clone(), old_size - size);
        }
    }

    fn get_page(&self, pn: u64) -> Data {
        let (fx, size, ix) = self.get_pn_info(pn);
        if fx == 0 {
            return nd();
        }
        let mut data = vec![0; size];
        let off = PAGE_HSIZE as u64 + ix * self.page_size(fx);
        self.read(fx, off, &mut data);
        Arc::new(data)
    }

    fn size(&self, pn: u64) -> usize {
        self.get_pn_info(pn).1
    }

    fn save(&mut self) {
        // Free the temporary set of free logical pages.
        let flist = std::mem::take(&mut self.free_pn);
        for pn in flist.iter().rev() {
            let pn = *pn;
            let (fx, _size, ix) = self.get_pn_info(pn);
            self.free_page(fx, ix);
            self.set_pn_info(pn, 0, self.first_free_pn);
            self.first_free_pn = pn;
            self.header_dirty = true;
        }
        if self.header_dirty {
            self.write_header();
        }
        self.ds.save();
    }

    fn rollback(&mut self) {
        self.free_pn.clear();
        self.read_header();
    }

    fn wait_complete(&self) {
        self.ds.wait_complete();
    }

    #[cfg(feature = "verify")]
    fn get_free(&mut self) -> (crate::HashSet<u64>, u64) {
        let mut free = crate::HashSet::default();
        let mut pn = self.first_free_pn;
        while pn != NOT_PN {
            assert!(free.insert(pn));
            let (_fx, _size, next) = self.get_pn_info(pn);
            pn = next;
        }
        (free, self.alloc_pn)
    }

    #[cfg(feature = "renumber")]
    fn load_free_pages(&mut self) -> Option<u64> {
        let mut pn = self.first_free_pn;
        if pn == NOT_PN {
            return None;
        }
        while pn != NOT_PN {
            let (_sx, _size, next) = self.get_pn_info(pn);
            self.drop_page(pn);
            pn = next;
        }
        self.first_free_pn = NOT_PN;
        self.header_dirty = true;
        Some(self.alloc_pn - self.free_pn.len() as u64)
    }

    #[cfg(feature = "renumber")]
    fn renumber(&mut self, pn: u64) -> u64 {
        let new_pn = self.new_page();
        let (fx, size, ix) = self.get_pn_info(pn);
        if fx != 0 {
            let off = ix * self.page_size(fx);
            self.write(fx, off, &new_pn.to_le_bytes());
        }
        self.set_pn_info(new_pn, size, ix);
        self.set_pn_info(pn, 0, 0);
        self.drop_page(pn);
        new_pn
    }

    #[cfg(feature = "renumber")]
    fn set_alloc_pn(&mut self, target: u64) {
        assert!(self.first_free_pn == NOT_PN);
        self.alloc_pn = target;
        self.header_dirty = true;
        self.free_pn.clear();
        self.truncate(PN_FILE, self.header_size + target * 8);
    }
}

#[derive(Clone)]
struct SizeInfo {
    blk_cap: usize,
    max_div: usize,
    sizes: usize,
}

impl PageStorageInfo for SizeInfo {
    /// The number of different page sizes.
    fn sizes(&self) -> usize {
        self.sizes
    }

    /// Size index for given page size.
    fn index(&self, size: usize) -> usize {
        let r = self.blk_cap / (size + PAGE_HSIZE);
        if r >= self.max_div {
            1
        } else {
            1 + self.max_div - r
        }
    }

    /// Page size for given index.
    fn size(&self, ix: usize) -> usize {
        debug_assert!(ix > 0 && ix <= self.sizes);
        let size = self.blk_cap / (1 + self.max_div - ix);
        size - PAGE_HSIZE
    }
}

#[test]
fn test_block_page_stg() {
    let stg = crate::MemFile::new();
    let limits = Limits::default();
    let mut bps = BlockPageStg::new(stg.clone(), &limits);

    let pn = bps.new_page();
    let data = Arc::new(b"hello george".to_vec());

    bps.set_page(pn, data.clone());

    bps.save();
    let mut bps = BlockPageStg::new(stg, &limits);

    let data1 = bps.get_page(pn);
    assert!(data == data1);

    bps.save();
}
