use crate::{
    dividedstg::{DividedStg, FD, FD_SIZE},
    nd, util, Arc, BTreeSet, Data, HashSet, Limits, PageStorage, PageStorageInfo, Storage,
};

const PAGE_HSIZE: usize = 8;
const HEADER_SIZE: usize = 24;

const PN_FILE: usize = 0;
const NOT_PN: u64 = u64::MAX >> 16;

#[derive(Clone)]
struct Info {
    blk_cap: u64,
    max_div: usize,
    sizes: usize,
}

impl PageStorageInfo for Info {
    /// The number of different page sizes.
    fn sizes(&self) -> usize {
        self.sizes
    }

    /// Size index for given page size.
    fn index(&self, size: usize) -> usize {
        let size = size + PAGE_HSIZE;
        let r = (self.blk_cap as usize) / size;
        if r >= self.max_div {
            1
        } else {
            1 + self.max_div - r
        }
    }

    /// Page size for given index.
    fn size(&self, ix: usize) -> usize {
        let size = self.blk_cap as usize / (1 + self.max_div - ix);
        size - PAGE_HSIZE
    }
}

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
    psi: Info,
    header_size: u64,
    zbytes: Data,
}

impl BlockPageStg {
    /// Construct from specified Storage and limits.
    pub fn new(stg: Box<dyn Storage>, lim: &Limits) -> Box<Self> {
        let is_new = stg.size() == 0;

        let blk_cap = lim.blk_cap;
        let sizes = lim.page_sizes;
        let max_div = lim.max_div;

        let mut s = Self {
            ds: DividedStg::new(stg, blk_cap),
            alloc_pn: 0,
            first_free_pn: NOT_PN,
            fd: Vec::new(),
            free_pn: BTreeSet::default(),
            header_dirty: false,
            is_new,
            psi: Info {
                blk_cap,
                max_div,
                sizes,
            },
            header_size: 0,
            zbytes: nd(),
        };

        // Page sizes are assumed to fit in u16.
        assert!(
            s.psi.max_size_page() <= u16::MAX as usize,
            "Max page size is 65535"
        );

        if is_new {
            for _i in 0..sizes + 1 {
                s.fd.push(s.ds.new_file());
            }
            s.ds.set_root(&s.fd[0]);
            s.header_dirty = true;
        } else {
            s.psi.blk_cap = s.ds.blk_cap();
            s.read_header();
        }
        s.header_size = (HEADER_SIZE + s.psi.sizes * FD_SIZE) as u64;
        s.zbytes = Arc::new(vec![0; s.psi.max_size_page()]);

        #[cfg(feature = "log")]
        println!("bps new alloc={:?}", &s.allocs());

        Box::new(s)
    }

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

        for i in 0..sizes {
            let off = i * FD_SIZE;
            self.fd.push(self.ds.load_fd(&buf[off..]));
        }
        self.header_dirty = false;
    }

    fn write_header(&mut self) {
        let mut buf = vec![0; self.header_size as usize];
        util::setu64(&mut buf, self.alloc_pn);
        util::setu64(&mut buf[8..], self.first_free_pn);
        util::set(&mut buf, 16, self.psi.max_div as u64, 4);
        util::set(&mut buf, 20, self.psi.sizes as u64, 4);

        for i in 0..self.psi.sizes {
            let off = HEADER_SIZE + i * FD_SIZE;
            self.ds.save_fd(&self.fd[i + 1], &mut buf[off..]);
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

    fn page_size(&self, sx: usize) -> u64 {
        (self.psi.size(sx) + PAGE_HSIZE) as u64
    }

    fn fsize(&self, ix: usize) -> u64 {
        let size = self.fd[ix].size();
        if ix == 0 && size < self.header_size {
            self.header_size
        } else {
            size
        }
    }

    // Use file size to calculate allocation index.
    fn alloc(&self, ix: usize) -> u64 {
        let size = self.fsize(ix);
        if ix == 0 {
            (size - self.header_size) / 8
        } else {
            let ps = self.page_size(ix);
            (size + ps - 1) / ps
        }
    }

    fn free_page(&mut self, sx: usize, ix: u64) {
        if sx != 0 {
            // Relocate last page in file to fill gap.
            let last = self.alloc(sx) - 1;
            self.relocate(sx, last, ix);
            self.truncate(sx, last * self.page_size(sx));
        }
    }

    fn relocate(&mut self, sx: usize, from: u64, to: u64) {
        if from != to {
            let ps = self.page_size(sx);
            let mut buf = vec![0; ps as usize];

            self.read(sx, from * ps, &mut buf);
            let pn = util::getu64(&buf, 0);

            let (sx1, _size, ix1) = self.get_pn_info(pn);
            assert!(sx1 == sx && ix1 == from);

            self.update_ix(pn, to);
            self.write_data(sx, to * ps, Arc::new(buf));
        }
    }

    fn get_pn_info(&self, pn: u64) -> (usize, usize, u64) {
        let off = self.header_size + pn * 8;
        if off >= self.fsize(0) {
            return (0, 0, 0);
        }
        let mut buf = [0; 8];
        self.read(PN_FILE, off, &mut buf);
        let ix = util::get(&buf, 0, 6);
        let size = util::get(&buf, 6, 2) as usize;
        let sx = if size == 0 { 0 } else { self.psi.index(size) };
        (sx, size, ix)
    }

    fn set_pn_info(&mut self, pn: u64, size: usize, ix: u64) {
        let off = self.header_size + pn * 8;
        let eof = self.fsize(0);
        if off > eof {
            self.clear(PN_FILE, eof, off - eof);
        }
        let mut buf = [0; 8];
        util::set(&mut buf, 0, ix, 6);
        util::set(&mut buf, 6, size as u64, 2);
        self.write(PN_FILE, off, &buf);
    }

    fn update_ix(&mut self, pn: u64, ix: u64) {
        let mut buf = [0; 6];
        util::set(&mut buf, 0, ix, 6);
        let off = self.header_size + pn * 8;
        self.write(PN_FILE, off, &buf);
    }

    fn clear(&mut self, fx: usize, off: u64, n: u64) {
        let z = Arc::new(vec![0; n as usize]);
        self.write_data(fx, off, z);
    }

    fn write(&mut self, fx: usize, off: u64, data: &[u8]) {
        let data = Arc::new(data.to_vec());
        self.write_data(fx, off, data);
    }

    fn write_data(&mut self, fx: usize, off: u64, data: Data) {
        let n = data.len();
        self.write_data_n(fx, off, data, n);
    }

    fn write_data_n(&mut self, fx: usize, off: u64, data: Data, n: usize) {
        self.ds.write_data(&mut self.fd[fx], off, data, n);
        self.save_fd(fx);
    }

    fn truncate(&mut self, fx: usize, off: u64) {
        self.ds.truncate(&mut self.fd[fx], off);
        self.save_fd(fx);
    }

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
                let (_sx, _size, next) = self.get_pn_info(pn);
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
        let sx = self.psi.index(size);
        let ps = self.page_size(sx);
        let (old_sx, mut old_size, mut ix) = self.get_pn_info(pn);
        if sx != old_sx {
            self.free_page(old_sx, ix);
            ix = self.alloc(sx);
            old_size = ps as usize - PAGE_HSIZE;
            self.write(sx, ix * ps, &pn.to_le_bytes());
        }
        self.set_pn_info(pn, size, ix);

        let off = PAGE_HSIZE as u64 + ix * ps;
        self.write_data_n(sx, off, data, size);

        // Clear unused space in page.
        if old_size > size {
            self.write_data_n(sx, off + size as u64, self.zbytes.clone(), old_size - size);
        }
    }

    fn get_page(&self, pn: u64) -> Data {
        let (sx, size, ix) = self.get_pn_info(pn);
        if sx == 0 {
            return nd();
        }
        let mut data = vec![0; size];
        let off = PAGE_HSIZE as u64 + ix * self.page_size(sx);
        self.read(sx, off, &mut data);
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
            let (sx, _size, ix) = self.get_pn_info(pn);
            self.free_page(sx, ix);
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
    fn get_free(&mut self) -> (HashSet<u64>, u64) {
        let mut free = crate::HashSet::default();
        let mut pn = self.first_free_pn;
        while pn != NOT_PN {
            assert!(free.insert(pn));
            let (_sx, _size, next) = self.get_pn_info(pn);
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
        let (sx, size, ix) = self.get_pn_info(pn);
        if sx != 0 {
            let off = ix * self.page_size(sx);
            self.write(sx, off, &new_pn.to_le_bytes());
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
