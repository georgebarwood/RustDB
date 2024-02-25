use crate::{
    block::BLK_CAP,
    dividedstg::{DividedStg, FD, FD_SIZE},
    nd, util, Arc, BTreeSet, Data, HashSet, PageStorage, PageStorageInfo, Storage,
};

const PAGE_SIZES: usize = 7;
const PAGE_HSIZE: usize = 8;

const PINFO_FILE: usize = 0;
const HEADER_SIZE: usize = 32 + (8 + FD_SIZE) * PAGE_SIZES;
const NOT_PN: u64 = u64::MAX >> 16;

struct Info();
impl PageStorageInfo for Info {
    /// The number of different page sizes.
    fn sizes(&self) -> usize {
        PAGE_SIZES
    }

    /// Size index for given page size.
    fn index(&self, size: usize) -> usize {
        BlockPageStg::size_index(size + PAGE_HSIZE)
    }

    /// Page size for given index.
    fn size(&self, ix: usize) -> usize {
        BlockPageStg::page_size(ix) as usize - PAGE_HSIZE
    }
}

/// Implementation of [PageStorage] using [DividedStg].
///
///  File 0 (PINFO_FILE) is used to store fixed size header ( allocation info and FDs + info for each numbered page ( 16-bit size and index into sub-file ).
///
///  First word of allocated page is 64-bit page number ( to allow relocation ).

pub struct BlockPageStg {
    /// Underlying Divided Storage.
    ds: DividedStg,
    alloc_pn: u64,
    first_free_pn: u64,
    pn_init: u64,
    fd: [FD; PAGE_SIZES + 1],
    alloc: [u64; PAGE_SIZES + 1],
    free_pn: BTreeSet<u64>, // Temporary set of free page numbers.
    header_dirty: bool,
    is_new: bool,
}

impl BlockPageStg {
    /// This is the raw page size including PAGE_HSIZE bytes for the containing page number.
    fn page_size(ix: usize) -> u64 {
        BLK_CAP / (13 - ix as u64)
    }

    fn size_index(size: usize) -> usize {
        let mut ix = 1;
        while Self::page_size(ix) < size as u64 {
            ix += 1;
        }
        ix
    }

    ///
    pub fn new(stg: Box<dyn Storage>) -> Self {
        let is_new = stg.size() == 0;
        let mut result = Self {
            ds: DividedStg::new(stg),
            alloc_pn: 0,
            first_free_pn: NOT_PN,
            pn_init: 0,
            alloc: [0; PAGE_SIZES + 1],
            fd: [FD::default(); PAGE_SIZES + 1],
            free_pn: BTreeSet::default(),
            header_dirty: false,
            is_new,
        };
        if is_new {
            for i in 0..PAGE_SIZES + 1 {
                result.fd[i] = result.ds.new_file();
            }
            result.ds.set_root(&result.fd[0]);
            result.header_dirty = true;
        } else {
            result.read_header();
        }
        result
    }

    fn read_header(&mut self) {
        self.ds.get_root(&mut self.fd[0]);
        let mut buf = [0; HEADER_SIZE];
        self.read(PINFO_FILE, 0, &mut buf);
        self.alloc_pn = util::getu64(&buf, 0);
        self.first_free_pn = util::getu64(&buf, 8);
        self.pn_init = util::getu64(&buf, 16);
        self.alloc[0] = util::getu64(&buf, 24);

        for i in 1..PAGE_SIZES + 1 {
            let off = 32 + (i - 1) * (8 + FD_SIZE);
            self.alloc[i] = util::getu64(&buf, off);
            self.fd[i].load(&buf[off + 8..]);
        }
        self.header_dirty = false;
        #[cfg(feature = "log")]
        println!("bps read_header alloc={:?} fd={:?}", &self.alloc, &self.fd);
    }

    fn write_header(&mut self) {
        let mut buf = [0; HEADER_SIZE];
        util::setu64(&mut buf, self.alloc_pn);
        util::setu64(&mut buf[8..], self.first_free_pn);
        util::setu64(&mut buf[16..], self.pn_init);
        util::setu64(&mut buf[24..], self.alloc[0]);

        for i in 1..PAGE_SIZES + 1 {
            let off = 32 + (i - 1) * (8 + FD_SIZE);
            util::setu64(&mut buf[off..], self.alloc[i]);
            self.fd[i].save(&mut buf[off + 8..]);
        }
        self.write(PINFO_FILE, 0, &buf);
        self.header_dirty = false;

        #[cfg(feature = "log")]
        println!("bps write_header alloc={:?} fd={:?}", &self.alloc, &self.fd);
    }

    fn alloc_page(&mut self, sx: usize) -> u64 {
        assert!(sx > 0);
        let ix = self.alloc[sx];
        self.alloc[sx] += 1;
        self.header_dirty = true;
        ix
    }

    fn free_page(&mut self, sx: usize, ix: u64) {
        if sx == 0 {
            return;
        }

        // Relocate last page in file to fill gap.
        self.alloc[sx] -= 1;
        let from = self.alloc[sx];
        self.header_dirty = true;

        self.relocate(sx, from, ix);

        let end = from * Self::page_size(sx);

        self.ds.truncate(&mut self.fd[sx], end);
    }

    fn relocate(&mut self, sx: usize, from: u64, to: u64) {
        if from == to {
            return;
        }
        let ps = Self::page_size(sx);
        let mut buf = vec![0; ps as usize];

        self.read(sx, from * ps, &mut buf);
        let pn = util::getu64(&buf, 0);

        let (sx1, _size, ix1) = self.get_page_info(pn);
        assert!(
            sx1 == sx && ix1 == from,
            "pn={pn} sx1={sx1} sx={sx} ix1={ix1} from={from}"
        );

        self.update_ix(pn, to);

        self.write(sx, to * ps, &buf);
    }

    fn get_page_info(&self, pn: u64) -> (usize, usize, u64) {
        if pn >= self.alloc[0] {
            return (0, 0, 0);
        }
        let mut buf = [0; 8];
        let off = HEADER_SIZE as u64 + pn * 8;
        self.read(PINFO_FILE, off, &mut buf);
        let ix = util::get(&buf, 0, 6);
        let size = util::get(&buf, 6, 2) as usize;
        let sx = if size == 0 {
            0
        } else {
            Self::size_index(size + PAGE_HSIZE)
        };
        (sx, size, ix)
    }

    fn set_page_info(&mut self, pn: u64, size: usize, ix: u64) {
        let off = HEADER_SIZE as u64 + pn * 8;
        if pn >= self.alloc[0] {
            let start = HEADER_SIZE as u64 + self.alloc[0] * 8;
            self.clear(PINFO_FILE, start, off - start);
            self.alloc[0] = pn + 1;
            self.header_dirty = true;
        }
        let mut buf = [0; 8];
        util::set(&mut buf, 0, ix, 6);
        util::set(&mut buf, 6, size as u64, 2);
        self.write(PINFO_FILE, off, &buf);
    }

    fn truncate_page_info(&mut self) {
        let off = HEADER_SIZE as u64 + self.alloc_pn * 8;
        self.truncate(PINFO_FILE, off);
    }

    fn update_ix(&mut self, pn: u64, ix: u64) {
        let mut buf = [0; 6];
        util::set(&mut buf, 0, ix, 6);
        let off = HEADER_SIZE as u64 + pn * 8;
        self.write(PINFO_FILE, off, &buf);
    }

    fn clear(&mut self, fx: usize, off: u64, n: u64) {
        if n > 0 {
            let buf = vec![0; n as usize];
            self.write(fx, off, &buf);
        }
    }

    fn write(&mut self, fx: usize, off: u64, data: &[u8]) {
        let data = Arc::new(data.to_vec());
        self.write_data(fx, off, data);
    }

    fn write_data(&mut self, fx: usize, off: u64, data: Data) {
        self.ds.write_data(&mut self.fd[fx], off, data);
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
        #[cfg(feature = "log-bps")]
        println!("bps new_page");

        if let Some(pn) = self.free_pn.pop_first() {
            pn
        } else {
            self.header_dirty = true;
            let pn = self.first_free_pn;
            if pn != NOT_PN {
                let (_sx, _size, next) = self.get_page_info(pn);
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
        #[cfg(feature = "log-bps")]
        println!("bps drop_page pn={}", pn);
        self.free_pn.insert(pn);
    }

    fn info(&self) -> Box<dyn PageStorageInfo> {
        Box::new(Info {})
    }

    fn set_page(&mut self, pn: u64, data: Data) {
        #[cfg(feature = "log-bps")]
        println!("bps set_page pn={} data len={}", pn, data.len());

        let size = data.len();
        let rsx = Self::size_index(size + PAGE_HSIZE);
        assert!(rsx <= PAGE_SIZES);
        assert!(size == 0 || rsx > 0);

        let (sx, _size, ix) = self.get_page_info(pn);

        let ix = if sx != rsx {
            // Re-allocate page.
            self.free_page(sx, ix);
            let ix = self.alloc_page(rsx);

            // Set first word of page to page number.
            if rsx != 0 {
                let off = ix * Self::page_size(rsx);
                self.write(rsx, off, &pn.to_le_bytes());
            }
            ix
        } else {
            ix
        };
        self.set_page_info(pn, size, ix);

        if rsx != 0 {
            // Offset of user data within sub-file.
            let off = PAGE_HSIZE as u64 + ix * Self::page_size(rsx);

            // Write data.
            self.write_data(rsx, off, data);
        }
    }

    fn get_page(&self, pn: u64) -> Data {
        #[cfg(feature = "log-bps")]
        println!("bps get_page pn={}", pn);

        let (sx, size, ix) = self.get_page_info(pn);

        if sx == 0 {
            return nd();
        }

        // Offset of data within sub-file.
        let off = PAGE_HSIZE as u64 + ix * Self::page_size(sx);

        let mut data = vec![0; size];
        self.read(sx, off, &mut data);
        Arc::new(data)
    }

    fn size(&self, pn: u64) -> usize {
        let (_sx, size, _ix) = self.get_page_info(pn);
        size
    }

    fn save(&mut self) {
        // Free the temporary set of free logical pages.
        let flist = std::mem::take(&mut self.free_pn);
        for pn in flist.iter().rev() {
            let pn = *pn;
            let (sx, _size, ix) = self.get_page_info(pn);
            self.free_page(sx, ix);
            self.set_page_info(pn, 0, self.first_free_pn);
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
            let (_sx, _size, next) = self.get_page_info(pn);
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
            let (_sx, _size, next) = self.get_page_info(pn);
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
        let (sx, size, ix) = self.get_page_info(pn);
        let off = ix * Self::page_size(sx);
        self.write(sx, off, &new_pn.to_le_bytes());
        self.set_page_info(new_pn, size, ix);
        self.set_page_info(pn, 0, 0);
        self.drop_page(pn);
        new_pn
    }

    #[cfg(feature = "renumber")]
    fn set_alloc_pn(&mut self, target: u64) {
        assert!(self.first_free_pn == NOT_PN);
        self.alloc_pn = target;
        self.alloc[0] = target;
        self.header_dirty = true;
        self.free_pn.clear();
        self.truncate_page_info();
    }
}

#[test]
fn test_block_page_stg() {
    let stg = crate::MemFile::new();
    let mut bps = BlockPageStg::new(stg.clone());

    let pn = bps.new_page();
    let data = Arc::new(b"hello george".to_vec());

    bps.set_page(pn, data.clone());

    bps.save();
    let mut bps = BlockPageStg::new(stg);

    let data1 = bps.get_page(pn);
    assert!(data == data1);

    //let data = Arc::new(vec![99; 2000]);
    //bps.set_page(pn, data);

    //bps.drop_page(pn);
    bps.save();
}
