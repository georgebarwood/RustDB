use crate::dividedstg::*;
use crate::*;

const PAGE_SIZES: usize = 16;
const PAGE_UNIT: usize = 1024;
const PAGE_HSIZE: usize = 8;

const HEADER_SIZE: usize = 24 + (8 + FD_SIZE) * (PAGE_SIZES + 1);

const NOT_PN: u64 = u64::MAX >> 16;

const PINFO_FILE: usize = 0;

struct Info();
impl PageStorageInfo for Info {
    /// The number of different page sizes.
    fn sizes(&self) -> usize {
        PAGE_SIZES
    }

    /// Size index for given page size.
    fn index(&self, size: usize) -> usize {
        BlockPageStg::size_index(size)
    }

    /// Page size for given index.
    fn size(&self, ix: usize) -> usize {
        ix * PAGE_UNIT - PAGE_HSIZE
    }
}

/// Implementation of [PageStorage] using [DividedStg].
///
///  File 0 (PINFO_FILE) is used to store fixed size header ( allocation info ) and info for each numbered page ( 4-bit sub-file index, index into sub-file ).
///
///  Within each subfile, first word of allocated page is 64-bit page number ( to allow relocation ), followed by data size (16 bits).

pub struct BlockPageStg {
    /// Underlying Divided Storage.
    ds: DividedStg,
    alloc_pn: u64,
    first_free_pn: u64,
    pn_init: u64,
    fd: [FD; PAGE_SIZES + 1],
    alloc: [u64; PAGE_SIZES + 1],
    /// alloc[0] is currently unused
    free_pn: BTreeSet<u64>, // Temporary set of free page numbers.
    header_dirty: bool,
    is_new: bool,
}

impl BlockPageStg {
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
            result.header_dirty = true;
        } else {
            result.read_header();
        }
        result
    }

    fn read_header(&mut self) {
        let mut buf = [0; HEADER_SIZE];
        self.ds.read(self.fd[PINFO_FILE], 0, &mut buf);
        self.alloc_pn = util::getu64(&buf, 0);
        self.first_free_pn = util::getu64(&buf, 8);
        self.pn_init = util::getu64(&buf, 16);

        for i in 0..PAGE_SIZES + 1 {
            self.alloc[i] = util::getu64(&buf, 24 + i * (8 + FD_SIZE));
            self.fd[i].load(&buf[24 + 8 + i * (8 + FD_SIZE)..]);
        }
    }

    fn write_header(&mut self) {
        let mut buf = [0; HEADER_SIZE];
        util::setu64(&mut buf, self.alloc_pn);
        util::setu64(&mut buf[8..], self.first_free_pn);
        util::setu64(&mut buf[16..], self.pn_init);

        for i in 0..PAGE_SIZES + 1 {
            util::setu64(&mut buf[24 + i * (8 + FD_SIZE)..], self.alloc[i]);
            self.fd[i].save(&mut buf[24 + 8 + i * (8 + FD_SIZE)..]);
        }
        self.ds.write(self.fd[PINFO_FILE], 0, &buf);
    }

    fn alloc_page(&mut self, sx: usize) -> u64 {
        let ix = self.alloc[sx];
        self.alloc[sx] += 1;
        self.header_dirty = true;
        ix
    }

    fn free_page(&mut self, sx: usize, ix: u64) {
        if sx == 0 {
            return;
        }

        // Relocate last item in file.
        self.alloc[sx] -= 1;
        let from = self.alloc[sx];
        self.header_dirty = true;
        self.relocate(sx, from, ix);

        let end = from * (sx * PAGE_UNIT) as u64;
        self.fd[sx] = self.ds.truncate(self.fd[sx], end);
    }

    fn relocate(&mut self, sx: usize, from: u64, to: u64) {
        if from == to {
            return;
        }
        let mut buf = vec![0; sx * PAGE_UNIT];
        let from = from * (sx * PAGE_UNIT) as u64;
        self.read(sx, from, &mut buf);
        let pn = util::getu64(&buf, 0);

        self.update_ix(pn, to);

        let to = to * (sx * PAGE_UNIT) as u64;
        self.write(sx, to, &buf);
    }

    fn get_page_info(&self, pn: u64) -> (usize, usize, u64) {
        let mut buf = [0; 8];
        let off = HEADER_SIZE as u64 + pn * 8;
        self.read(PINFO_FILE, off, &mut buf);
        let ix = util::get(&buf, 0, 6);
        let size = util::get(&buf, 6, 2) as usize;
        let sx = if size == 0 { 0 } else { Self::size_index(size) };
        (sx, size, ix)
    }

    fn set_page_info(&mut self, pn: u64, size: usize, ix: u64) {
        let mut buf = [0; 8];
        util::set(&mut buf, 0, ix, 6);
        util::set(&mut buf, 6, size as u64, 2);

        let off = HEADER_SIZE as u64 + pn * 8;
        self.write(PINFO_FILE, off, &buf);
    }

    fn update_ix(&mut self, pn: u64, ix: u64) {
        let mut buf = [0; 6];
        util::set(&mut buf, 0, ix, 6);
        let off = HEADER_SIZE as u64 + pn * 8;
        self.write(PINFO_FILE, off, &buf);
    }

    fn size_index(size: usize) -> usize {
        (size + PAGE_HSIZE + PAGE_UNIT - 1) / PAGE_UNIT
    }

    fn write(&mut self, fx: usize, off: u64, data: &[u8]) {
        let mut fd = self.fd[fx];
        fd = self.ds.allocate(fd, off + data.len() as u64);
        if fd.changed {
            fd.changed = false;
            self.fd[fx] = fd;
            self.header_dirty = true
        }
        self.ds.write(fd, off, data);
    }

    fn read(&self, fx: usize, off: u64, data: &mut [u8]) {
        self.ds.read(self.fd[fx], off, data);
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
        self.free_pn.insert(pn);
    }

    fn info(&self) -> Box<dyn PageStorageInfo> {
        Box::new(Info {})
    }

    fn set_page(&mut self, pn: u64, data: Data) {
        let size = data.len();
        let rsx = Self::size_index(size);

        let (sx, _size, ix) = self.get_page_info(pn);

        let ix = if sx != rsx {
            // Re-allocate page.
            self.free_page(sx, ix);
            let ix = self.alloc_page(rsx);

            // Set first word of page to page number.
            let off = ix * (rsx * PAGE_UNIT) as u64;
            self.write(rsx, off, &pn.to_le_bytes());
            ix
        } else {
            ix
        };
        self.set_page_info(pn, size, ix);

        // Offset of user data within sub-file.
        let off = PAGE_HSIZE as u64 + ix * (rsx * PAGE_UNIT) as u64;

        // Write data.
        self.write(rsx, off, &data);
    }

    fn get_page(&self, pn: u64) -> Data {
        let (sx, size, ix) = self.get_page_info(pn);

        if sx == 0 {
            return nd();
        }

        // Offset of data within sub-file.
        let off = PAGE_HSIZE as u64 + ix * (sx * PAGE_UNIT) as u64;

        let mut data = vec![0; size];
        self.read(sx, off, &mut data);
        Arc::new(data)
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
            self.header_dirty = false;
        }
        self.ds.save();
    }

    fn rollback(&mut self) {
        todo!()
    }

    fn renumber(&mut self, _pn: u64) -> u64 {
        todo!()
    }

    fn wait_complete(&self) {
        self.ds.wait_complete();
    }
}

#[test]
fn test_block_page_stg() {
    let stg = MemFile::new();
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
