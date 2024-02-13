use crate::block::*;
use crate::*;

const PAGE_SIZES: usize = 16;
const PAGE_UNIT: usize = 1024;
const PAGE_HSIZE: usize = 10;

const HEADER_SIZE: usize = 24 + 8 * PAGE_SIZES;
const INDEX_BITS: u8 = 60;
const INDEX_MASK: u64 = (1 << 60) - 1;
const NOT_PN: u64 = INDEX_MASK;
const PINFO_FILE: u64 = 0;

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
    pub ds: DividedStg,
    alloc_pn: u64,
    first_free_pn: u64,
    pn_init: u64,
    alloc: [u64; PAGE_SIZES],
    free_pn: BTreeSet<u64>, // Temporary set of free page numbers.
    header_dirty: bool,
}

impl BlockPageStg {
    ///
    pub fn new(stg: Box<dyn Storage>) -> Self {
        let mut result = Self {
            ds: DividedStg::new(stg),
            alloc_pn: 0,
            first_free_pn: NOT_PN,
            pn_init: 0,
            alloc: [0; PAGE_SIZES],
            free_pn: BTreeSet::default(),
            header_dirty: false,
        };
        if result.is_new() {
            for i in 0..PAGE_SIZES + 1 {
                assert!(result.ds.new_file() == i as u64);
            }
            result.header_dirty = true;
        } else {
            result.read_header();
        }
        result
    }

    fn read_header(&mut self) {
        let mut buf = [0; HEADER_SIZE];
        self.ds.read(0, 0, &mut buf);
        self.alloc_pn = util::getu64(&buf, 0);
        self.first_free_pn = util::getu64(&buf, 8);
        self.pn_init = util::getu64(&buf, 16);

        for i in 0..PAGE_SIZES - 1 {
            self.alloc[i] = util::getu64(&buf, 24 + i * 8);
        }
    }

    fn write_header(&mut self) {
        let mut buf = [0; HEADER_SIZE];
        util::setu64(&mut buf, self.alloc_pn);
        util::setu64(&mut buf[8..], self.first_free_pn);
        util::setu64(&mut buf[16..], self.pn_init);

        for i in 0..PAGE_SIZES - 1 {
            util::setu64(&mut buf[24 + i * 8..], self.alloc[i]);
        }
        self.ds.write(0, 0, &buf);
    }

    fn alloc_page(&mut self, sx: usize) -> u64 {
        let ix = self.alloc[sx];
        self.alloc[sx] += 1;
        self.header_dirty = true;
        ix
    }

    fn free_page(&mut self, sx: usize, ix: u64) {
        println!("free sx={} ix={}", sx, ix);
        if sx == 0 {
            return;
        }
        // Relocate last item in file.
        self.alloc[sx] -= 1;
        let from = self.alloc[sx];
        self.header_dirty = true;
        self.relocate(sx, from, ix);

        self.ds.truncate(sx as u64, from * (sx * PAGE_UNIT) as u64);
    }

    fn relocate(&mut self, sx: usize, from: u64, to: u64) {
        println!("relocate sx={} from={} to={}", sx, from, to);
        if from == to {
            return;
        }
        let mut buf = vec![0; sx * PAGE_UNIT];
        let from = from * (sx * PAGE_UNIT) as u64;
        self.ds.read(sx as u64, from, &mut buf);
        let pn = util::getu64(&buf, 0);
        self.set_page_info(pn, sx, to);
        let to = to * (sx * PAGE_UNIT) as u64;
        self.ds.write(sx as u64, to, &buf);
    }

    fn get_page_info(&self, pn: u64) -> (usize, u64) {
        let mut buf = [0; 8];
        self.ds
            .read(PINFO_FILE, HEADER_SIZE as u64 + pn * 8, &mut buf);
        let value = u64::from_le_bytes(buf);
        let sx = (value >> INDEX_BITS) as usize;
        let ix = value & INDEX_MASK;
        println!("get_page_info pn={} sx={} ix={}", pn, sx, ix);
        (sx, ix)
    }

    fn set_page_info(&mut self, pn: u64, sx: usize, ix: u64) {
        println!("set_page_info pn={} sx={} ix={}", pn, sx, ix);
        let value = ix + ((sx as u64) << INDEX_BITS);
        self.ds.write(
            PINFO_FILE,
            HEADER_SIZE as u64 + pn * 8,
            &value.to_le_bytes(),
        );
    }

    fn size_index(size: usize) -> usize {
        (size + PAGE_HSIZE + PAGE_UNIT - 1) / PAGE_UNIT
    }
}

impl PageStorage for BlockPageStg {
    fn is_new(&self) -> bool {
        self.ds.is_new()
    }

    fn new_page(&mut self) -> u64 {
        if let Some(pn) = self.free_pn.pop_first() {
            pn
        } else {
            self.header_dirty = true;
            let pn = self.first_free_pn;
            if pn != NOT_PN {
                let (_sx, next) = self.get_page_info(pn);
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

        let (sx, ix) = self.get_page_info(pn);

        println!(
            "set_page pn={} size={} sx={} rsx={} ix={}",
            pn, size, sx, rsx, ix
        );

        let ix = if sx != rsx {
            // Re-allocate page.
            self.free_page(sx, ix);
            let ix = self.alloc_page(rsx);
            self.set_page_info(pn, rsx, ix);
            // Set first word of page to page number.
            let poff = ix * (sx * PAGE_UNIT) as u64;
            self.ds.write(rsx as u64, poff, &pn.to_le_bytes());
            ix
        } else {
            ix
        };

        // Offset of user data within sub-file.
        let off = PAGE_HSIZE as u64 + ix * (sx * PAGE_UNIT) as u64;

        // Write data size.
        self.ds
            .write(rsx as u64, off - 2, &(size as u16).to_le_bytes());
        // Write data.
        self.ds.write(rsx as u64, off, &data);
    }

    fn get_page(&mut self, pn: u64) -> Data {
        println!("get_page pn={}", pn);
        let (sx, ix) = self.get_page_info(pn);
        // Offset of data within sub-file.
        let off = PAGE_HSIZE as u64 + ix * (sx * PAGE_UNIT) as u64;
        // Get user data size and allocate data.
        let mut buf = [0; 2];
        self.ds.read(sx as u64, off - 2, &mut buf);
        let size = u16::from_le_bytes(buf);
        let mut data = vec![0; size as usize];
        // Read data.
        self.ds.read(sx as u64, off, &mut data);
        Arc::new(data)
    }

    fn save(&mut self) {
        println!("save");

        // Free the temporary set of free logical pages.
        let flist = std::mem::take(&mut self.free_pn);
        for pn in flist.iter().rev() {
            let pn = *pn;
            let (sx, ix) = self.get_page_info(pn);
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
    assert_eq!(data, data1);

    let data = Arc::new(vec![99; 2000]);
    bps.set_page(pn, data);

    bps.drop_page(pn);
    bps.save();
}
