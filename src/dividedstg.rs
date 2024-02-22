use crate::{block::*, *};
use std::cmp::min;

/// Divides Storage into sub-files of arbitrary size using [BlockStg].
pub struct DividedStg(BlockStg);

/// Number of block numbers that will fit in a block.
const BASE: u64 = BLK_CAP / NSZ;

/// Bytes required to save FD ( root, blocks ).
pub const FD_SIZE: usize = 8 + 8;

/// [DividedStg] File Descriptor.
#[derive(Clone, Copy, Default)]
pub struct FD {
    root: u64,
    blocks: u64,
    level: u8,
    ///
    pub changed: bool,
}

impl FD {
    fn set_blocks(&mut self, blocks: u64) {
        self.changed = self.blocks != blocks;
        self.blocks = blocks;
    }
    /// Save to byte buffer.
    pub fn save(&self, buf: &mut [u8]) {
        debug_assert!(self.level == DividedStg::levels(self.blocks));
        util::setu64(&mut buf[0..8], self.root);
        util::setu64(&mut buf[8..16], self.blocks);
    }
    /// Load from  byte buffer.
    pub fn load(&mut self, buf: &[u8]) {
        self.root = util::getu64(buf, 0);
        self.blocks = util::getu64(buf, 8);
        self.level = DividedStg::levels(self.blocks);
    }
}

#[cfg(any(feature = "log", feature = "log-div"))]
impl std::fmt::Debug for FD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{},{},{}", self.root, self.blocks, self.level))
    }
}

impl DividedStg {
    /// Construct DividedStg from specified Storage.
    pub fn new(stg: Box<dyn Storage>) -> Self {
        Self(BlockStg::new(stg))
    }

    /// Get file descriptor for a new file.
    pub fn new_file(&mut self) -> FD {
        FD {
            root: self.0.new_block(),
            level: 0,
            blocks: 1,
            changed: true,
        }
    }

    /// Drop specified file.
    pub fn drop_file(&mut self, f: &mut FD) {
        #[cfg(feature = "log-div")]
        println!("DS drop_file f={:?}", f);
        self.truncate(f, 0);
        self.0.drop_block(f.root);
    }

    /// Deallocate blocks not required for file of specified size.
    pub fn truncate(&mut self, f: &mut FD, size: u64) {
        #[cfg(feature = "log-div")]
        println!("DS truncate f={:?} size={}", f, size);

        let reqd = Self::blocks(size);
        if reqd < f.blocks {
            let levels = Self::levels(reqd);

            // Calculate new root
            let mut new_root = f.root;
            let mut n = f.level;
            while n > levels {
                new_root = self.get_num(new_root, 0);
                n -= 1;
            }

            // For each level reduce the number of blocks.
            let mut level = f.level;
            let mut old = f.blocks;
            let mut new = reqd;
            while level > 0 && old != new {
                self.reduce_blocks(f, level, old, new);
                new = (new + BASE - 1) / BASE;
                old = (old + BASE - 1) / BASE;
                level -= 1;
            }
            if levels < f.level {
                self.0.drop_block(f.root);
                f.root = new_root;
                f.level = levels;
            }
            f.set_blocks(reqd);
            if f.blocks == 0 {
                *f = self.new_file();
            }
        }
    }

    /// Write data to specified file at specified offset.
    pub fn write(&mut self, f: &mut FD, offset: u64, data: &[u8]) {
        let data = Arc::new(data.to_vec());
        self.write_data(f, offset, data);
    }

    /// Write Data to specified file at specified offset.
    pub fn write_data(&mut self, f: &mut FD, offset: u64, data: Data) {
        #[cfg(feature = "log-div")]
        println!(
            "DS write_data f={:?} offset={} data len={}",
            f,
            offset,
            data.len()
        );

        self.allocate(f, offset + data.len() as u64);

        if f.blocks == 1 {
            let n = data.len();
            self.0.write_data(f.root, offset, data, 0, n);
        } else {
            self.write_blocks(f, offset, data);
        }
    }

    /// Read data from file at specified offset.
    pub fn read(&self, f: &FD, offset: u64, data: &mut [u8]) {
        #[cfg(feature = "log-div")]
        println!(
            "DS read_data f{:?} offset={} data len={}",
            f,
            offset,
            data.len()
        );
        if f.blocks == 1 {
            self.0.read(f.root, offset, data);
        } else {
            self.read_blocks(f, offset, data);
        }
    }

    /// Set root file descriptor.
    pub fn set_root(&mut self, fd: &FD) {
        let mut rsvd = [0; RSVD_SIZE];
        fd.save(&mut rsvd);
        self.0.set_rsvd(rsvd);
    }

    /// Get root file descriptor.
    pub fn get_root(&self, fd: &mut FD) {
        let rsvd = self.0.get_rsvd();
        fd.load(&rsvd);
    }

    /// Save file to backing storage.
    pub fn save(&mut self) {
        self.0.save();
    }

    /// Wait for save to complete.
    pub fn wait_complete(&self) {
        self.0.stg.wait_complete();
    }

    /// Allocate sufficient blocks for file of specified size.
    fn allocate(&mut self, f: &mut FD, size: u64) {
        #[cfg(feature = "log-div")]
        println!("DS allocate f={:?} size={} BASE={}", f, size, BASE);
        let reqd = Self::blocks(size);
        if reqd > f.blocks {
            let new_level = Self::levels(reqd);
            while f.level < new_level {
                let blk = self.0.new_block();
                self.set_num(blk, 0, f.root);
                f.root = blk;
                f.level += 1;
                #[cfg(feature = "log-div")]
                println!("DS allocate file level increased f={:?}", f);
            }
            self.add_blocks(f, reqd);
        }
    }

    fn write_blocks(&mut self, f: &FD, offset: u64, data: Data) {
        let (mut done, len) = (0, data.len());
        while done < len {
            let off = offset + done as u64;
            let (blk, off) = (off / BLK_CAP, off % BLK_CAP);
            let a = min(len - done, (BLK_CAP - off) as usize);
            let blk = self.get_block(f.root, f.level, blk);
            self.0.write_data(blk, off, data.clone(), done, a);
            done += a;
        }
    }

    fn read_blocks(&self, f: &FD, offset: u64, data: &mut [u8]) {
        let (mut done, len) = (0, data.len());
        while done < len {
            let off = offset + done as u64;
            let (blk, off) = (off / BLK_CAP, off % BLK_CAP);
            let a = min(len - done, (BLK_CAP - off) as usize);
            if blk < f.blocks {
                let blk = self.get_block(f.root, f.level, blk);
                self.0.read(blk, off, &mut data[done..done + a]);
            }
            done += a;
        }
    }

    fn add_blocks(&mut self, f: &mut FD, new: u64) {
        #[cfg(feature = "log-div")]
        println!("DS add blocks f={:?} new={}", f, new);
        for ix in f.blocks..new {
            let nb = self.0.new_block();
            self.set_block(f.root, f.level, ix, nb);
        }
        f.set_blocks(new);
    }

    fn reduce_blocks(&mut self, f: &mut FD, level: u8, old: u64, new: u64) {
        #[cfg(feature = "log-div")]
        println!(
            "DS reduce blocks f={:?} level={} old={} new={}",
            f, level, old, new
        );
        for ix in new..old {
            let blk = self.get_block(f.root, level, ix);
            self.0.drop_block(blk);
        }
    }

    /// Calculate the number of data blocks required for a file of specified size.
    fn blocks(size: u64) -> u64 {
        (size + BLK_CAP - 1) / BLK_CAP
    }

    /// Calculate the number of extra levels needed for specified number of data blocks.
    fn levels(blocks: u64) -> u8 {
        if blocks <= 1 {
            0
        } else {
            (blocks - 1).ilog(BASE) as u8 + 1
        }
    }

    /// Set the block at index ix at specified level.
    fn set_block(&mut self, mut blk: u64, level: u8, mut ix: u64, value: u64) {
        if level > 1 {
            let x = ix / BASE;
            ix %= BASE;
            blk = if ix == 0 {
                let nb = self.0.new_block();
                self.set_block(blk, level - 1, x, nb);
                nb
            } else {
                self.get_block(blk, level - 1, x)
            };
        }
        self.set_num(blk, ix * NSZ, value);
    }

    /// Get the block at index ix at specified level.
    fn get_block(&self, mut blk: u64, level: u8, mut ix: u64) -> u64 {
        if level > 1 {
            let x = ix / BASE;
            ix %= BASE;
            blk = self.get_block(blk, level - 1, x);
        }
        self.get_num(blk, ix * NSZ)
    }

    fn get_num(&self, blk: u64, off: u64) -> u64 {
        let mut bytes = [0; 8];
        self.0.read(blk, off, &mut bytes[0..NSZ as usize]);
        u64::from_le_bytes(bytes)
    }

    fn set_num(&mut self, blk: u64, off: u64, v: u64) {
        self.0.write(blk, off, &v.to_le_bytes()[0..NSZ as usize]);
    }
}

#[test]
fn divided_stg_test() {
    let stg = MemFile::new();
    let mut ds = DividedStg::new(stg.clone());

    // let fx = ds.new_file();
    let mut f = ds.new_file();
    let data = b"hello george";

    ds.write(&mut f, 0, data);

    let test_off = 2 * BLK_CAP;
    ds.write(&mut f, test_off, data);

    ds.save();

    let mut ds = DividedStg::new(stg.clone());

    let mut buf = vec![0; data.len()];
    ds.read(&f, 0, &mut buf);
    assert!(&buf == data);

    let mut buf = vec![0; data.len()];
    ds.read(&f, test_off, &mut buf);
    assert!(&buf == data);

    ds.truncate(&mut f, 10 * BLK_CAP);
    ds.drop_file(&mut f);
    ds.save();

    // ds.write(fx, 1, data);

    // let test_off = 2 * (BLK_CAP + 1) * BASE + 10;

    // let test_off = 4 * BLK_CAP;
    // f = ds.allocate(f, test_off + data.len() as u64);
    // ds.write(f, test_off, data);
    // ds.drop_file(fx);
    // ds.save();
    /*

        let ds = DividedStg::new(stg.clone());

        let mut buf = vec![0; data.len()];
        ds.read(f, test_off, &mut buf);
        assert!(&buf == data);

        let mut buf = vec![0; data.len()];
        ds.read(f, 0, &mut buf);
        assert!(&buf == data);

        // ds.truncate(f, 0);

        let ds = DividedStg::new(stg.clone());
        let mut buf = vec![0; data.len()];
        ds.read(f, test_off, &mut buf);
        assert!(&buf == data);
    */
}
