use crate::{block::*, *};
use std::cmp::min;

/// Divides Storage into sub-files of arbitrary size using [BlockStg].
pub struct DividedStg(BlockStg);

/// Block capacity.
const BASE: u64 = BLK_CAP / NUM_SIZE;

/// Bytes required to save FD ( root, blocks, level ).
pub const FD_SIZE: usize = 8 + 8 + 1;

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
    fn set_level(&mut self, level: u8) {
        self.changed = self.level != level;
        self.level = level;
    }
    fn set_blocks(&mut self, blocks: u64) {
        self.changed = self.blocks != blocks;
        self.blocks = blocks;
    }
    /// Save to byte buffer.
    pub fn save(&self, buf: &mut [u8]) {
        util::setu64(&mut buf[0..8], self.root);
        util::setu64(&mut buf[8..16], self.blocks);
        buf[16] = self.level;
    }
    /// Load from  byte buffer.
    pub fn load(&mut self, buf: &[u8]) {
        self.root = util::getu64(buf, 0);
        self.blocks = util::getu64(buf, 8);
        self.level = buf[16];
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
    pub fn drop_file(&mut self, f: FD) {
        #[cfg(feature = "log-div")]
        println!("DS drop_file f={:?}", f);
        let f = self.truncate(f, 0);
        self.0.drop_block(f.root);
    }

    /// Allocate sufficient blocks for file of specified size.
    #[must_use]
    pub fn allocate(&mut self, mut f: FD, size: u64) -> FD {
        #[cfg(feature = "log-div")]
        println!("DS allocate f={:?} size={} BASE={}", f, size, BASE);
        let reqd = Self::blocks(size);
        if reqd > f.blocks {
            // Increase level if necessary.
            while reqd > self.block_limit(f.level) {
                let blk = self.0.new_block();
                self.set_num(blk, 0, f.root);
                f.root = blk;
                f.set_level(f.level + 1);
                #[cfg(feature = "log-div")]
                println!(
                    "File {} reached level {} reqd={} limit={}",
                    f.root,
                    f.level,
                    reqd,
                    self.block_limit(f.level)
                );
            }
            #[cfg(feature = "log-div")]
            println!("Adding {} blocks", reqd - f.blocks);
            f = self.add_blocks(f, reqd);
        }
        f
    }

    /// Deallocate blocks not required for file of specified size.
    #[must_use]
    pub fn truncate(&mut self, mut f: FD, size: u64) -> FD {
        #[cfg(feature = "log-div")]
        println!("DS truncate f={:?} size={}", f, size);

        /* With BASE = 2, 5 data blocks looks like this:
           x
           x x
           x x x
           d d d d d

           If we only need 3 data blocks, it looks like this
           x
           x x
           d d d

           If we only need 2 data blocks, it goes to this:
           x
           d d
        */
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
            while level > 0 {
                self.reduce_blocks(f, level, old, new);
                new = (new + BASE - 1) / BASE;
                old = (old + BASE - 1) / BASE;
                level -= 1;
            }
            if levels < f.level {
                self.0.drop_block(f.root);
                f.root = new_root;
                f.set_level(levels);
            }
            f.set_blocks(reqd);
        }
        if f.blocks == 0 {
            f = self.new_file();
        }
        f
    }

    /// Write Data to specified file at specified offset. allocate must be called before write..
    pub fn write_data(&mut self, f: FD, offset: u64, data: Data) {
        #[cfg(feature = "log-div")]
        println!("DS write_data f={:?} offset={}", f, offset);

        assert!(f.blocks >= Self::blocks(offset + data.len() as u64));

        if f.level == 0 {
            let n = data.len();
            self.0.write_data(f.root, offset, data, 0, n);
        } else {
            self.write_blocks(f, offset, data);
        }
    }

    /// Write data to specified file at specified offset. allocate must be called before write.
    pub fn write(&mut self, f: FD, offset: u64, data: &[u8]) {
        let data = Arc::new(data.to_vec());
        self.write_data(f, offset, data);
    }

    /// Read data from file at specified offset.
    pub fn read(&self, f: FD, offset: u64, data: &mut [u8]) {
        #[cfg(feature = "log-div")]
        println!("DS read_data f{:?} offset={}", f, offset);
        if f.level == 0 {
            self.0.read(f.root, offset, data);
        } else {
            self.read_blocks(f, offset, data);
        }
    }

    /// Set root file descriptor.
    pub fn set_root(&mut self, fd: FD) {
        let mut rsvd = [0; RSVD_SIZE];
        fd.save(&mut rsvd);
        self.0.set_rsvd(rsvd);
    }

    /// Get root file descriptor.
    pub fn get_root(&self) -> FD {
        let rsvd = self.0.get_rsvd();
        let mut fd = FD::default();
        fd.load(&rsvd);
        fd
    }

    /// Save file to backing storage.
    pub fn save(&mut self) {
        self.0.save();
    }

    /// Wait for save to complete.
    pub fn wait_complete(&self) {
        self.0.stg.wait_complete();
    }

    fn write_blocks(&mut self, f: FD, offset: u64, data: Data) {
        let mut done = 0;
        let n = data.len();
        while done < n {
            let off = offset + done as u64;
            let blk = off / BLK_CAP;
            let off = off - blk * BLK_CAP;
            let blk = self.get_block(f.root, f.level, blk);
            let amount = min(n - done, (BLK_CAP - off) as usize);
            self.0.write_data(blk, off, data.clone(), done, amount);
            done += amount;
        }
    }

    fn read_blocks(&self, f: FD, offset: u64, data: &mut [u8]) {
        let mut done = 0;
        let len = data.len();
        while done < len {
            let off = offset + done as u64;
            let blk = off / BLK_CAP;
            let off = off - blk * BLK_CAP;
            let amount = min(len - done, (BLK_CAP - off) as usize);
            if blk < f.blocks {
                let blk = self.get_block(f.root, f.level, blk);
                self.0.read(blk, off, &mut data[done..done + amount]);
            }
            done += amount;
        }
    }

    fn add_blocks(&mut self, mut f: FD, new: u64) -> FD {
        for i in f.blocks..new {
            let blk = self.0.new_block();
            self.set_block(f.root, f.level, i, blk);
        }
        f.set_blocks(new);
        f
    }

    fn reduce_blocks(&mut self, f: FD, level: u8, old: u64, new: u64) {
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

    /// Calculate the number of data blocks supported for a file of specified level.
    fn block_limit(&self, level: u8) -> u64 {
        BASE.pow(level.into())
    }

    /// Calculates the number of levels needed for specified number of data blocks.
    fn levels(blocks: u64) -> u8 {
        let mut level = 0;
        let mut x = 1;
        while x < blocks {
            level += 1;
            x *= BASE
        }
        level
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
        self.set_num(blk, ix * NUM_SIZE, value);
    }

    /// Get the block at index ix at specified level.
    fn get_block(&self, mut blk: u64, level: u8, mut ix: u64) -> u64 {
        if level > 1 {
            let x = ix / BASE;
            ix %= BASE;
            blk = self.get_block(blk, level - 1, x);
        }
        self.get_num(blk, ix * NUM_SIZE)
    }

    fn get_num(&self, blk: u64, off: u64) -> u64 {
        let mut bytes = [0; 8];
        self.0.read(blk, off, &mut bytes[0..NUM_SIZE as usize]);
        u64::from_le_bytes(bytes)
    }

    fn set_num(&mut self, blk: u64, off: u64, value: u64) {
        self.0
            .write(blk, off, &value.to_le_bytes()[0..NUM_SIZE as usize]);
    }
}

#[test]
fn divided_stg_test() {
    let stg = MemFile::new();
    let mut ds = DividedStg::new(stg.clone());

    // let fx = ds.new_file();
    let mut f = ds.new_file();
    let data = b"hello george";

    f = ds.allocate(f, data.len() as u64);
    ds.write(f, 0, data);

    let test_off = 2 * BLK_CAP;
    f = ds.allocate(f, test_off + data.len() as u64);
    ds.write(f, test_off, data);

    ds.save();

    let mut ds = DividedStg::new(stg.clone());

    let mut buf = vec![0; data.len()];
    ds.read(f, 0, &mut buf);
    assert!(&buf == data);

    let mut buf = vec![0; data.len()];
    ds.read(f, test_off, &mut buf);
    assert!(&buf == data);

    f = ds.truncate(f, 10 * BLK_CAP);
    ds.drop_file(f);
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
