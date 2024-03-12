use crate::{
    block::{BlockStg, RSVD_SIZE},
    util, Arc, Data, Storage,
};
use std::cmp::min;

/// Divides Storage into sub-files of arbitrary size using [BlockStg].
pub struct DividedStg {
    /// Underlying block storage.
    pub bs: BlockStg,
    /// Block capacity.
    pub blk_cap: u64,
    /// Number of block numbers that will fit in a block.
    base: u64,
}

/// Bytes required to save FD ( root, size ).
pub const FD_SIZE: usize = 8 + 8;

/// [DividedStg] File Descriptor.
pub struct FD {
    /// Root block.
    root: u64,
    /// File size in bytes.
    size: u64,
    /// Number of data blocks needed ( can be computed from file size ).
    blocks: u64,
    /// Number of levels needed ( can be computed from file size ).
    level: u8,
    /// Set true when the FD is updated.
    pub changed: bool,
}

impl FD {
    /// File size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Sets the file size and number of blocks required.
    fn set_size(&mut self, size: u64, blocks: u64) {
        self.changed = true;
        self.size = size;
        self.blocks = blocks;
    }
}

impl DividedStg {
    /// Construct DividedStg from specified Storage and block capacity.
    pub fn new(stg: Box<dyn Storage>, blk_cap: u64) -> Self {
        let bs = BlockStg::new(stg, blk_cap);
        let blk_cap = bs.blk_cap();
        let base = blk_cap / bs.nsz() as u64;
        Self { bs, base, blk_cap }
    }

    /// Get file descriptor for a new file.
    pub fn new_file(&mut self) -> FD {
        FD {
            root: self.bs.new_block(),
            level: 0,
            size: 0,
            blocks: 1,
            changed: true,
        }
    }

    /// Drop specified file.
    pub fn drop_file(&mut self, f: &mut FD) {
        self.truncate(f, 0);
        self.bs.drop_block(f.root);
    }

    /// Free blocks not required for file of specified size.
    pub fn truncate(&mut self, f: &mut FD, size: u64) {
        if size < f.size {
            let reqd = self.blocks(size);
            if reqd < f.blocks {
                let levels = self.levels(reqd);

                // Calculate new root
                let mut new_root = f.root;
                for _ in levels..f.level {
                    new_root = self.get_num(new_root, 0);
                }

                // For each level reduce the number of blocks.
                let mut level = f.level;
                let mut old = f.blocks;
                let mut new = reqd;
                while level > 0 && old != new {
                    self.reduce_blocks(f, level, old, new);
                    new = (new + self.base - 1) / self.base;
                    old = (old + self.base - 1) / self.base;
                    level -= 1;
                }
                if levels < f.level {
                    self.bs.drop_block(f.root);
                    f.root = new_root;
                    f.level = levels;
                }
            }
            f.set_size(size, reqd);
        }
    }

    /// Write data to specified file at specified offset.
    pub fn write(&mut self, f: &mut FD, offset: u64, data: &[u8]) {
        let n = data.len();
        let data = Arc::new(data.to_vec());
        self.write_data(f, offset, data, n);
    }

    /// Write Data to specified file at specified offset.
    pub fn write_data(&mut self, f: &mut FD, offset: u64, data: Data, n: usize) {
        self.allocate(f, offset + n as u64);

        if f.blocks == 1 {
            self.bs.set_data(f.root, offset, data, 0, n);
        } else {
            self.write_blocks(f, offset, data, n);
        }
    }

    /// Read data from file at specified offset.
    pub fn read(&self, f: &FD, offset: u64, data: &mut [u8]) {
        if f.blocks == 1 {
            self.bs.get(f.root, offset, data);
        } else {
            self.read_blocks(f, offset, data);
        }
    }

    /// Save fd to byte buffer.
    pub fn save_fd(&self, fd: &FD, buf: &mut [u8]) {
        debug_assert!(fd.level == self.levels(fd.blocks));
        debug_assert!(fd.blocks == self.blocks(fd.size));
        util::setu64(&mut buf[0..8], fd.root);
        util::setu64(&mut buf[8..16], fd.size);
    }

    /// Load fd from  byte buffer.
    pub fn load_fd(&self, buf: &[u8]) -> FD {
        let root = util::getu64(buf, 0);
        let size = util::getu64(buf, 8);
        let blocks = self.blocks(size);
        let level = self.levels(blocks);
        FD {
            root,
            size,
            blocks,
            level,
            changed: false,
        }
    }

    /// Set root file descriptor.
    pub fn set_root(&mut self, fd: &FD) {
        let mut rsvd = [0; RSVD_SIZE];
        self.save_fd(fd, &mut rsvd);
        self.bs.set_rsvd(rsvd);
    }

    /// Get root file descriptor.
    pub fn get_root(&self) -> FD {
        let rsvd = self.bs.get_rsvd();
        self.load_fd(&rsvd)
    }

    /// Save files to backing storage.
    pub fn save(&mut self) {
        self.bs.save();
    }

    /// Wait for save to complete.
    pub fn wait_complete(&self) {
        self.bs.wait_complete();
    }

    /// Allocate sufficient blocks for file of specified size.
    fn allocate(&mut self, f: &mut FD, size: u64) {
        if size > f.size {
            let reqd = self.blocks(size);
            if reqd > f.blocks {
                let new_level = self.levels(reqd);
                while f.level < new_level {
                    let blk = self.bs.new_block();
                    self.set_num(blk, 0, f.root);
                    f.root = blk;
                    f.level += 1;
                }
                self.add_blocks(f, reqd);
            }
            f.set_size(size, reqd);
        }
    }

    /// Write data to file at specified offset.
    fn write_blocks(&mut self, f: &FD, offset: u64, data: Data, n: usize) {
        let mut done = 0;
        while done < n {
            let off = offset + done as u64;
            let (blk, off) = (off / self.blk_cap, off % self.blk_cap);
            let a = min(n - done, (self.blk_cap - off) as usize);
            let blk = self.get_block(f.root, f.level, blk);
            self.bs.set_data(blk, off, data.clone(), done, a);
            done += a;
        }
    }

    /// Read data from file at specified offset.
    fn read_blocks(&self, f: &FD, offset: u64, data: &mut [u8]) {
        let (mut done, len) = (0, data.len());
        while done < len {
            let off = offset + done as u64;
            let (blk, off) = (off / self.blk_cap, off % self.blk_cap);
            let a = min(len - done, (self.blk_cap - off) as usize);
            if blk < f.blocks {
                let blk = self.get_block(f.root, f.level, blk);
                self.bs.get(blk, off, &mut data[done..done + a]);
            }
            done += a;
        }
    }

    /// Add data blocks up to specified number.
    fn add_blocks(&mut self, f: &mut FD, new: u64) {
        for ix in f.blocks..new {
            let nb = self.bs.new_block();
            self.set_block(f.root, f.level, ix, nb);
        }
    }

    /// Reduce blocks at specified level from old to new.
    fn reduce_blocks(&mut self, f: &mut FD, level: u8, old: u64, new: u64) {
        for ix in new..old {
            let blk = self.get_block(f.root, level, ix);
            self.bs.drop_block(blk);
        }
    }

    /// Calculate the number of data blocks required for a file of specified size.
    fn blocks(&self, size: u64) -> u64 {
        if size == 0 {
            return 1;
        }
        (size + self.blk_cap - 1) / self.blk_cap
    }

    /// Calculate the number of extra levels needed for specified number of data blocks.
    fn levels(&self, blocks: u64) -> u8 {
        if blocks <= 1 {
            0
        } else {
            (blocks - 1).ilog(self.base) as u8 + 1
        }
    }

    /// Set the block at index ix at specified level.
    fn set_block(&mut self, mut blk: u64, level: u8, mut ix: u64, value: u64) {
        if level > 1 {
            let x = ix / self.base;
            ix %= self.base;
            blk = if ix == 0 {
                let nb = self.bs.new_block();
                self.set_block(blk, level - 1, x, nb);
                nb
            } else {
                self.get_block(blk, level - 1, x)
            };
        }
        self.set_num(blk, ix, value);
    }

    /// Get the block at index ix at specified level.
    fn get_block(&self, mut blk: u64, level: u8, mut ix: u64) -> u64 {
        if level > 1 {
            let x = ix / self.base;
            ix %= self.base;
            blk = self.get_block(blk, level - 1, x);
        }
        self.get_num(blk, ix)
    }

    /// Get block number from block at specified index.
    fn get_num(&self, blk: u64, ix: u64) -> u64 {
        let nsz = self.bs.nsz();
        let mut bytes = [0; 8];
        self.bs.get(blk, ix * nsz as u64, &mut bytes[0..nsz]);
        u64::from_le_bytes(bytes)
    }

    /// Set block number in block at specified index.
    fn set_num(&mut self, blk: u64, ix: u64, v: u64) {
        let nsz = self.bs.nsz();
        self.bs.set(blk, ix * nsz as u64, &v.to_le_bytes()[0..nsz]);
    }
}

#[test]
fn divided_stg_test() {
    let blk_cap = 10000;
    let stg = crate::MemFile::new();
    let mut ds = DividedStg::new(stg.clone(), blk_cap);

    let mut f = ds.new_file();
    let data = b"hello george";

    ds.write(&mut f, 0, data);

    let test_off = 200 * blk_cap;
    ds.write(&mut f, test_off, data);

    ds.save();

    let mut ds = DividedStg::new(stg.clone(), blk_cap);

    let mut buf = vec![0; data.len()];
    ds.read(&f, 0, &mut buf);
    assert!(&buf == data);

    let mut buf = vec![0; data.len()];
    ds.read(&f, test_off, &mut buf);
    assert!(&buf == data);

    ds.truncate(&mut f, 10 * blk_cap);
    ds.drop_file(&mut f);
    ds.save();
}
