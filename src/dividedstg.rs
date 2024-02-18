use crate::{block::*, *};
use std::cmp::min;

/// Divides Storage into sub-files of arbitrary size using [BlockStg].
pub struct DividedStg(BlockStg);

/// Block capacity.
const NUMS_PER_BLK: u64 = BLK_CAP / NUM_SIZE;

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
        self.level = level;
        self.changed = true;
    }
    fn set_blocks(&mut self, blocks: u64) {
        self.blocks = blocks;
        self.changed = true;
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

#[cfg(feature = "log")]
impl std::fmt::Debug for FD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!(
            "{},{}{}",
            self.root,
            self.blocks,
            if self.level == 2 { "!" } else { "" }
        ))
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
            blocks: 0,
            changed: true,
        }
    }

    /// Drop specified file.
    pub fn drop_file(&mut self, mut f: FD) {
        f = self.truncate(f, 0);
        self.0.drop_block(f.root)
    }

    /// Allocate sufficient blocks for file of specified size.
    #[must_use]
    pub fn allocate(&mut self, mut f: FD, size: u64) -> FD {
        // println!("DS allocate f.root={} size={}", f.root, size);
        if f.level == 0 {
            if size < BLK_CAP {
                return f;
            }
            let mut save = vec![0; BLK_CAP as usize];
            self.read(f, 0, &mut save);
            f.set_level(1);
            f.set_blocks(0);
            f = self.allocate(f, save.len() as u64);
            self.write(f, 0, &save);
        }

        let reqd = self.blocks(size);
        if reqd > f.blocks {
            if f.level == 1 && reqd > NUMS_PER_BLK {
                // Copy existing child block numbers to new block.
                let blk = self.0.new_block();
                let mut buf = vec![0; (f.blocks * NUM_SIZE) as usize];
                self.0.read(f.root, 0, &mut buf);
                self.0.write(blk, 0, &buf);

                self.set_block(f.root, 1, 0, blk);
                f.set_level(2);
                // println!("File {} reached level 2", f.root);
            }
            // println!("Adding {} blocks", reqd - f.blocks);
            f = self.add_blocks(f, reqd);
            // println!("Finished adding blocks");
        }
        f
    }

    /// Deallocate blocks not required for file of specified size.
    #[must_use]
    pub fn truncate(&mut self, mut f: FD, size: u64) -> FD {
        if f.level == 0 {
            // ToDo : maybe free physical block if size == 0
        } else {
            let need = self.blocks(size);
            if need < f.blocks {
                f = self.drop_blocks(f, need);
            }
        }
        f
    }

    /// Write Data to specified file at specified offset. allocate must be called before write..
    pub fn write_data(&mut self, f: FD, offset: u64, data: Data) {
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

    fn drop_blocks(&mut self, mut f: FD, new: u64) -> FD {
        for i in new..f.blocks {
            let blk = self.get_block(f.root, f.level, i);
            self.0.drop_block(blk);
        }
        if f.level == 2 {
            let new_l1 = (new + NUMS_PER_BLK - 1) / NUMS_PER_BLK;
            let old_l1 = (f.blocks + NUMS_PER_BLK - 1) / NUMS_PER_BLK;
            for i in new_l1..old_l1 {
                let blk = self.get_block(f.root, 1, i);
                self.0.drop_block(blk);
            }
        }
        f.set_blocks(new);
        f
    }
    fn set_block(&mut self, mut blk: u64, level: u8, mut ix: u64, value: u64) {
        if level == 2 {
            let x = ix / NUMS_PER_BLK;
            ix %= NUMS_PER_BLK;
            blk = if ix == 0 {
                let blk = self.0.new_block();
                self.set_block(blk, 1, x, blk);
                blk
            } else {
                self.get_block(blk, 1, x)
            };
        }
        self.set_num(blk, ix * NUM_SIZE, value);
    }

    fn get_block(&self, mut blk: u64, level: u8, mut ix: u64) -> u64 {
        if level == 2 {
            let x = ix / NUMS_PER_BLK;
            ix %= NUMS_PER_BLK;
            blk = self.get_block(blk, 1, x);
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

    fn blocks(&self, size: u64) -> u64 {
        (size + BLK_CAP - 1) / BLK_CAP
    }
}

#[test]
fn divided_stg_test() {
    let stg = MemFile::new();
    let mut ds = DividedStg::new(stg.clone());

    let fx = ds.new_file();
    let mut f = ds.new_file();
    let data = b"hello george";
    ds.write(fx, 1, data);

    // let test_off = 2 * (BLK_CAP + 1) * NUMS_PER_BLK + 10;

    let test_off = 4 * BLK_CAP;
    ds.write(f, 0, data);
    f = ds.allocate(f, test_off + data.len() as u64);
    ds.write(f, test_off, data);
    ds.drop_file(fx);
    ds.save();

    let ds = DividedStg::new(stg.clone());
    let mut buf = vec![0; data.len()];
    ds.read(f, 0, &mut buf);
    assert!(&buf == data);

    let mut buf = vec![0; data.len()];
    ds.read(f, test_off, &mut buf);
    assert!(&buf == data);

    // ds.truncate(f, 0);

    let ds = DividedStg::new(stg.clone());
    let mut buf = vec![0; data.len()];
    ds.read(f, test_off, &mut buf);
    assert!(&buf == data);
}
