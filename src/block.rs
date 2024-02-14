use crate::*;
use std::cmp::min;

/// Magic Value ( first word of file for version check).
const MAGIC_VALUE: [u8; 8] = *b"RDBF1099";

/// Size of file header.
const HSIZE: u64 = 40;

/// Log (base 2) of Block Size.
const LOG_BLK_SIZE: u8 = 17;
// const LOG_BLK_SIZE: u8 = 8; // Small block size for testing.

/// Size of block.
const BLK_SIZE: u64 = 1 << LOG_BLK_SIZE;

/// Number of bits for block number ( either logical or physical ).
const NUM_BITS: u8 = 64 - LOG_BLK_SIZE;

/// Bit mask for block number.
const NUM_MASK: u64 = (1 << NUM_BITS) - 1;

/// Special value used to terminate free logical block chain.
const NOTLB: u64 = NUM_MASK;

/// Number of bytes for block number, plus an extra bit (for ALLOC_BIT).
const NUM_SIZE: u64 = (NUM_BITS as u64 + 8) / 8;

/// Bit that indicates Logical Block Entry represents allocated phsyical page.
const ALLOC_BIT: u64 = 1 << NUM_BITS;

/// Manages allocation and deallocation of relocatable fixed size blocks.
pub struct BlockStg {
    stg: Box<dyn Storage>,
    lb_count: u64, // Number of Logical Block Info entries.
    pb_count: u64, // Number of Physical Blocks.
    pb_first: u64,
    first_free: u64,
    free: BTreeSet<u64>,          // Temporary set of free logical blocks
    physical_free: BTreeSet<u64>, // Temporary set of free physical blocks
    header_dirty: bool,
    is_new: bool,
}

impl BlockStg {
    ///
    pub fn new(stg: Box<dyn Storage>) -> Self {
        let is_new = stg.size() == 0;
        let mut x = Self {
            stg,
            lb_count: 0,
            pb_count: 1,
            pb_first: 1,
            first_free: NOTLB,
            free: BTreeSet::default(),
            physical_free: BTreeSet::default(),
            header_dirty: false,
            is_new,
        };
        let magic: u64 = crate::util::getu64(&MAGIC_VALUE, 0);
        if is_new {
            x.stg.write_u64(0, magic);
            x.write_header();
        } else {
            assert!(
                x.stg.read_u64(0) == magic,
                "Database File Invalid (maybe wrong version)"
            );
            x.read_header();
        }
        x
    }

    ///
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    ///
    pub fn save(&mut self) {
        let flist = std::mem::take(&mut self.free);
        for p in flist.iter().rev() {
            let p = *p;
            self.free_block(p);
        }

        // Relocate blocks from end of file to fill free blocks.
        while !self.physical_free.is_empty() {
            self.pb_count -= 1;
            self.header_dirty = true;
            let from = self.pb_count;
            // If the last block is not a free block, relocate it using a free block.
            if !self.physical_free.remove(&from) {
                let to = self.physical_free.pop_first().unwrap();
                self.relocate(from, to);
            }
        }

        if self.header_dirty {
            self.write_header();
            self.header_dirty = false;
        }

        self.stg.commit(self.pb_count * BLK_SIZE);
    }

    ///
    pub fn new_block(&mut self) -> u64 {
        if let Some(p) = self.free.pop_first() {
            return p;
        }
        let mut p = self.first_free;
        if p != NOTLB {
            self.first_free = self.next_free(p);
        } else {
            p = self.lb_count;
            self.lb_count += 1;
        }
        self.header_dirty = true;
        p
    }

    ///
    pub fn drop_block(&mut self, bn: u64) {
        self.free.insert(bn);
    }

    ///
    pub fn write(&mut self, bn: u64, off: u64, data: &[u8]) {
        debug_assert!(!self.free.contains(&bn));

        #[cfg(feature = "log")]
        println!(
            "block write bn={} off={:?} data len={} data={:?}",
            bn,
            off,
            data.len(),
            &data[0..min(data.len(), 20)]
        );

        self.expand_binfo(bn);
        let mut pb = self.get_binfo(bn);
        if pb & ALLOC_BIT == 0 {
            pb = if let Some(pb) = self.physical_free.pop_first() {
                pb
            } else {
                let pb = self.pb_count;
                self.pb_count += 1;
                self.header_dirty = true;
                pb
            };
            self.set_binfo(bn, ALLOC_BIT | pb);
            // Write block number at start of physical block, to allow relocation.
            self.write_num(pb * BLK_SIZE, bn);
        }
        pb &= NUM_MASK;
        assert!(NUM_SIZE + off + data.len() as u64 <= BLK_SIZE);
        self.stg.write(pb * BLK_SIZE + NUM_SIZE + off, data);
    }

    ///
    pub fn read(&self, bn: u64, off: u64, data: &mut [u8]) {
        debug_assert!(!self.free.contains(&bn));

        let pb = self.get_binfo(bn);
        if pb & ALLOC_BIT != 0 {
            let pb = pb & NUM_MASK;
            assert!(NUM_SIZE + off + data.len() as u64 <= BLK_SIZE);
            // println!("read bn={} pb={} off={}", bn, pb, off);
            self.stg.read(pb * BLK_SIZE + NUM_SIZE + off, data);

            #[cfg(feature = "log")]
            println!(
                "block read bn={} off={} data len={} data={:?}",
                bn,
                off,
                data.len(),
                &data[0..min(data.len(), 20)]
            );
        }
    }

    fn relocate(&mut self, from: u64, to: u64) {
        if from == to { return; }
        let mut buf = vec![0; BLK_SIZE as usize];
        self.stg.read(from * BLK_SIZE, &mut buf);
        let bn = util::get(&buf, 0, NUM_SIZE as usize);

        #[cfg(feature = "log")]
        println!("Relocating block from={} to={} bn={}", from, to, bn);

        assert!(self.get_binfo(bn) == ALLOC_BIT | from);

        self.set_binfo(bn, ALLOC_BIT | to);
        self.stg.write(to * BLK_SIZE, &buf);
    }

    fn expand_binfo(&mut self, bn: u64) {
        
        let target = HSIZE + bn * NUM_SIZE + NUM_SIZE;
        while target > self.pb_first * BLK_SIZE {

            #[cfg(feature = "log")]
            println!("expand_binfo bn={} target={} pb_first={} pb_count={} lb_count={}", bn, target, self.pb_first, self.pb_count, self.lb_count);
            self.relocate(self.pb_first, self.pb_count);
            self.clear_block(self.pb_first);
            self.pb_first += 1;
            self.pb_count += 1;
            self.header_dirty = true;
        }
    }

    fn clear_block(&mut self, pb: u64) {
        let buf = vec![0; BLK_SIZE as usize];
        self.stg.write(pb * BLK_SIZE, &buf);
    }

    fn set_binfo(&mut self, ix: u64, value: u64) {
        let off = HSIZE + ix * NUM_SIZE;
        self.expand_binfo(off + NUM_SIZE);
        self.write_num(off, value);
        // println!("set_binfo ix={} value={} alloc_bit={}", ix, value & NUM_MASK, value & ALLOC_BIT != 0);
    }

    fn get_binfo(&self, ix: u64) -> u64 {
        let off = HSIZE + ix * NUM_SIZE;
        if off + NUM_SIZE > self.pb_first * BLK_SIZE {
            return 0;
        }
        self.read_num(off)
    }

    fn write_header(&mut self) {
        self.stg.write_u64(8, self.pb_count);
        self.stg.write_u64(16, self.lb_count);
        self.stg.write_u64(24, self.first_free);
        self.stg.write_u64(32, self.pb_first);
    }

    fn read_header(&mut self) {
        self.pb_count = self.stg.read_u64(8);
        self.lb_count = self.stg.read_u64(16);
        self.first_free = self.stg.read_u64(24);
        self.pb_first = self.stg.read_u64(32);
    }

    fn free_block(&mut self, bn: u64) {
        let info = self.get_binfo(bn);
        if info & ALLOC_BIT != 0 {
            let pp = info & NUM_MASK;
            self.physical_free.insert(pp);
        }
        self.set_binfo(bn, self.first_free);
        self.first_free = bn;
        self.header_dirty = true;
    }

    fn next_free(&self, bn: u64) -> u64 {
        self.get_binfo(bn)
    }

    fn write_num(&mut self, off: u64, value: u64) {
        self.stg
            .write(off, &value.to_le_bytes()[0..NUM_SIZE as usize]);
        assert_eq!(self.read_num(off), value);
    }

    fn read_num(&self, off: u64) -> u64 {
        let mut bytes = [0; 8];
        self.stg.read(off, &mut bytes[0..NUM_SIZE as usize]);
        u64::from_le_bytes(bytes)
    }
}

#[test]
fn block_test() {
    let data = b"hello there";
    let stg = MemFile::new();
    let mut bf = BlockStg::new(stg.clone());
    let bnx = bf.new_block();
    let bny = bf.new_block();
    let bn = bf.new_block();

    bf.write(bnx, 2, data);
    bf.write(bny, 1, data);
    bf.write(bn, 0, data);
    let mut buf = vec![0; data.len()];
    bf.read(bn, 0, &mut buf);
    assert_eq!(&buf, &data);

    bf.drop_block(bnx);
    bf.drop_block(bny);

    bf.save();

    let bf = BlockStg::new(stg.clone());
    let mut buf = vec![0; data.len()];
    bf.read(bn, 0, &mut buf);
    assert_eq!(&buf, &data);
}

// *****************************************************************************************************************

/// Divides Storage into sub-files of arbitrary size using [BlockStg].
pub struct DividedStg(BlockStg);

/// Block capacity.
const BLK_CAP: u64 = BLK_SIZE - NUM_SIZE;
const NUMS_PER_BLK: u64 = BLK_CAP / NUM_SIZE;

/// Bytes required to save FD ( root, blocks, level ).
pub const FD_SIZE: usize = 8 + 8 + 1;

/// [DividedStg] File Descriptor.
#[derive(Clone, Copy, Default)]
pub struct FD {
    root: u64,
    blocks: u64, // Number of child blocks
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
    ///
    pub fn save(&self, buf: &mut [u8]) {
        util::setu64(&mut buf[0..8], self.root);
        util::setu64(&mut buf[8..16], self.blocks);
        buf[16] = self.level;
    }
    ///
    pub fn load(&mut self, buf: &[u8]) {
        self.root = util::getu64(buf, 0);
        self.blocks = util::getu64(buf, 8);
        self.level = buf[16];
    }
}

impl DividedStg {
    ///
    pub fn new(stg: Box<dyn Storage>) -> Self {
        Self(BlockStg::new(stg))
    }

    ///
    pub fn new_file(&mut self) -> FD {
        FD {
            root: self.0.new_block(),
            level: 0,
            blocks: 0,
            changed: true,
        }
    }

    ///
    pub fn drop_file(&mut self, f: FD) {
        // ToDo: check level and deallocate child blocks
        self.0.drop_block(f.root)
    }

    /// Allocate sufficient blocks for file of specified size.
    #[must_use]
    pub fn allocate(&mut self, mut f: FD, size: u64) -> FD {
        println!("DS allocate f.root={} size={}", f.root, size);
        if f.level == 0 {
            if size < BLK_CAP {
                return f;
            }
            let mut save = vec![0; BLK_CAP as usize - 1];
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
                println!("File {} reached level 2", f.root);
            }
            println!("Adding {} blocks", reqd - f.blocks);
            f = self.add_blocks(f, reqd);
            println!("Finished adding blocks");
        }
        f
    }

    ///
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

    /// allocate must be called before write ( although BLK_CAP bytes are pre-allocated ).
    pub fn write(&mut self, f: FD, offset: u64, data: &[u8]) {
        println!("DS write f.root={} f.level={} offset={} data len={}", f.root, f.level, offset, data.len());
        if f.level == 0 {
            self.0.write(f.root, offset, data);
        } else {
            self.write_blocks(f, offset, data);
            println!("DS write complete");
        }
    }

    ///
    pub fn read(&self, f: FD, offset: u64, data: &mut [u8]) {
        println!(
            "DS read f.root={} f.level=={} offset={} data len={}",
            f.root,
            f.level,
            offset,
            data.len()
        );
        if f.level == 0 {
            self.0.read(f.root, offset, data);
        } else {
            self.read_blocks(f, offset, data);
        }
    }

    ///
    pub fn save(&mut self) {
        self.0.save();
    }

    ///
    pub fn wait_complete(&self) {
        self.0.stg.wait_complete();
    }

    fn write_blocks(&mut self, f: FD, offset: u64, data: &[u8]) {
        let mut done = 0;
        let len = data.len();
        while done < len {
            let off = offset + done as u64;
            let blk = off / BLK_CAP;
            let off = off - blk * BLK_CAP;
            let blk = self.get_block(f.root, f.level, blk);
            let amount = min(len - done, (BLK_CAP - off) as usize);
            self.0.write(blk, off, &data[done..done + amount]);
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
        f.set_blocks(new);
        f
    }
    fn set_block(&mut self, blk: u64, level: u8, ix: u64, value: u64) {
        if level == 1 {
            self.set_num(blk, ix * NUM_SIZE, value)
        } else {
            debug_assert!(level == 2);
            let x = ix / NUMS_PER_BLK;
            let ix = ix % NUMS_PER_BLK;
            let blk = if ix == 0 {
                let blk = self.0.new_block();
                self.set_block(blk, 1, x, blk);
                blk
            } else {
                self.get_block(blk, 1, x)
            };
            self.set_num(blk, ix * NUM_SIZE, value)
        }
    }

    fn get_block(&self, blk: u64, level: u8, ix: u64) -> u64 {
        if level == 1 {
            self.get_num(blk, ix * NUM_SIZE)
        } else {
            debug_assert!(level == 2);
            let x = ix / NUMS_PER_BLK;
            let ix = ix % NUMS_PER_BLK;
            let blk = self.get_block(blk, 1, x);
            self.get_num(blk, ix * NUM_SIZE)
        }
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
    // ds.write(f, 0, data);
    f = ds.allocate(f, test_off + data.len() as u64);
    ds.write(f, test_off, data);
    ds.drop_file(fx);
    ds.save();

    let ds = DividedStg::new(stg.clone());
    // let mut buf = vec![0; data.len()];
    // ds.read(f, 0, &mut buf);
    // assert!(&buf == data);

    let mut buf = vec![0; data.len()];
    ds.read(f, test_off, &mut buf);
    assert!(&buf == data);

    /*
        ds.truncate(f, 0);
        ds.save();

        let ds = DividedStg::new(stg.clone());
        let mut buf = vec![0; data.len()];
        ds.read(f, 0, &mut buf);
        assert!(&buf != data);

        let mut buf = vec![0; data.len()];
        ds.read(f, test_off, &mut buf);
        assert!(&buf != data);
    */
}
