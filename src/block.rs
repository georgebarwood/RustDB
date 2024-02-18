use crate::{util, Arc, BTreeSet, Data, Storage};
use std::cmp::min;

/// Magic Value ( first word of file for version check).
const MAGIC_VALUE: [u8; 8] = *b"RDBV1.00";

/// Reserved area for client.
pub const RSVD_SIZE: usize = 24;

/// Size of file header.
const HSIZE: u64 = 40 + RSVD_SIZE as u64;

/// Log (base 2) of Block Size.
const LOG_BLK_SIZE: u8 = 17;

/// Size of block.
const BLK_SIZE: u64 = 1 << LOG_BLK_SIZE;

/// Number of bits for block number ( either logical or physical ).
const NUM_BITS: u8 = 64 - LOG_BLK_SIZE;

/// Bit mask for block number.
const NUM_MASK: u64 = (1 << NUM_BITS) - 1;

/// Special value used to terminate free logical block chain.
const NOTLB: u64 = NUM_MASK;

/// Number of bytes for block number, plus an extra bit (for ALLOC_BIT).
pub const NUM_SIZE: u64 = (NUM_BITS as u64 + 8) / 8;

/// Bit that indicates Logical Block Entry represents allocated phsyical page.
const ALLOC_BIT: u64 = 1 << NUM_BITS;

/// Capacity of block in bytes ( after allowing for number reserved for block number ).
pub const BLK_CAP: u64 = BLK_SIZE - NUM_SIZE;

/// Manages allocation and deallocation of numbered relocatable fixed size blocks from underlying Storage.
///
/// Blocks are numbered. A map of the physical location of each block is kept at the start of the storage (after the header).
///
/// Physical blocks can be relocated by adjusting the map entry to point to the new location.
///
/// On save, the map of free block numbers is processed and any associated physical blocks are freed.
///
/// When a physical block is freed, the last physical block is relocated to fill it.

pub struct BlockStg {
    pub(crate) stg: Box<dyn Storage>,
    lb_count: u64, // Number of Logical Block Info entries.
    pb_count: u64, // Number of Physical Blocks.
    pb_first: u64,
    first_free: u64,
    rsvd: [u8; RSVD_SIZE], // For boot-strapping first file.
    free: BTreeSet<u64>,   // Temporary set of free block numbers.
    header_dirty: bool,
    rsvd_dirty: bool,
    is_new: bool,
}

impl BlockStg {
    /// Construct BlockStg with specified underlying Storage.
    pub fn new(stg: Box<dyn Storage>) -> Self {
        let is_new = stg.size() == 0;
        let mut x = Self {
            stg,
            lb_count: 0,
            pb_count: 1,
            pb_first: 1,
            first_free: NOTLB,
            rsvd: [0; RSVD_SIZE],
            free: BTreeSet::default(),
            header_dirty: false,
            rsvd_dirty: false,
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

    /// Is this new storage.
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// Allocate a new block number.
    pub fn new_block(&mut self) -> u64 {
        if let Some(p) = self.free.pop_first() {
            return p;
        }
        let mut p = self.first_free;
        if p != NOTLB {
            self.first_free = self.get_binfo(p);
        } else {
            p = self.lb_count;
            self.lb_count += 1;
        }
        self.header_dirty = true;
        p
    }

    /// Release a block number ( no longer valid ).
    pub fn drop_block(&mut self, bn: u64) {
        self.free.insert(bn);
    }

    /// Write data to specified numbered block at specified offset.
    pub fn write(&mut self, bn: u64, offset: u64, data: &[u8]) {
        let n = data.len();
        let data = Arc::new(data.to_vec());
        self.write_data(bn, offset, data, 0, n);
    }

    /// Write slice of Data to specified numbered block at specified offset.
    pub fn write_data(&mut self, bn: u64, offset: u64, data: Data, s: usize, n: usize) {
        debug_assert!(!self.free.contains(&bn));

        #[cfg(feature = "log-block")]
        println!(
            "block write bn={} offset={:?} s={} n={} data={:?}",
            bn,
            offset,
            s,
            n,
            &data[s..s + min(n, 20)]
        );

        self.expand_binfo(bn);
        let mut pb = self.get_binfo(bn);
        if pb & ALLOC_BIT == 0 {
            pb = self.pb_count;
            self.pb_count += 1;

            #[cfg(feature = "log-block")]
            println!("Allocating physical block {} to block number {}", pb, bn);

            self.header_dirty = true;
            self.set_binfo(bn, ALLOC_BIT | pb);
            // Write block number at start of physical block, to allow relocation.
            self.write_num(pb * BLK_SIZE, bn);
        }
        pb &= NUM_MASK;
        assert!(NUM_SIZE + offset + n as u64 <= BLK_SIZE);
        let offset = pb * BLK_SIZE + NUM_SIZE + offset;
        self.stg.write_data(offset, data, s, n);
    }

    /// Read data from specified numbered block and offset.
    pub fn read(&self, bn: u64, offset: u64, data: &mut [u8]) {
        debug_assert!(!self.free.contains(&bn));

        let pb = self.get_binfo(bn);
        if pb & ALLOC_BIT != 0 {
            let pb = pb & NUM_MASK;
            let avail = BLK_SIZE - (NUM_SIZE + offset);
            let amount = min(data.len(), avail as usize);
            self.stg
                .read(pb * BLK_SIZE + NUM_SIZE + offset, &mut data[0..amount]);

            #[cfg(feature = "log-block")]
            println!(
                "block read bn={} off={} data len={} data={:?}",
                bn,
                offset,
                data.len(),
                &data[0..min(data.len(), 20)]
            );
        }
    }

    /// Set the reserved area in the storage header.
    pub fn set_rsvd(&mut self, rsvd: [u8; RSVD_SIZE]) {
        self.rsvd = rsvd;
        self.rsvd_dirty = true;
        self.header_dirty = true;
    }

    /// Get the reserved area from the storage header.
    pub fn get_rsvd(&self) -> [u8; RSVD_SIZE] {
        self.rsvd
    }

    /// Save changes to underlying storage.
    pub fn save(&mut self) {
        // Process the set of freed page numbers, adding any associated physical blocks to a map of free blocks.
        let flist = std::mem::take(&mut self.free);
        let mut free_blocks = BTreeSet::default();
        for bn in flist.iter().rev() {
            let bn = *bn;
            let info = self.get_binfo(bn);
            if info & ALLOC_BIT != 0 {
                let pp = info & NUM_MASK;
                free_blocks.insert(pp);
            }
            self.set_binfo(bn, self.first_free);
            self.first_free = bn;
            self.header_dirty = true;
        }

        // Relocate blocks from end of file to fill free blocks.
        while !free_blocks.is_empty() {
            self.pb_count -= 1;
            self.header_dirty = true;
            let last = self.pb_count;
            // If the last block is not a free block, relocate it using a free block.
            if !free_blocks.remove(&last) {
                let to = free_blocks.pop_first().unwrap();
                self.relocate(last, to);
            }
        }

        if self.header_dirty {
            self.write_header();
            self.header_dirty = false;
        }

        self.stg.commit(self.pb_count * BLK_SIZE);
    }

    /// Write header fields to underlying storage.
    fn write_header(&mut self) {
        self.stg.write_u64(8, self.pb_count);
        self.stg.write_u64(16, self.lb_count);
        self.stg.write_u64(24, self.first_free);
        self.stg.write_u64(32, self.pb_first);
        if self.rsvd_dirty {
            self.stg.write(40, &self.rsvd);
            self.rsvd_dirty = false;
        }
    }

    /// Read the header fields from underlying storage.
    fn read_header(&mut self) {
        self.pb_count = self.stg.read_u64(8);
        self.lb_count = self.stg.read_u64(16);
        self.first_free = self.stg.read_u64(24);
        self.pb_first = self.stg.read_u64(32);
        self.stg.read(40, &mut self.rsvd);
    }

    /// Relocate physical block, from and to are block numbers.
    fn relocate(&mut self, from: u64, to: u64) {
        if from == to {
            return;
        }

        let mut buf = vec![0; BLK_SIZE as usize];
        self.stg.read(from * BLK_SIZE, &mut buf);
        let bn = util::get(&buf, 0, NUM_SIZE as usize);

        #[cfg(feature = "log-block")]
        println!("Relocating block from={} to={} bn={}", from, to, bn);

        assert_eq!(self.get_binfo(bn), ALLOC_BIT | from);

        self.set_binfo(bn, ALLOC_BIT | to);
        self.stg.write(to * BLK_SIZE, &buf);
    }

    /// Expand the map to accomodate the specified block number.
    fn expand_binfo(&mut self, bn: u64) {
        let target = HSIZE + bn * NUM_SIZE + NUM_SIZE;
        while target > self.pb_first * BLK_SIZE {
            #[cfg(feature = "log-block")]
            println!(
                "expand_binfo bn={} target={} pb_first={} pb_count={} lb_count={}",
                bn, target, self.pb_first, self.pb_count, self.lb_count
            );
            self.relocate(self.pb_first, self.pb_count);
            self.clear_block(self.pb_first);
            self.pb_first += 1;
            self.pb_count += 1;
            self.header_dirty = true;
        }
    }

    /// Fill the specified physical block with zeroes.
    fn clear_block(&mut self, pb: u64) {
        let buf = vec![0; BLK_SIZE as usize];
        self.stg.write(pb * BLK_SIZE, &buf);
    }

    /// Set the value associated with the specified block number.
    fn set_binfo(&mut self, bn: u64, value: u64) {
        self.expand_binfo(bn);
        let off = HSIZE + bn * NUM_SIZE;
        self.write_num(off, value);
    }

    /// Get the value associated with the specified block number.
    fn get_binfo(&self, bn: u64) -> u64 {
        let off = HSIZE + bn * NUM_SIZE;
        if off + NUM_SIZE > self.pb_first * BLK_SIZE {
            return 0;
        }
        self.read_num(off)
    }

    /// Write number to specified offset in underlying storage.
    fn write_num(&mut self, offset: u64, num: u64) {
        self.stg
            .write(offset, &num.to_le_bytes()[0..NUM_SIZE as usize]);
        debug_assert_eq!(self.read_num(offset), num);
    }

    /// Read number from specified offset in underlying storage.
    fn read_num(&self, offset: u64) -> u64 {
        let mut bytes = [0; 8];
        self.stg.read(offset, &mut bytes[0..NUM_SIZE as usize]);
        u64::from_le_bytes(bytes)
    }
}

#[test]
fn block_test() {
    let data = b"hello there";
    let stg = crate::MemFile::new();
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
