use crate::{util, Arc, BTreeSet, Data, Storage};
use std::cmp::min;

/// Magic Value ( first word of file for version check).
const MAGIC_VALUE: [u8; 8] = *b"RDBV1.06";

/// Reserved area for client.
pub const RSVD_SIZE: usize = 16;

/// Size of file header.
const HSIZE: u64 = 48 + RSVD_SIZE as u64;

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
    stg: Box<dyn Storage>,
    lb_count: u64, // Number of Logical Block Info entries.
    pb_count: u64, // Number of Physical Blocks.
    pb_first: u64,
    first_free: u64,
    rsvd: [u8; RSVD_SIZE], // For boot-strapping first file.
    free: BTreeSet<u64>,   // Temporary set of free block numbers.
    header_dirty: bool,
    rsvd_dirty: bool,
    is_new: bool,
    blk_size: u64, // Block Size including block number.
    nsz: usize,    // Number of bytes for block number.
}

impl BlockStg {
    /// Blocks required for file header.
    fn hblks(bs: u64) -> u64 {
        (HSIZE + bs - 1) / bs
    }

    /// Number of bits for block number ( either logical or physical ).
    fn num_bits(bc: u64) -> usize {
        64 - bc.ilog(2) as usize
    }

    /// Number of bytes for block number, plus an extra bit (for self.alloc_bit()).
    fn calc_nsz(bc: u64) -> usize {
        (Self::num_bits(bc) + 8) / 8
    }

    /// Bit that indicates Logical Block Entry represents allocated phsyical page.
    fn alloc_bit0(nsz: usize) -> u64 {
        1 << (nsz * 8 - 1)
    }

    /// Bit mask for block number.
    fn num_mask0(nsz: usize) -> u64 {
        Self::alloc_bit0(nsz) - 1
    }

    fn num_mask(&self) -> u64 {
        Self::num_mask0(self.nsz)
    }

    fn alloc_bit(&self) -> u64 {
        Self::alloc_bit0(self.nsz)
    }

    /// Construct BlockStg with specified underlying Storage and block capacity.
    /// For existing file, block capacity will be read from file header.
    pub fn new(stg: Box<dyn Storage>, blk_cap: u64) -> Self {
        let is_new = stg.size() == 0;
        let nsz = Self::calc_nsz(blk_cap);
        let blk_size = blk_cap + nsz as u64;
        let hblks = Self::hblks(blk_size);

        let mut x = Self {
            stg,
            lb_count: 0,
            pb_count: hblks,
            pb_first: hblks,
            first_free: Self::num_mask0(nsz),
            rsvd: [0; RSVD_SIZE],
            free: BTreeSet::default(),
            header_dirty: false,
            rsvd_dirty: false,
            is_new,
            blk_size,
            nsz,
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

            #[cfg(feature = "log")]
            println!(
                "BlockStg::new allocated blocks={} first={}",
                x.pb_count - x.pb_first,
                x.pb_first
            );
        }
        x
    }

    /// Get the block capacity.
    pub fn blk_cap(&self) -> u64 {
        self.blk_size - self.nsz as u64
    }

    /// Get size of block number in bytes.
    pub fn nsz(&self) -> usize {
        self.nsz
    }

    /// Is this new storage.
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// Allocate a new block number.
    #[cfg(feature = "log-block")]
    pub fn new_block(&mut self) -> u64 {
        let bn = self.alloc_block();
        println!("BlockStg::new_block bn={}", bn);
        bn
    }

    /// Allocate a new block number.
    #[cfg(not(feature = "log-block"))]
    pub fn new_block(&mut self) -> u64 {
        self.alloc_block()
    }

    /// Release a block number ( no longer valid ).
    pub fn drop_block(&mut self, bn: u64) {
        #[cfg(feature = "log-block")]
        println!("BlockStg::drop_block bn={}", bn);

        assert!(!self.free.contains(&bn)); // Not a comprehensive check as bn could be in free chain.
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
            "BlockStg::write_data bn={bn} off={offset} s={s} n={n} data={:?}",
            &data[s..s + min(n, 20)]
        );

        self.expand_binfo(bn);
        let mut pb = self.get_binfo(bn);
        if pb & self.alloc_bit() == 0 {
            pb = self.pb_count;
            self.pb_count += 1;

            #[cfg(feature = "log-block")]
            println!("Allocating physical block {} to block number {}", pb, bn);

            self.header_dirty = true;
            self.set_binfo(bn, self.alloc_bit() | pb);
            // Write block number at start of physical block, to allow relocation.
            self.set_num(pb * self.blk_size, bn);
        }
        pb &= self.num_mask();
        assert!(self.nsz as u64 + offset + n as u64 <= self.blk_size);
        let offset = pb * self.blk_size + self.nsz as u64 + offset;
        self.stg.write_data(offset, data, s, n);
    }

    /// Read data from specified numbered block and offset.
    pub fn read(&self, bn: u64, offset: u64, data: &mut [u8]) {
        debug_assert!(!self.free.contains(&bn), "bn={}", bn);

        let pb = self.get_binfo(bn);
        if pb & self.alloc_bit() != 0 {
            let pb = pb & self.num_mask();
            let avail = self.blk_size - (self.nsz as u64 + offset);
            let n = min(data.len(), avail as usize);
            self.stg.read(
                pb * self.blk_size + self.nsz as u64 + offset,
                &mut data[0..n],
            );

            #[cfg(feature = "log-block")]
            println!(
                "BlockStg::read bn={bn} off={offset} data len={} data={:?}",
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
            if info & self.alloc_bit() != 0 {
                let pb = info & self.num_mask();
                #[cfg(feature = "log-block")]
                println!("BlockStg::save free physical block {}", pb);
                free_blocks.insert(pb);
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

        #[cfg(feature = "log")]
        println!(
            "BlockStg::save allocated blocks={}",
            self.pb_count - self.pb_first
        );

        self.stg.commit(self.pb_count * self.blk_size);
    }

    /// Wait for save to complete.
    pub fn wait_complete(&self) {
        self.stg.wait_complete();
    }

    /// Write header fields to underlying storage.
    fn write_header(&mut self) {
        self.stg.write_u64(8, self.pb_count);
        self.stg.write_u64(16, self.lb_count);
        self.stg.write_u64(24, self.first_free);
        self.stg.write_u64(32, self.pb_first);
        self.stg.write_u64(40, self.blk_cap());
        if self.rsvd_dirty {
            self.stg.write(48, &self.rsvd);
            self.rsvd_dirty = false;
        }
    }

    /// Read the header fields from underlying storage.
    fn read_header(&mut self) {
        self.pb_count = self.stg.read_u64(8);
        self.lb_count = self.stg.read_u64(16);
        self.first_free = self.stg.read_u64(24);
        self.pb_first = self.stg.read_u64(32);
        let blk_cap = self.stg.read_u64(40);
        self.nsz = Self::calc_nsz(blk_cap);
        self.blk_size = blk_cap + self.nsz as u64;
        self.stg.read(48, &mut self.rsvd);
    }

    /// Allocate a new block number.
    fn alloc_block(&mut self) -> u64 {
        if let Some(bn) = self.free.pop_first() {
            bn
        } else {
            let mut bn = self.first_free;
            if bn != self.num_mask() {
                self.first_free = self.get_binfo(bn);
            } else {
                bn = self.lb_count;
                self.lb_count += 1;
            }
            self.header_dirty = true;
            bn
        }
    }

    /// Relocate physical block, from and to are block numbers.
    fn relocate(&mut self, from: u64, to: u64) {
        if from == to {
            return;
        }

        let mut buf = vec![0; self.blk_size as usize];
        self.stg.read(from * self.blk_size, &mut buf);

        let bn = util::get(&buf, 0, self.nsz);

        #[cfg(feature = "log-block")]
        println!("BlockStg::relocate from={} to={} bn={}", from, to, bn);

        assert_eq!(self.get_binfo(bn), self.alloc_bit() | from);

        self.set_binfo(bn, self.alloc_bit() | to);
        self.stg.write_vec(to * self.blk_size, buf);
    }

    /// Expand the map to accomodate the specified block number.
    fn expand_binfo(&mut self, bn: u64) {
        let target = HSIZE + (bn + 1) * self.nsz as u64;
        while target > self.pb_first * self.blk_size {
            #[cfg(feature = "log-block")]
            println!(
                "BlockStg::expand_binfo bn={} target={} pb_first={} pb_count={} lb_count={}",
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
        let buf = vec![0; self.blk_size as usize];
        self.stg.write_vec(pb * self.blk_size, buf);
    }

    /// Set the value associated with the specified block number.
    fn set_binfo(&mut self, bn: u64, value: u64) {
        self.expand_binfo(bn);
        let off = HSIZE + bn * self.nsz as u64;
        self.set_num(off, value);
    }

    /// Get the value associated with the specified block number.
    fn get_binfo(&self, bn: u64) -> u64 {
        let off = HSIZE + bn * self.nsz as u64;
        if off + self.nsz as u64 > self.pb_first * self.blk_size {
            return 0;
        }
        self.get_num(off)
    }

    /// Write number to specified offset in underlying storage.
    fn set_num(&mut self, offset: u64, num: u64) {
        self.stg.write(offset, &num.to_le_bytes()[0..self.nsz]);
        debug_assert_eq!(self.get_num(offset), num);
    }

    /// Read number from specified offset in underlying storage.
    fn get_num(&self, offset: u64) -> u64 {
        let mut bytes = [0; 8];
        self.stg.read(offset, &mut bytes[0..self.nsz]);
        u64::from_le_bytes(bytes)
    }
}

#[test]
fn block_test() {
    let blk_cap = 10000;
    let data = b"hello there";
    let stg = crate::MemFile::new();
    let mut bf = BlockStg::new(stg.clone(), blk_cap);
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

    let bf = BlockStg::new(stg.clone(), blk_cap);
    let mut buf = vec![0; data.len()];
    bf.read(bn, 0, &mut buf);
    assert_eq!(&buf, &data);
}
