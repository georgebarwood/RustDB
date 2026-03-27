//!
//! Values allocated from Temp and Local Allocators must be freed by the same thread that allocated them, or the program will abort.

use pstd::alloc::{Allocator, Global};
use pstd::collections::{HashMap, DefaultHashBuilder};
use pstd::collections::{BTreeMap, btree_map::CustomTuning};

use crate::RefCell;
use std::ptr::slice_from_raw_parts_mut;
use std::{alloc::Layout, ptr::NonNull};
use std::marker::PhantomData;

/// Box allocated from Temp
pub type TBox<T> = pstd::Box<T, Temp>;

/// Allocate a TBox.
pub fn tbox<T>(t: T) -> TBox<T> {
    TBox::new_in(t, Temp::new())
}

/// Vec allocated from Temp
pub type TVec<T> = pstd::Vec<T, Temp>;

/// Create a TVec.
pub fn tvec<T>() -> TVec<T> {
    TVec::new_in(Temp::new())
}

/// Box allocated from Local
pub type LBox<T> = pstd::Box<T, Local>;

/// Allocate a LBox.
pub fn lbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, Local::new())
}

/// Vec allocated from Local
pub type LVec<T> = pstd::Vec<T, Local>;

/// Create a LVec.
pub fn lvec<T>() -> LVec<T> {
    LVec::new_in(Local::new())
}

/// BTreeMap allocated from Local
pub type LBTreeMap<K, V> = BTreeMap<K, V, CustomTuning<Local>>;

/// Create a LBTreeMap .
pub fn lbtreemap<K, V>() -> LBTreeMap<K, V> {
    LBTreeMap::with_tuning(CustomTuning::default())
}

/// HashMap allocated from Local
pub type LHashMap<K, V> = HashMap<K, V, DefaultHashBuilder, Local>;

/// Create a LHashMap.
pub fn lhashmap<K, V>() -> LHashMap<K, V> {
    LHashMap::new_in(Local::new())
}

/// Allocate a Box or LBox depending on whether dynbox feature is selected.
#[cfg(feature = "dynbox")]
pub fn dbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, Local::new())
}

/// Allocate a Box or LBox depending on whether dynbox feature is selected.
#[cfg(not(feature = "dynbox"))]
pub fn dbox<T>(t: T) -> Box<T> {
    Box::new(t)
}

/// String allocated from Local
pub type LString = pstd::String<Local>;

/// Convert str to LString.
pub fn lstring(s: &str) -> LString {
    pstd::String::from_str_in( s, Local::new() )
}

thread_local! {
    static TA: RefCell<BumpAllocator> = RefCell::new(BumpAllocator::new(true,256*K));
    static LA: RefCell<BumpAllocator> = RefCell::new(BumpAllocator::new(false,0));
}

const USE_BUMP: bool = !cfg!(miri);
const K: usize = 1024;
const MAX_ALIGN: usize = 128;

/// Temp Allocator.
#[derive(Default, Clone)]
pub struct Temp
{
    pd : PhantomData<NonNull<()>> // To make Temp !Send and !Sync
}

impl Temp
{
    /// Create a Temp allocator
    pub fn new() -> Self
    {
        Self{ pd: PhantomData }
    }
}

unsafe impl pstd::alloc::Allocator for Temp {
    fn allocate(&self, lay: Layout) -> Result<NonNull<[u8]>, pstd::alloc::AllocError> {
        TA.with_borrow_mut(|a| a.allocate(lay))
    }

    unsafe fn deallocate(&self, p: NonNull<u8>, lay: Layout) {
        TA.with_borrow_mut(|a| a.deallocate(p, lay));
    }
}

/// Local Allocator.
#[derive(Default, Clone)]
pub struct Local
{
    pd : PhantomData<NonNull<()>> // To make Local !Send and !Sync
}

impl Local {
    /// Create a Local allocator
    pub fn new() -> Self
    {
        Self{ pd: PhantomData }
    }
    
    /// Enable Local bump allocation for current thread with default size (256KB).
    pub fn enable_bump() {
        Self::enable_bump_with(256 * K);
    }

    /// Enable Local bump allocation for current thread with specified size.
    pub fn enable_bump_with(mut size: usize) {
        if USE_BUMP {
            if size < 16 * K {
                size = 16 * K;
            }
            LA.with_borrow_mut(|a| a.enable_with(size));
        }
    }
}

unsafe impl pstd::alloc::Allocator for Local {
    fn allocate(&self, lay: Layout) -> Result<NonNull<[u8]>, pstd::alloc::AllocError> {
        LA.with_borrow_mut(|a| a.allocate(lay))
    }

    unsafe fn deallocate(&self, p: NonNull<u8>, lay: Layout) {
        LA.with_borrow_mut(|a| a.deallocate(p, lay));
    }
}

struct Block(NonNull<u8>);

impl Block {
    fn new(size: usize) -> Self {
        let p = if size > 0 {
            let lay = Layout::from_size_align(size, MAX_ALIGN).unwrap();
            let p = Global::allocate(&Global, lay).unwrap();
            let p = p.as_ptr().cast::<u8>();
            unsafe { NonNull::new_unchecked(p) }
        } else {
            NonNull::dangling()
        };
        Self(p)
    }

    fn alloc(&self, i: usize, n: usize) -> NonNull<[u8]> {
        let p = unsafe { self.0.as_ptr().add(i) };
        let p = slice_from_raw_parts_mut(p, n);
        unsafe { NonNull::new_unchecked(p) }
    }

    /*
    fn contains(&self, a: *mut u8, bsize: usize) -> bool {
        let start = self.0.as_ptr();
        let end = unsafe { start.add(bsize) };
        start <= a && a < end
    }
    */

    fn drop(&mut self, bsize: usize) {
        if self.0 != NonNull::dangling() {
            let lay = Layout::from_size_align(bsize, MAX_ALIGN).unwrap();
            unsafe { Global::deallocate(&Global, self.0, lay) }
            self.0 = NonNull::dangling();
        }
    }
}

struct BumpAllocator {
    bsize: usize, // Block size
    max_size: usize, // Limit on sizes that can be bump allocated
    alloc_count: u64, // Number of current allocations
    idx: usize, // Current bytes allocated from cur
    cur: Block, // Current block for allocation
    overflow: Vec<Block>, // List of used up blocks
    _alloc_bytes: usize, // Only for diagnostic purposes.
    _max_alloc: usize,
    _reset_count: usize,
    _total_count: usize,
    _total_alloc: usize,
    _temp: bool,
}

impl BumpAllocator {
    fn new(_temp: bool, bsize: usize) -> Self {
        Self {
            bsize,
            max_size: bsize / 4,
            alloc_count: 0,
            idx: 0,
            cur: Block::new(bsize),
            overflow: Vec::new(),
            _alloc_bytes: 0,
            _max_alloc: 0,
            _reset_count: 0,
            _total_count: 0,
            _total_alloc: 0,
            _temp,
        }
    }

    fn enable_with(&mut self, bsize: usize) {
        if self.alloc_count != 0
        {
            println!("enable_with alloc_count not zero, aborting");
            std::process::abort();
        }
        if self.bsize == 0 {
            self.bsize = bsize;
            self.max_size = bsize / 4;
            self.cur = Block::new(bsize);
        }
    }

    /*
    fn contains(&self, a: *mut u8) -> bool {
        if self.cur.contains(a, self.bsize) {
            return true;
        }
        for b in &self.overflow {
            if b.contains(a, self.bsize) {
                return true;
            }
        }
        false
    }
    */

    fn allocate(&mut self, lay: Layout) -> Result<NonNull<[u8]>, pstd::alloc::AllocError> {
        self.alloc_count += 1;
        let (n, m) = (lay.size(), lay.align());
        if USE_BUMP && n < self.max_size {
            #[cfg(feature = "log-bump")]
            {
                self._alloc_bytes += n;
                self._total_count += 1;
                self._total_alloc += n;
            }
            let mut i = self.idx.checked_next_multiple_of(m).unwrap();
            let e = i + n;
            // Make a new block if necessary.
            let bsize = self.bsize;
            if e >= bsize && (e > bsize || n == 0) {
                let old = std::mem::replace(&mut self.cur, Block::new(bsize));
                self.overflow.push(old);
                i = 0;
            }
            self.idx = i + n;
            Ok(self.cur.alloc(i, n))
        } else {
            Global::allocate(&Global, lay)
        }
    }

    fn deallocate(&mut self, p: NonNull<u8>, lay: Layout) {
        self.alloc_count -= 1;
        if USE_BUMP && lay.size() < self.max_size {
            /*
            let p = p.as_ptr();
            if !self.contains(p) {
                println!("Bad deallocate, aborting");
                std::process::abort();
            }
            */
            if self.alloc_count == 0 {
                #[cfg(feature = "log-bump")]
                {
                    self._reset_count += 1;
                    self._max_alloc = std::cmp::max(self._max_alloc, self._alloc_bytes);
                    self._alloc_bytes = 0;
                }
                self.idx = 0;
                self.reset_overflow();
            }
        } else {
            unsafe {
                Global::deallocate(&Global, p, lay);
            }
        }
    }

    fn reset_overflow(&mut self) {
        while let Some(mut b) = self.overflow.pop() {
            b.drop(self.bsize);
        }
    }
}

impl Drop for BumpAllocator {
    fn drop(&mut self) {
        if self.alloc_count != 0 {
            println!("BumpAllocator has outstanding allocations, aborting");
            std::process::abort();
        }

        #[cfg(feature = "log-bump")]
        println!(
            "Bump Allocator Dropped temp={} total_count={} total_alloc={} max_alloc={} reset_count={}",
            self._temp, self._total_count, self._total_alloc, self._max_alloc, self._reset_count
        );

        self.cur.drop(self.bsize);
        self.reset_overflow();
    }
}

#[test]
fn alloc_test() {
    {
        let b = tbox(99);
        assert_eq!(*b, 99);
    }
    for _i in 0..50 {
        let b = tbox(99);
        assert_eq!(*b, 99);
        let b = tbox(99);
        assert_eq!(*b, 99);
    }
}
