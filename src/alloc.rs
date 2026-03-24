//!
//! Values allocated from Temp and Local Allocators must be freed by the same thread that allocated them, or the program will abort.

use pstd::alloc::{Allocator, Global};
use std::{alloc::Layout, cell::Cell, ptr::NonNull};

/// Temp Allocator.
pub struct Temp;

/// Local Allocator.
pub struct Local;

/// TBox = pstd::Box<T, Temp>
pub type TBox<T> = pstd::Box<T, Temp>;

/// Allocate a TBox.
pub fn tbox<T>(t: T) -> TBox<T> {
    TBox::new_in(t, Temp)
}

/// TVec = pstd::Vec<T, Temp>
pub type TVec<T> = pstd::Vec<T, Temp>;

/// Create a TVec.
pub fn tvec<T>() -> TVec<T> {
    TVec::new_in(Temp)
}

/// LBox = pstd::Box<T, Local>
pub type LBox<T> = pstd::Box<T, Local>;

/// Allocate a LBox.
pub fn lbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, Local)
}

/// LVec = pstd::Vec<T, Local>
pub type LVec<T> = pstd::Vec<T, Local>;

/// Create a LVec.
pub fn lvec<T>() -> LVec<T> {
    LVec::new_in(Local)
}

/// Allocate a Box or LBox depending on whether dynbox feature is selected.
#[cfg(feature = "dynbox")]
pub fn dbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, Local)
}

/// Allocate a Box or LBox depending on whether dynbox feature is selected.
#[cfg(not(feature = "dynbox"))]
pub fn dbox<T>(t: T) -> Box<T> {
    Box::new(t)
}

thread_local! {
    static TA: Cell<Option<Box<BumpAllocator>>> = Cell::new(BumpAllocator::new(true,1024*256));
    static LA: Cell<Option<Box<BumpAllocator>>> = const { Cell::new(None) };
}

const USE_BUMP: bool = !cfg!(miri);
const MAX_BUMP: usize = 1024;
const MAX_ALIGN: usize = 128;

unsafe impl pstd::alloc::Allocator for Temp {
    fn allocate(&self, lay: Layout) -> Result<NonNull<[u8]>, pstd::alloc::AllocError> {
        if lay.size() <= MAX_BUMP
            && let Some(mut a) = TA.take()
        {
            let result = a.allocate(lay);
            TA.set(Some(a));
            result
        } else {
            pstd::alloc::Global::allocate(&Global, lay)
        }
    }

    unsafe fn deallocate(&self, p: NonNull<u8>, lay: Layout) {
        if lay.size() <= MAX_BUMP
            && let Some(mut a) = TA.take()
        {
            a.deallocate(p, lay);
            TA.set(Some(a));
        } else {
            unsafe {
                pstd::alloc::Global::deallocate(&Global, p, lay);
            }
        }
    }
}

impl Local {
    /// Enable Local bump allocation for current thread with default size (256KB).
    pub fn enable_bump() {
        Self::enable_bump_with(256 * MAX_BUMP);
    }

    /// Enable Local bump allocation for current thread with specified size.
    pub fn enable_bump_with(mut size: usize) {
        if USE_BUMP {
            if size < 16 * MAX_BUMP {
                size = 16 * MAX_BUMP;
            }
            let mut a = LA.take();
            if a.is_none() {
                a = BumpAllocator::new(false, size);
            }
            LA.set(a);
        }
    }
}

unsafe impl pstd::alloc::Allocator for Local {
    fn allocate(&self, lay: Layout) -> Result<NonNull<[u8]>, pstd::alloc::AllocError> {
        if lay.size() <= MAX_BUMP
            && let Some(mut a) = LA.take()
        {
            let result = a.allocate(lay);
            LA.set(Some(a));
            result
        } else {
            pstd::alloc::Global::allocate(&Global, lay)
        }
    }

    unsafe fn deallocate(&self, p: NonNull<u8>, lay: Layout) {
        if lay.size() <= MAX_BUMP
            && let Some(mut a) = LA.take()
        {
            a.deallocate(p, lay);
            LA.set(Some(a));
        } else {
            unsafe {
                pstd::alloc::Global::deallocate(&Global, p, lay);
            }
        }
    }
}

struct Block(NonNull<[u8]>);

impl Block {
    fn new(size: usize) -> Self {
        let lay = Layout::from_size_align(size,MAX_ALIGN).unwrap();
        Self(Global::allocate(&Global, lay).unwrap())
    }
    fn contains(&self, addr: *const u8) -> bool {
        unsafe { (*self.0.as_ptr()).as_ptr_range().contains(&addr) }
    }
}

impl Drop for Block
{
    fn drop(&mut self)
    {
        let size = self.0.len();
        let lay = Layout::from_size_align(size,MAX_ALIGN).unwrap();
        let p = NonNull::new(self.0.as_ptr().cast::<u8>()).unwrap();
        unsafe { Global::deallocate(&Global, p, lay) }
    }
}

struct BumpAllocator {
    alloc_count: u64,
    idx: usize,
    cur: Block,
    overflow: Vec<Block>,
    _alloc_bytes: usize, // Only for diagnostic purposes.
    _max_alloc: usize,
    _reset_count: usize,
    _total_count: usize,
    _total_alloc: usize,
    _temp: bool,
}

impl BumpAllocator {
    fn new(_temp: bool, bsize: usize) -> Option<Box<Self>> {
        if USE_BUMP {
            Some(Box::new(Self {
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
            }))
        } else {
            None
        }
    }

    fn overflow_contains(&self, a: *const u8) -> bool {
        for b in &self.overflow {
            if b.contains(a) {
                return true;
            }
        }
        false
    }

    fn allocate(&mut self, lay: Layout) -> Result<NonNull<[u8]>, pstd::alloc::AllocError> {
        let m = lay.align();
        let mut i = self.idx.checked_next_multiple_of(m).unwrap();
        let n = lay.size();
        let bsize = self.cur.0.len();
        let mut e = i + n;
        if e >= bsize && ( e > bsize || n == 0 )
        {
            let old = std::mem::replace(&mut self.cur, Block::new(bsize));
            self.overflow.push(old);
            i = 0;
            e = n;
        }

        let p = self.cur.0.as_ptr();
        let p = unsafe { &raw mut (&mut (*p))[i..e] };

        self.idx = e;
        self.alloc_count += 1;
        #[cfg(feature = "log-bump")]
        {
            self._alloc_bytes += n;
            self._total_count += 1;
            self._total_alloc += n;
        }

        unsafe { Ok(NonNull::new_unchecked(p)) }
    }

    fn deallocate(&mut self, p: NonNull<u8>, _lay: Layout) {
        let p = p.as_ptr();
        if !self.cur.contains(p) && !self.overflow_contains(p) {
            println!("Bad deallocate, aborting");
            std::process::abort();
        }

        self.alloc_count -= 1;
        if self.alloc_count == 0 {
            // println!("reset alloc max={}", self.max_alloc);
            #[cfg(feature = "log-bump")]
            {
                self._reset_count += 1;
                self._max_alloc = std::cmp::max(self._max_alloc, self._alloc_bytes);
                self._alloc_bytes = 0;
            }
            self.idx = 0;
            self.overflow.clear();
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
    }
}

#[test]
fn alloc_test() {
    {
        let b = TBox::new_in(99, Temp);
        assert_eq!(*b, 99);
    }

    let start = std::time::Instant::now();

    for _i in 0..50 {
        let b = TBox::new_in(99, Temp);
        assert_eq!(*b, 99);
        let b = TBox::new_in(99, Temp);
        assert_eq!(*b, 99);
    }

    println!(
        "alloc_test time elapsed = {}",
        start.elapsed().as_nanos() as u64
    );
}
