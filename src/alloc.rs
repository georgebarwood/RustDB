use pstd::alloc::Global;
use std::alloc::Layout;
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

/// Temp Allocator.
pub struct Temp;

/// Local Allocator.
pub struct Local;

/// TBox.
pub type TBox<T> = pstd::Box<T, Temp>;

/// Allocate a TBox.
pub fn tbox<T>(t: T) -> TBox<T> {
    TBox::new_in(t, Temp)
}

/// TVec.
pub type TVec<T> = pstd::Vec<T, Temp>;

/// Allocate a TVec.
pub fn tvec<T>() -> TVec<T> {
    TVec::new_in(Temp)
}

/// LBox.
pub type LBox<T> = pstd::Box<T, Local>;

/// Allocate a LBox.
pub fn lbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, Local)
}

/// Allocate a Box or LBox.
#[cfg(feature = "dynbox")]
pub fn dbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, Local)
}

/// Allocate a Box or LBox.
#[cfg(not(feature = "dynbox"))]
pub fn dbox<T>(t: T) -> Box<T> {
    Box::new(t)
}

/// LVec.
pub type LVec<T> = pstd::Vec<T, Local>;

/// Allocate a LVec.
pub fn lvec<T>() -> LVec<T> {
    LVec::new_in(Local)
}

thread_local! {
    static TA: Cell<Option<Box<BumpAllocator>>> = Cell::new(BumpAllocator::new(true));
    static LA: Cell<Option<Box<BumpAllocator>>> = const { Cell::new(None) };
}

const USE_BUMP: bool = !cfg!(miri);
const MAX_BUMP: usize = 1024;

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
    /// Enable Local bump allocation for current thread.
    pub fn enable_bump() {
        if USE_BUMP {
            let mut a = LA.take();
            if a.is_none() {
                a = BumpAllocator::new(false);
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

const N: usize = 1024 * 16;

#[repr(align(128))]
struct Block([MaybeUninit<u8>; N]);

impl Block {
    fn new() -> Box<Self> {
        Box::new(Self([MaybeUninit::uninit(); N]))
    }
}

struct BumpAllocator {
    alloc_count: u64,
    idx: usize,
    cur: Box<Block>,
    overflow: Vec<Box<Block>>,
    _alloc_bytes: usize, // Only for diagnostic purposes.
    _max_alloc: usize,
    _reset_count: usize,
    _total_count: usize,
    _total_alloc: usize,
    _temp: bool,
}

impl BumpAllocator {
    fn new(_temp: bool) -> Option<Box<Self>> {
        if USE_BUMP {
            Some(Box::new(Self {
                alloc_count: 0,
                idx: 0,
                cur: Block::new(),
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

    fn allocate(&mut self, lay: Layout) -> Result<NonNull<[u8]>, pstd::alloc::AllocError> {
        let m = lay.align();
        self.idx = self.idx.checked_next_multiple_of(m).unwrap();
        let n = lay.size();
        if self.idx + n > N {
            let old = std::mem::replace(&mut self.cur, Block::new());
            self.overflow.push(old);
            self.idx = 0;
            assert!(self.idx + n <= N);
        }

        let p = &raw mut self.cur.0[self.idx..self.idx + n] as *mut [u8];
        self.idx += n;
        self.alloc_count += 1;
        #[cfg(feature = "log-bump")]
        {
            self._alloc_bytes += n;
            self._total_count += 1;
            self._total_alloc += n;
        }
        unsafe { Ok(NonNull::new_unchecked(p)) }
    }

    fn deallocate(&mut self, _p: NonNull<u8>, _lay: Layout) {
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
            self.overflow = Vec::new();
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
