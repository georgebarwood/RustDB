use pstd::{
    Box, String, Vec,
    collections::btree_map::CustomTuning,
    collections::{BTreeMap, BTreeSet, DefaultHashBuilder, HashMap},
    rc::{Rc, RcStr},
};

pub use pstd::localalloc::{Local, Temp};

/// `Local::new()`
pub fn local() -> Local {
    Local::new()
}

/// `Temp::new()`
pub fn temp() -> Temp {
    Temp::new()
}

/// `Box` allocated from `Temp`
pub type TBox<T> = Box<T, Temp>;

/// Allocate a `TBox`.
pub fn tbox<T>(t: T) -> TBox<T> {
    TBox::new_in(t, temp())
}

/// `Vec` allocated from `Temp`
pub type TVec<T> = Vec<T, Temp>;

/// Create a `TVec`.
pub fn tvec<T>() -> TVec<T> {
    TVec::new_in(temp())
}

/// `Box` allocated from `Local`
pub type LBox<T> = Box<T, Local>;

/// Allocate a `LBox`.
pub fn lbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, local())
}

/// `Rc` allocated from `Local`
pub type LRc<T> = Rc<T, Local>;

/// `RcStr` allocated from `Local`
pub type LRcStr = RcStr<Local>;

/// Allocate a `LRc`.
pub fn lrc<T>(t: T) -> LRc<T> {
    LRc::new_in(t, local())
}

///  Create a `LRcStr`
pub fn lrcstr(s: &str) -> LRcStr {
    LRcStr::new_in(s, local())
}

/// `Vec` allocated from `Local`
pub type LVec<T> = Vec<T, Local>;

/// Create a `LVec`.
pub fn lvec<T>() -> LVec<T> {
    LVec::new_in(local())
}

/// Create a `LVec` with specified capacity.
pub fn lvec_with_capacity<T>(capacity: usize) -> LVec<T> {
    LVec::with_capacity_in(capacity, local())
}

/// `BTreeMap` allocated from `Local`
pub type LBTreeMap<K, V> = BTreeMap<K, V, CustomTuning<Local>>;

/// Create a `LBTreeMap`.
pub fn lbtreemap<K, V>() -> LBTreeMap<K, V> {
    LBTreeMap::new_in(local())
}

/// `BTreeSet` allocated from `Local`
pub type LBTreeSet<T> = BTreeSet<T, CustomTuning<Local>>;

/// Create a `LBTreeSet`.
pub fn lbtreeset<T>() -> LBTreeSet<T> {
    LBTreeSet::new_in(local())
}

/// `HashMap` allocated from `Local`
pub type LHashMap<K, V> = HashMap<K, V, DefaultHashBuilder, Local>;

/// Create a `LHashMap`.
pub fn lhashmap<K, V>() -> LHashMap<K, V> {
    LHashMap::new_in(local())
}

/// `std::boxed::Box` or `LBox` depending on whether dynbox feature is selected.
#[cfg(feature = "dynbox")]
pub type DBox<T> = LBox<T>;

/// `std::boxed::Box` or `LBox` depending on whether dynbox feature is selected.
#[cfg(not(feature = "dynbox"))]
pub type DBox<T> = std::boxed::Box<T>;

/// Allocate a `std::boxed::Box` or `LBox` depending on whether dynbox feature is selected.
#[cfg(feature = "dynbox")]
pub fn dbox<T>(t: T) -> LBox<T> {
    LBox::new_in(t, local())
}

/// Allocate a `std::boxed::Box` or `LBox` depending on whether dynbox feature is selected.
#[cfg(not(feature = "dynbox"))]
pub fn dbox<T>(t: T) -> std::boxed::Box<T> {
    std::boxed::Box::new(t)
}

/// `String` allocated from `Local`
pub type LString = String<Local>;

/// Convert `str` to `LString`.
pub fn lstring(s: &str) -> LString {
    String::from_str_in(s, local())
}

/// Convert `str` to `LBox<str>`.
pub fn lboxstr(s: &str) -> LBox<str> {
    LBox::<str>::from_str_in(s, local())
}

/// Convert `str` to `TBox<str>`.
pub fn tboxstr(s: &str) -> TBox<str> {
    TBox::<str>::from_str_in(s, temp())
}
