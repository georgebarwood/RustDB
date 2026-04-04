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

/// `Vec` allocated from `Temp`
pub type TVec<T> = Vec<T, Temp>;

/// `Box` allocated from `Local`
pub type LBox<T> = Box<T, Local>;

/// `Rc` allocated from `Local`
pub type LRc<T> = Rc<T, Local>;

/// `RcStr` allocated from `Local`
pub type LRcStr = RcStr<Local>;

/// `Vec` allocated from `Local`
pub type LVec<T> = Vec<T, Local>;

/// `String` allocated from `Local`
pub type LString = String<Local>;

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
