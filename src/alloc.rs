use pstd::{
    BoxA, RcA, RcStrA, StringA, VecA,
    collections::btree_map::CustomTuning,
    collections::{BTreeMap, BTreeSet, DefaultHashBuilder, HashMap},
};

pub use pstd::localalloc::{Local, Perm, Temp};

/// `Local::new()`
pub fn local() -> Local {
    Local::new()
}

/// `Temp::new()`
pub fn temp() -> Temp {
    Temp::new()
}

/// `Box` allocated from `Temp`
pub type TBox<T> = BoxA<T, Temp>;

/// `Vec` allocated from `Temp`
pub type TVec<T> = VecA<T, Temp>;

/// `Box` allocated from `Local`
pub type LBox<T> = BoxA<T, Local>;

/// `Rc` allocated from `Local`
pub type LRc<T> = RcA<T, Local>;

/// `RcStr` allocated from `Local`
pub type LRcStr = RcStrA<Local>;

/// `Vec` allocated from `Local`
pub type LVec<T> = VecA<T, Local>;

/// `String` allocated from `Local`
pub type LString = StringA<Local>;

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

/// Macro to make a new LBox, place value into it, and unsize it.
#[macro_export]
macro_rules! lbox {
    ($val:expr) => {
        pstd::unsize_box!(LBox::new($val))
    };
}
