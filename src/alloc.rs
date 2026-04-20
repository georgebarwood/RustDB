use pstd::{
    BoxA, RcA, RcStrA, StringA, VecA,
    collections::btree_map::CustomTuning,
    collections::{BTreeMapA, BTreeSetA, DefaultHashBuilder, HashMap},
};

pub use pstd::veca;
pub use pstd::localalloc::{GTemp, Local, Perm, Temp};

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

/// `Box` allocated from `GTemp`
pub type GBox<T> = BoxA<T, GTemp>;

/// `Rc` allocated from `GTemp`
pub type GRc<T> = RcA<T, GTemp>;

/// `String` allocated from `GTemp`
pub type GString = StringA<GTemp>;

/// `Vec` allocated from `GTemp`
pub type GVec<T> = VecA<T, GTemp>;

/// `BTreeMap` allocated from `GTemp`
pub type GBTreeMap<K, V> = BTreeMapA<K, V, CustomTuning<GTemp>>;

/// `BTreeMap` allocated from `Local`
pub type LBTreeMap<K, V> = BTreeMapA<K, V, CustomTuning<Local>>;

/// `BTreeSet` allocated from `Local`
pub type LBTreeSet<T> = BTreeSetA<T, CustomTuning<Local>>;

/// `HashMap` allocated from `Local`
pub type LHashMap<K, V> = HashMap<K, V, DefaultHashBuilder, Local>;

/// Create a `LHashMap`.
pub fn lhashmap<K, V>() -> LHashMap<K, V> {
    LHashMap::new_in(Local::new())
}

/// Macro to make a new LBox, place value into it, and unsize it.
#[macro_export]
macro_rules! lbox {
    ($val:expr) => {
        pstd::unsize_box!(LBox::new($val))
    };
}

/// Macro to make a new GBox, place value into it, and unsize it.
#[macro_export]
macro_rules! gbox {
    ($val:expr) => {
        pstd::unsize_box!(GBox::new($val))
    };
}
