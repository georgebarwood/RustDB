/* Derived from std library BTreeMap tests ( https://github.com/rust-lang/rust/blob/master/library/alloc/src/collections/btree/map/tests.rs ), but has been hacked around a lot, many tests removed for various reasons.
   Some could be restored if I ever find time.
*/

use crate::Entry::*;
use crate::*;

use std::fmt::Debug;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

#[test]
fn test_basic_large() {
    let mut map = BTreeMap::new();
    // Miri is too slow
    let size = 10000;
    let size = size + (size % 2); // round up to even number
    assert_eq!(map.len(), 0);

    for i in 0..size {
        assert_eq!(map.insert(i, 10 * i), None);
        assert_eq!(map.len(), i + 1);
    }

    assert_eq!(map.first_key_value(), Some((&0, &0)));
    assert_eq!(
        map.last_key_value(),
        Some((&(size - 1), &(10 * (size - 1))))
    );
    assert_eq!(map.first_entry().unwrap().key(), &0);
    assert_eq!(map.last_entry().unwrap().key(), &(size - 1));

    for i in 0..size {
        assert_eq!(map.get(&i).unwrap(), &(i * 10));
    }

    for i in size..size * 2 {
        assert_eq!(map.get(&i), None);
    }

    for i in 0..size {
        assert_eq!(map.insert(i, 100 * i), Some(10 * i));
        assert_eq!(map.len(), size);
    }

    for i in 0..size {
        assert_eq!(map.get(&i).unwrap(), &(i * 100));
    }

    for i in 0..size / 2 {
        assert_eq!(map.remove(&(i * 2)), Some(i * 200));
        assert_eq!(map.len(), size - i - 1);
    }

    for i in 0..size / 2 {
        assert_eq!(map.get(&(2 * i)), None);
        assert_eq!(map.get(&(2 * i + 1)).unwrap(), &(i * 200 + 100));
    }

    for i in 0..size / 2 {
        assert_eq!(map.remove(&(2 * i)), None);
        assert_eq!(map.remove(&(2 * i + 1)), Some(i * 200 + 100));
        assert_eq!(map.len(), size / 2 - i - 1);
    }
    map.check();
}

#[test]
fn test_basic_small() {
    let mut map = BTreeMap::new();
    // Empty, root is absent (None):
    assert_eq!(map.remove(&1), None);
    assert_eq!(map.len(), 0);
    assert_eq!(map.get(&1), None);
    assert_eq!(map.get_mut(&1), None);
    assert_eq!(map.first_key_value(), None);
    assert_eq!(map.last_key_value(), None);
    assert_eq!(map.keys().count(), 0);
    assert_eq!(map.values().count(), 0);
    assert_eq!(map.range(..).next(), None);
    assert_eq!(map.range(..1).next(), None);
    assert_eq!(map.range(1..).next(), None);
    assert_eq!(map.range(1..=1).next(), None);
    assert_eq!(map.range(1..2).next(), None);

    assert_eq!(map.insert(1, 1), None);

    map.check();

    // 1 key-value pair:
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&1), Some(&1));
    assert_eq!(map.get_mut(&1), Some(&mut 1));
    assert_eq!(map.first_key_value(), Some((&1, &1)));
    assert_eq!(map.last_key_value(), Some((&1, &1)));
    assert_eq!(map.keys().collect::<Vec<_>>(), vec![&1]);
    assert_eq!(map.values().collect::<Vec<_>>(), vec![&1]);
    assert_eq!(map.insert(1, 2), Some(1));
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&1), Some(&2));
    assert_eq!(map.get_mut(&1), Some(&mut 2));
    assert_eq!(map.first_key_value(), Some((&1, &2)));
    assert_eq!(map.last_key_value(), Some((&1, &2)));
    assert_eq!(map.keys().collect::<Vec<_>>(), vec![&1]);
    assert_eq!(map.values().collect::<Vec<_>>(), vec![&2]);
    assert_eq!(map.insert(2, 4), None);

    map.check();

    // 2 key-value pairs:
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&2), Some(&4));
    assert_eq!(map.get_mut(&2), Some(&mut 4));
    assert_eq!(map.first_key_value(), Some((&1, &2)));
    assert_eq!(map.last_key_value(), Some((&2, &4)));
    assert_eq!(map.keys().collect::<Vec<_>>(), vec![&1, &2]);
    assert_eq!(map.values().collect::<Vec<_>>(), vec![&2, &4]);
    assert_eq!(map.remove(&1), Some(2));

    map.check();

    // 1 key-value pair:
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&1), None);
    assert_eq!(map.get_mut(&1), None);
    assert_eq!(map.get(&2), Some(&4));
    assert_eq!(map.get_mut(&2), Some(&mut 4));
    assert_eq!(map.first_key_value(), Some((&2, &4)));
    assert_eq!(map.last_key_value(), Some((&2, &4)));
    assert_eq!(map.keys().collect::<Vec<_>>(), vec![&2]);
    assert_eq!(map.values().collect::<Vec<_>>(), vec![&4]);
    assert_eq!(map.remove(&2), Some(4));

    map.check();

    // Empty but root is owned (Some(...)):
    assert_eq!(map.len(), 0);
    assert_eq!(map.get(&1), None);
    assert_eq!(map.get_mut(&1), None);
    assert_eq!(map.first_key_value(), None);
    assert_eq!(map.last_key_value(), None);
    assert_eq!(map.keys().count(), 0);
    assert_eq!(map.values().count(), 0);
    assert_eq!(map.range(..).next(), None);
    assert_eq!(map.range(..1).next(), None);
    assert_eq!(map.range(1..).next(), None);
    assert_eq!(map.range(1..=1).next(), None);
    assert_eq!(map.range(1..2).next(), None);
    assert_eq!(map.remove(&1), None);

    map.check();
}

#[test]
fn test_iter() {
    // Miri is too slow
    let size = if cfg!(miri) { 200 } else { 10000 };
    let mut map = BTreeMap::from_iter((0..size).map(|i| (i, i)));

    fn test<T>(size: usize, mut iter: T)
    where
        T: Iterator<Item = (usize, usize)>,
    {
        for i in 0..size {
            // assert_eq!(iter.size_hint(), (size - i, Some(size - i)));
            assert_eq!(iter.next().unwrap(), (i, i));
        }
        // assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
    }
    test(size, map.iter().map(|(&k, &v)| (k, v)));
    test(size, map.iter_mut().map(|(&k, &mut v)| (k, v)));
    test(size, map.into_iter());
}

#[test]
fn test_iter_rev() {
    // Miri is too slow
    let size = if cfg!(miri) { 200 } else { 10000 };
    let mut map = BTreeMap::from_iter((0..size).map(|i| (i, i)));

    fn test<T>(size: usize, mut iter: T)
    where
        T: Iterator<Item = (usize, usize)>,
    {
        for i in 0..size {
            // assert_eq!(iter.size_hint(), (size - i, Some(size - i)));
            assert_eq!(iter.next().unwrap(), (size - i - 1, size - i - 1));
        }
        // assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
    }
    test(size, map.iter().rev().map(|(&k, &v)| (k, v)));
    test(size, map.iter_mut().rev().map(|(&k, &mut v)| (k, v)));
    test(size, map.into_iter().rev());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(align(32))]
struct Align32(usize);

impl TryFrom<usize> for Align32 {
    type Error = ();

    fn try_from(s: usize) -> Result<Align32, ()> {
        Ok(Align32(s))
    }
}

#[test]
fn test_values_mut_mutation() {
    let mut a = BTreeMap::new();
    a.insert(1, String::from("hello"));
    a.insert(2, String::from("goodbye"));

    for value in a.values_mut() {
        value.push_str("!");
    }

    let values = Vec::from_iter(a.values().cloned());
    assert_eq!(values, [String::from("hello!"), String::from("goodbye!")]);
    a.check();
}

#[test]
fn test_iter_entering_root_twice() {
    let mut map = BTreeMap::from([(0, 0), (1, 1)]);
    let mut it = map.iter_mut();
    let front = it.next().unwrap();
    let back = it.next_back().unwrap();
    assert_eq!(front, (&0, &mut 0));
    assert_eq!(back, (&1, &mut 1));
    *front.1 = 24;
    *back.1 = 42;
    assert_eq!(front, (&0, &mut 24));
    assert_eq!(back, (&1, &mut 42));
    assert_eq!(it.next(), None);
    assert_eq!(it.next_back(), None);
}

#[test]
fn test_iter_mixed() {
    // Miri is too slow
    let size = if cfg!(miri) { 200 } else { 10000 };

    let mut map = BTreeMap::from_iter((0..size).map(|i| (i, i)));

    fn test<T>(size: usize, mut iter: T)
    where
        T: Iterator<Item = (usize, usize)> + DoubleEndedIterator,
    {
        for i in 0..size / 4 {
            // assert_eq!(iter.size_hint(), (size - i * 2, Some(size - i * 2)));
            assert_eq!(iter.next().unwrap(), (i, i));
            assert_eq!(iter.next_back().unwrap(), (size - i - 1, size - i - 1));
        }
        for i in size / 4..size * 3 / 4 {
            // assert_eq!(iter.size_hint(), (size * 3 / 4 - i, Some(size * 3 / 4 - i)));
            assert_eq!(iter.next().unwrap(), (i, i));
        }
        // assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
    }
    test(size, map.iter().map(|(&k, &v)| (k, v)));
    test(size, map.iter_mut().map(|(&k, &mut v)| (k, v)));
    test(size, map.into_iter());
}

#[test]
fn test_iter_min_max() {
    let mut a = BTreeMap::new();
    assert_eq!(a.iter().min(), None);
    assert_eq!(a.iter().max(), None);
    assert_eq!(a.iter_mut().min(), None);
    assert_eq!(a.iter_mut().max(), None);
    assert_eq!(a.range(..).min(), None);
    assert_eq!(a.range(..).max(), None);
    assert_eq!(a.range_mut(..).min(), None);
    assert_eq!(a.range_mut(..).max(), None);
    assert_eq!(a.keys().min(), None);
    assert_eq!(a.keys().max(), None);
    assert_eq!(a.values().min(), None);
    assert_eq!(a.values().max(), None);
    assert_eq!(a.values_mut().min(), None);
    assert_eq!(a.values_mut().max(), None);
    a.insert(1, 42);
    a.insert(2, 24);
    assert_eq!(a.iter().min(), Some((&1, &42)));
    assert_eq!(a.iter().max(), Some((&2, &24)));
    assert_eq!(a.iter_mut().min(), Some((&1, &mut 42)));
    assert_eq!(a.iter_mut().max(), Some((&2, &mut 24)));
    assert_eq!(a.range(..).min(), Some((&1, &42)));
    assert_eq!(a.range(..).max(), Some((&2, &24)));
    assert_eq!(a.range_mut(..).min(), Some((&1, &mut 42)));
    assert_eq!(a.range_mut(..).max(), Some((&2, &mut 24)));
    assert_eq!(a.keys().min(), Some(&1));
    assert_eq!(a.keys().max(), Some(&2));
    assert_eq!(a.values().min(), Some(&24));
    assert_eq!(a.values().max(), Some(&42));
    assert_eq!(a.values_mut().min(), Some(&mut 24));
    assert_eq!(a.values_mut().max(), Some(&mut 42));
    a.check();
}

fn range_keys(map: &BTreeMap<i32, i32>, range: impl RangeBounds<i32>) -> Vec<i32> {
    Vec::from_iter(map.range(range).map(|(&k, &v)| {
        assert_eq!(k, v);
        k
    }))
}

#[test]
fn test_range_small() {
    let size = 4;

    let all = Vec::from_iter(1..=size);
    let (first, last) = (vec![all[0]], vec![all[size as usize - 1]]);
    let map = BTreeMap::from_iter(all.iter().copied().map(|i| (i, i)));

    assert_eq!(range_keys(&map, (Excluded(0), Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Excluded(0), Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Excluded(0), Included(size))), all);
    assert_eq!(range_keys(&map, (Excluded(0), Unbounded)), all);
    assert_eq!(range_keys(&map, (Included(0), Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(0), Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(0), Included(size))), all);
    assert_eq!(range_keys(&map, (Included(0), Unbounded)), all);
    assert_eq!(range_keys(&map, (Included(1), Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(1), Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(1), Included(size))), all);
    assert_eq!(range_keys(&map, (Included(1), Unbounded)), all);
    assert_eq!(range_keys(&map, (Unbounded, Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Unbounded, Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Unbounded, Included(size))), all);
    assert_eq!(range_keys(&map, ..), all);

    assert_eq!(range_keys(&map, (Excluded(0), Excluded(1))), vec![]);
    assert_eq!(range_keys(&map, (Excluded(0), Included(0))), vec![]);
    assert_eq!(range_keys(&map, (Included(0), Included(0))), vec![]);
    assert_eq!(range_keys(&map, (Included(0), Excluded(1))), vec![]);
    assert_eq!(range_keys(&map, (Unbounded, Excluded(1))), vec![]);
    assert_eq!(range_keys(&map, (Unbounded, Included(0))), vec![]);
    assert_eq!(range_keys(&map, (Excluded(0), Excluded(2))), first);
    assert_eq!(range_keys(&map, (Excluded(0), Included(1))), first);
    assert_eq!(range_keys(&map, (Included(0), Excluded(2))), first);
    assert_eq!(range_keys(&map, (Included(0), Included(1))), first);
    assert_eq!(range_keys(&map, (Included(1), Excluded(2))), first);
    assert_eq!(range_keys(&map, (Included(1), Included(1))), first);
    assert_eq!(range_keys(&map, (Unbounded, Excluded(2))), first);
    assert_eq!(range_keys(&map, (Unbounded, Included(1))), first);
    assert_eq!(
        range_keys(&map, (Excluded(size - 1), Excluded(size + 1))),
        last
    );
    assert_eq!(
        range_keys(&map, (Excluded(size - 1), Included(size + 1))),
        last
    );
    assert_eq!(range_keys(&map, (Excluded(size - 1), Included(size))), last);
    assert_eq!(range_keys(&map, (Excluded(size - 1), Unbounded)), last);
    assert_eq!(range_keys(&map, (Included(size), Excluded(size + 1))), last);
    assert_eq!(range_keys(&map, (Included(size), Included(size + 1))), last);
    assert_eq!(range_keys(&map, (Included(size), Included(size))), last);
    assert_eq!(range_keys(&map, (Included(size), Unbounded)), last);
    assert_eq!(
        range_keys(&map, (Excluded(size), Excluded(size + 1))),
        vec![]
    );
    assert_eq!(range_keys(&map, (Excluded(size), Included(size))), vec![]);
    assert_eq!(range_keys(&map, (Excluded(size), Unbounded)), vec![]);
    assert_eq!(
        range_keys(&map, (Included(size + 1), Excluded(size + 1))),
        vec![]
    );
    assert_eq!(
        range_keys(&map, (Included(size + 1), Included(size + 1))),
        vec![]
    );
    assert_eq!(range_keys(&map, (Included(size + 1), Unbounded)), vec![]);

    assert_eq!(range_keys(&map, ..3), vec![1, 2]);
    assert_eq!(range_keys(&map, 3..), vec![3, 4]);
    assert_eq!(range_keys(&map, 2..=3), vec![2, 3]);
}

#[test]
fn test_range_large() {
    let size = 200;

    let all = Vec::from_iter(1..=size);
    let (first, last) = (vec![all[0]], vec![all[size as usize - 1]]);
    let map = BTreeMap::from_iter(all.iter().copied().map(|i| (i, i)));

    assert_eq!(range_keys(&map, (Excluded(0), Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Excluded(0), Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Excluded(0), Included(size))), all);
    assert_eq!(range_keys(&map, (Excluded(0), Unbounded)), all);
    assert_eq!(range_keys(&map, (Included(0), Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(0), Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(0), Included(size))), all);
    assert_eq!(range_keys(&map, (Included(0), Unbounded)), all);
    assert_eq!(range_keys(&map, (Included(1), Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(1), Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Included(1), Included(size))), all);
    assert_eq!(range_keys(&map, (Included(1), Unbounded)), all);
    assert_eq!(range_keys(&map, (Unbounded, Excluded(size + 1))), all);
    assert_eq!(range_keys(&map, (Unbounded, Included(size + 1))), all);
    assert_eq!(range_keys(&map, (Unbounded, Included(size))), all);
    assert_eq!(range_keys(&map, ..), all);

    assert_eq!(range_keys(&map, (Excluded(0), Excluded(1))), vec![]);
    assert_eq!(range_keys(&map, (Excluded(0), Included(0))), vec![]);
    assert_eq!(range_keys(&map, (Included(0), Included(0))), vec![]);
    assert_eq!(range_keys(&map, (Included(0), Excluded(1))), vec![]);
    assert_eq!(range_keys(&map, (Unbounded, Excluded(1))), vec![]);
    assert_eq!(range_keys(&map, (Unbounded, Included(0))), vec![]);
    assert_eq!(range_keys(&map, (Excluded(0), Excluded(2))), first);
    assert_eq!(range_keys(&map, (Excluded(0), Included(1))), first);
    assert_eq!(range_keys(&map, (Included(0), Excluded(2))), first);
    assert_eq!(range_keys(&map, (Included(0), Included(1))), first);
    assert_eq!(range_keys(&map, (Included(1), Excluded(2))), first);
    assert_eq!(range_keys(&map, (Included(1), Included(1))), first);
    assert_eq!(range_keys(&map, (Unbounded, Excluded(2))), first);
    assert_eq!(range_keys(&map, (Unbounded, Included(1))), first);
    assert_eq!(
        range_keys(&map, (Excluded(size - 1), Excluded(size + 1))),
        last
    );
    assert_eq!(
        range_keys(&map, (Excluded(size - 1), Included(size + 1))),
        last
    );
    assert_eq!(range_keys(&map, (Excluded(size - 1), Included(size))), last);
    assert_eq!(range_keys(&map, (Excluded(size - 1), Unbounded)), last);
    assert_eq!(range_keys(&map, (Included(size), Excluded(size + 1))), last);
    assert_eq!(range_keys(&map, (Included(size), Included(size + 1))), last);
    assert_eq!(range_keys(&map, (Included(size), Included(size))), last);
    assert_eq!(range_keys(&map, (Included(size), Unbounded)), last);
    assert_eq!(
        range_keys(&map, (Excluded(size), Excluded(size + 1))),
        vec![]
    );
    assert_eq!(range_keys(&map, (Excluded(size), Included(size))), vec![]);
    assert_eq!(range_keys(&map, (Excluded(size), Unbounded)), vec![]);
    assert_eq!(
        range_keys(&map, (Included(size + 1), Excluded(size + 1))),
        vec![]
    );
    assert_eq!(
        range_keys(&map, (Included(size + 1), Included(size + 1))),
        vec![]
    );
    assert_eq!(range_keys(&map, (Included(size + 1), Unbounded)), vec![]);

    fn check<'a, L, R>(lhs: L, rhs: R)
    where
        L: IntoIterator<Item = (&'a i32, &'a i32)>,
        R: IntoIterator<Item = (&'a i32, &'a i32)>,
    {
        assert_eq!(Vec::from_iter(lhs), Vec::from_iter(rhs));
    }

    check(map.range(..=100), map.range(..101));
    check(
        map.range(5..=8),
        vec![(&5, &5), (&6, &6), (&7, &7), (&8, &8)],
    );
    check(map.range(-1..=2), vec![(&1, &1), (&2, &2)]);
}

#[test]
fn test_range_inclusive_max_value() {
    let max = usize::MAX;
    let map = BTreeMap::from([(max, 0)]);
    assert_eq!(Vec::from_iter(map.range(max..=max)), &[(&max, &0)]);
}

#[test]
fn test_range_equal_empty_cases() {
    let map = BTreeMap::from_iter((0..5).map(|i| (i, i)));
    assert_eq!(map.range((Included(2), Excluded(2))).next(), None);
    assert_eq!(map.range((Excluded(2), Included(2))).next(), None);
}

#[test]
#[should_panic]
fn test_range_equal_excluded() {
    let map = BTreeMap::from_iter((0..5).map(|i| (i, i)));
    let _ = map.range((Excluded(2), Excluded(2)));
}

#[test]
#[should_panic]
fn test_range_backwards_1() {
    let map = BTreeMap::from_iter((0..5).map(|i| (i, i)));
    let _ = map.range((Included(3), Included(2)));
}

#[test]
#[should_panic]
fn test_range_backwards_2() {
    let map = BTreeMap::from_iter((0..5).map(|i| (i, i)));
    let _ = map.range((Included(3), Excluded(2)));
}

#[test]
#[should_panic]
fn test_range_backwards_3() {
    let map = BTreeMap::from_iter((0..5).map(|i| (i, i)));
    let _ = map.range((Excluded(3), Included(2)));
}

#[test]
#[should_panic]
fn test_range_backwards_4() {
    let map = BTreeMap::from_iter((0..5).map(|i| (i, i)));
    let _ = map.range((Excluded(3), Excluded(2)));
}

#[test]
fn test_range_finding_ill_order_in_range_ord() {
    // Has proper order the first time asked, then flips around.
    struct EvilTwin(i32);

    impl PartialOrd for EvilTwin {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    static COMPARES: AtomicUsize = AtomicUsize::new(0);
    impl Ord for EvilTwin {
        fn cmp(&self, other: &Self) -> Ordering {
            let ord = self.0.cmp(&other.0);
            if COMPARES.fetch_add(1, SeqCst) > 0 {
                ord.reverse()
            } else {
                ord
            }
        }
    }

    impl PartialEq for EvilTwin {
        fn eq(&self, other: &Self) -> bool {
            self.0.eq(&other.0)
        }
    }

    impl Eq for EvilTwin {}

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    struct CompositeKey(i32, EvilTwin);

    impl Borrow<EvilTwin> for CompositeKey {
        fn borrow(&self) -> &EvilTwin {
            &self.1
        }
    }

    let map = BTreeMap::from_iter((0..12).map(|i| (CompositeKey(i, EvilTwin(i)), ())));
    let _ = map.range(EvilTwin(5)..=EvilTwin(7));
}

#[test]
fn test_range_borrowed_key() {
    let mut map = BTreeMap::new();
    map.insert("aardvark".to_string(), 1);
    map.insert("baboon".to_string(), 2);
    map.insert("coyote".to_string(), 3);
    map.insert("dingo".to_string(), 4);
    // NOTE: would like to use simply "b".."d" here...
    let mut iter = map.range::<str, _>((Included("b"), Excluded("d")));
    assert_eq!(iter.next(), Some((&"baboon".to_string(), &2)));
    assert_eq!(iter.next(), Some((&"coyote".to_string(), &3)));
    assert_eq!(iter.next(), None);
}

#[test]
fn test_range() {
    let size = 200;
    // Miri is too slow
    let step = if cfg!(miri) { 66 } else { 1 };
    let map = BTreeMap::from_iter((0..size).map(|i| (i, i)));

    for i in (0..size).step_by(step) {
        for j in (i..size).step_by(step) {
            let mut kvs = map
                .range((Included(&i), Included(&j)))
                .map(|(&k, &v)| (k, v));
            let mut pairs = (i..=j).map(|i| (i, i));

            for (kv, pair) in kvs.by_ref().zip(pairs.by_ref()) {
                assert_eq!(kv, pair);
            }
            assert_eq!(kvs.next(), None);
            assert_eq!(pairs.next(), None);
        }
    }
}

#[test]
fn test_range_mut() {
    let size = 200;
    // Miri is too slow
    let step = if cfg!(miri) { 66 } else { 1 };
    let mut map = BTreeMap::from_iter((0..size).map(|i| (i, i)));

    for i in (0..size).step_by(step) {
        for j in (i..size).step_by(step) {
            let mut kvs = map
                .range_mut((Included(&i), Included(&j)))
                .map(|(&k, &mut v)| (k, v));
            let mut pairs = (i..=j).map(|i| (i, i));

            for (kv, pair) in kvs.by_ref().zip(pairs.by_ref()) {
                assert_eq!(kv, pair);
            }
            assert_eq!(kvs.next(), None);
            assert_eq!(pairs.next(), None);
        }
    }
    map.check();
}

#[should_panic(expected = "range start is greater than range end in BTreeMap")]
#[test]
fn test_range_panic_1() {
    let mut map = BTreeMap::new();
    map.insert(3, "a");
    map.insert(5, "b");
    map.insert(8, "c");

    let _invalid_range = map.range((Included(&8), Included(&3)));
}

#[should_panic(expected = "range start and end are equal and excluded in BTreeMap")]
#[test]
fn test_range_panic_2() {
    let mut map = BTreeMap::new();
    map.insert(3, "a");
    map.insert(5, "b");
    map.insert(8, "c");

    let _invalid_range = map.range((Excluded(&5), Excluded(&5)));
}

#[should_panic(expected = "range start and end are equal and excluded in BTreeMap")]
#[test]
fn test_range_panic_3() {
    let mut map: BTreeMap<i32, ()> = BTreeMap::new();
    map.insert(3, ());
    map.insert(5, ());
    map.insert(8, ());

    let _invalid_range = map.range((Excluded(&5), Excluded(&5)));
}

#[test]
fn test_retain() {
    let mut map = BTreeMap::from_iter((0..100).map(|x| (x, x * 10)));

    map.retain(|&k, _| k % 2 == 0);
    assert_eq!(map.len(), 50);
    assert_eq!(map[&2], 20);
    assert_eq!(map[&4], 40);
    assert_eq!(map[&6], 60);
}

#[test]
fn test_borrow() {
    // make sure these compile -- using the Borrow trait
    {
        let mut map = BTreeMap::new();
        map.insert("0".to_string(), 1);
        assert_eq!(map["0"], 1);
    }

    {
        let mut map = BTreeMap::new();
        map.insert(Box::new(0), 1);
        assert_eq!(map[&0], 1);
    }

    {
        let mut map = BTreeMap::new();
        map.insert(Box::new([0, 1]) as Box<[i32]>, 1);
        assert_eq!(map[&[0, 1][..]], 1);
    }

    {
        let mut map = BTreeMap::new();
        map.insert(Rc::new(0), 1);
        assert_eq!(map[&0], 1);
    }

    #[allow(dead_code)]
    fn get<T: Ord>(v: &BTreeMap<Box<T>, ()>, t: &T) {
        let _ = v.get(t);
    }

    #[allow(dead_code)]
    fn get_mut<T: Ord>(v: &mut BTreeMap<Box<T>, ()>, t: &T) {
        let _ = v.get_mut(t);
    }

    #[allow(dead_code)]
    fn get_key_value<T: Ord>(v: &BTreeMap<Box<T>, ()>, t: &T) {
        let _ = v.get_key_value(t);
    }

    #[allow(dead_code)]
    fn contains_key<T: Ord>(v: &BTreeMap<Box<T>, ()>, t: &T) {
        let _ = v.contains_key(t);
    }

    #[allow(dead_code)]
    fn range<T: Ord>(v: &BTreeMap<Box<T>, ()>, t: T) {
        let _ = v.range(t..);
    }

    #[allow(dead_code)]
    fn range_mut<T: Ord>(v: &mut BTreeMap<Box<T>, ()>, t: T) {
        let _ = v.range_mut(t..);
    }

    #[allow(dead_code)]
    fn remove<T: Ord>(v: &mut BTreeMap<Box<T>, ()>, t: &T) {
        v.remove(t);
    }

    #[allow(dead_code)]
    fn remove_entry<T: Ord>(v: &mut BTreeMap<Box<T>, ()>, t: &T) {
        v.remove_entry(t);
    }

    #[allow(dead_code)]
    fn split_off<T: Ord>(v: &mut BTreeMap<Box<T>, ()>, t: &T) {
        v.split_off(t);
    }
}

#[test]
fn test_entry() {
    let xs = [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)];

    let mut map = BTreeMap::from(xs);

    // Existing key (insert)
    match map.entry(1) {
        Vacant(_) => unreachable!(),
        Occupied(mut view) => {
            assert_eq!(view.get(), &10);
            assert_eq!(view.insert(100), 10);
        }
    }
    assert_eq!(map.get(&1).unwrap(), &100);
    assert_eq!(map.len(), 6);

    // Existing key (update)
    match map.entry(2) {
        Vacant(_) => unreachable!(),
        Occupied(mut view) => {
            let v = view.get_mut();
            *v *= 10;
        }
    }
    assert_eq!(map.get(&2).unwrap(), &200);
    assert_eq!(map.len(), 6);
    map.check();

    // Existing key (take)
    match map.entry(3) {
        Vacant(_) => unreachable!(),
        Occupied(view) => {
            assert_eq!(view.remove(), 30);
        }
    }
    assert_eq!(map.get(&3), None);
    assert_eq!(map.len(), 5);
    map.check();

    // Inexistent key (insert)
    match map.entry(10) {
        Occupied(_) => unreachable!(),
        Vacant(view) => {
            assert_eq!(*view.insert(1000), 1000);
        }
    }
    assert_eq!(map.get(&10).unwrap(), &1000);
    assert_eq!(map.len(), 6);
    map.check();
}

#[test]
fn test_extend_ref() {
    let mut a = BTreeMap::new();
    a.insert(1, "one");
    let mut b = BTreeMap::new();
    b.insert(2, "two");
    b.insert(3, "three");

    a.extend(&b);

    assert_eq!(a.len(), 3);
    assert_eq!(a[&1], "one");
    assert_eq!(a[&2], "two");
    assert_eq!(a[&3], "three");
    a.check();
}

#[test]
fn test_zst() {
    let mut m = BTreeMap::new();
    assert_eq!(m.len(), 0);

    assert_eq!(m.insert((), ()), None);
    assert_eq!(m.len(), 1);

    assert_eq!(m.insert((), ()), Some(()));
    assert_eq!(m.len(), 1);
    assert_eq!(m.iter().count(), 1);

    m.clear();
    assert_eq!(m.len(), 0);

    for _ in 0..100 {
        m.insert((), ());
    }

    assert_eq!(m.len(), 1);
    assert_eq!(m.iter().count(), 1);
    m.check();
}

// This test's only purpose is to ensure that zero-sized keys with nonsensical orderings
// do not cause segfaults when used with zero-sized values. All other map behavior is
// undefined.
#[test]
fn test_bad_zst() {
    #[derive(Clone, Copy, Debug)]
    struct Bad;

    impl PartialEq for Bad {
        fn eq(&self, _: &Self) -> bool {
            false
        }
    }

    impl Eq for Bad {}

    impl PartialOrd for Bad {
        fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
            Some(Ordering::Less)
        }
    }

    impl Ord for Bad {
        fn cmp(&self, _: &Self) -> Ordering {
            Ordering::Less
        }
    }

    let mut m = BTreeMap::new();

    for _ in 0..100 {
        m.insert(Bad, Bad);
    }
    m.check();
}

#[allow(dead_code)]
fn assert_covariance() {
    fn map_key<'new>(v: BTreeMap<&'static str, ()>) -> BTreeMap<&'new str, ()> {
        v
    }
    fn map_val<'new>(v: BTreeMap<(), &'static str>) -> BTreeMap<(), &'new str> {
        v
    }

    fn into_iter_key<'new>(v: IntoIter<&'static str, ()>) -> IntoIter<&'new str, ()> {
        v
    }
    fn into_iter_val<'new>(v: IntoIter<(), &'static str>) -> IntoIter<(), &'new str> {
        v
    }

    fn into_keys_key<'new>(v: IntoKeys<&'static str, ()>) -> IntoKeys<&'new str, ()> {
        v
    }
    fn into_keys_val<'new>(v: IntoKeys<(), &'static str>) -> IntoKeys<(), &'new str> {
        v
    }

    fn into_values_key<'new>(v: IntoValues<&'static str, ()>) -> IntoValues<&'new str, ()> {
        v
    }
    fn into_values_val<'new>(v: IntoValues<(), &'static str>) -> IntoValues<(), &'new str> {
        v
    }
}

#[allow(dead_code)]
fn assert_send() {
    fn map<T: Send>(v: BTreeMap<T, T>) -> impl Send {
        v
    }

    fn into_iter<T: Send>(v: BTreeMap<T, T>) -> impl Send {
        v.into_iter()
    }

    fn into_keys<T: Send + Ord>(v: BTreeMap<T, T>) -> impl Send {
        v.into_keys()
    }

    fn into_values<T: Send + Ord>(v: BTreeMap<T, T>) -> impl Send {
        v.into_values()
    }

    fn iter<T: Send + Sync>(v: &BTreeMap<T, T>) -> impl Send + '_ {
        v.iter()
    }

    fn iter_mut<T: Send>(v: &mut BTreeMap<T, T>) -> impl Send + '_ {
        v.iter_mut()
    }

    fn keys<T: Send + Sync>(v: &mut BTreeMap<T, T>) -> impl Send + '_ {
        v.keys()
    }

    fn values<T: Send + Sync>(v: &BTreeMap<T, T>) -> impl Send + '_ {
        v.values()
    }

    fn values_mut<T: Send>(v: &mut BTreeMap<T, T>) -> impl Send + '_ {
        v.values_mut()
    }

    fn entry<T: Send + Ord + Default>(v: &mut BTreeMap<T, T>) -> impl Send + '_ {
        v.entry(Default::default())
    }

    fn occupied_entry<T: Send + Ord + Default>(v: &mut BTreeMap<T, T>) -> impl Send + '_ {
        match v.entry(Default::default()) {
            Occupied(entry) => entry,
            _ => unreachable!(),
        }
    }

    fn vacant_entry<T: Send + Ord + Default>(v: &mut BTreeMap<T, T>) -> impl Send + '_ {
        match v.entry(Default::default()) {
            Vacant(entry) => entry,
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_occupied_entry_key() {
    let mut a = BTreeMap::new();
    let key = "hello there";
    let value = "value goes here";

    a.insert(key, value);
    assert_eq!(a.len(), 1);
    assert_eq!(a[key], value);

    match a.entry(key) {
        Vacant(_) => panic!(),
        Occupied(e) => assert_eq!(key, *e.key()),
    }
    assert_eq!(a.len(), 1);
    assert_eq!(a[key], value);
    a.check();
}

#[test]
fn test_vacant_entry_key() {
    let mut a = BTreeMap::new();
    let key = "hello there";
    let value = "value goes here";

    match a.entry(key) {
        Occupied(_) => unreachable!(),
        Vacant(e) => {
            assert_eq!(key, *e.key());
            e.insert(value);
        }
    }
    assert_eq!(a.len(), 1);
    assert_eq!(a[key], value);
    a.check();
}

#[test]
fn test_vacant_entry_no_insert() {
    let mut a = BTreeMap::<&str, ()>::new();
    let key = "hello there";

    match a.entry(key) {
        Occupied(_) => unreachable!(),
        Vacant(e) => assert_eq!(key, *e.key()),
    }

    a.check();

    // Allocated but still empty
    a.insert(key, ());
    a.remove(&key);
    assert!(a.is_empty());
    match a.entry(key) {
        Occupied(_) => unreachable!(),
        Vacant(e) => assert_eq!(key, *e.key()),
    }
    assert!(a.is_empty());
    a.check();
}

#[test]
fn test_first_last_entry() {
    let mut a = BTreeMap::new();
    assert!(a.first_entry().is_none());
    assert!(a.last_entry().is_none());
    a.insert(1, 42);
    assert_eq!(a.first_entry().unwrap().key(), &1);
    assert_eq!(a.last_entry().unwrap().key(), &1);
    a.insert(2, 24);
    assert_eq!(a.first_entry().unwrap().key(), &1);
    assert_eq!(a.last_entry().unwrap().key(), &2);
    a.insert(0, 6);
    assert_eq!(a.first_entry().unwrap().key(), &0);
    assert_eq!(a.last_entry().unwrap().key(), &2);
    let (k1, v1) = a.first_entry().unwrap().remove_entry();
    assert_eq!(k1, 0);
    assert_eq!(v1, 6);
    let (k2, v2) = a.last_entry().unwrap().remove_entry();
    assert_eq!(k2, 2);
    assert_eq!(v2, 24);
    assert_eq!(a.first_entry().unwrap().key(), &1);
    assert_eq!(a.last_entry().unwrap().key(), &1);
    a.check();
}

#[test]
fn test_pop_first_last() {
    let mut map = BTreeMap::new();
    assert_eq!(map.pop_first(), None);
    assert_eq!(map.pop_last(), None);

    map.insert(1, 10);
    map.insert(2, 20);
    map.insert(3, 30);
    map.insert(4, 40);

    assert_eq!(map.len(), 4);

    let (key, val) = map.pop_first().unwrap();
    assert_eq!(key, 1);
    assert_eq!(val, 10);
    assert_eq!(map.len(), 3);

    let (key, val) = map.pop_first().unwrap();
    assert_eq!(key, 2);
    assert_eq!(val, 20);
    assert_eq!(map.len(), 2);
    let (key, val) = map.pop_last().unwrap();
    assert_eq!(key, 4);
    assert_eq!(val, 40);
    assert_eq!(map.len(), 1);

    map.insert(5, 50);
    map.insert(6, 60);
    assert_eq!(map.len(), 3);

    let (key, val) = map.pop_first().unwrap();
    assert_eq!(key, 3);
    assert_eq!(val, 30);
    assert_eq!(map.len(), 2);

    let (key, val) = map.pop_last().unwrap();
    assert_eq!(key, 6);
    assert_eq!(val, 60);
    assert_eq!(map.len(), 1);

    let (key, val) = map.pop_last().unwrap();
    assert_eq!(key, 5);
    assert_eq!(val, 50);
    assert_eq!(map.len(), 0);

    assert_eq!(map.pop_first(), None);
    assert_eq!(map.pop_last(), None);

    map.insert(7, 70);
    map.insert(8, 80);

    let (key, val) = map.pop_last().unwrap();
    assert_eq!(key, 8);
    assert_eq!(val, 80);
    assert_eq!(map.len(), 1);

    let (key, val) = map.pop_last().unwrap();
    assert_eq!(key, 7);
    assert_eq!(val, 70);
    assert_eq!(map.len(), 0);

    assert_eq!(map.pop_first(), None);
    assert_eq!(map.pop_last(), None);
}

#[test]
fn test_get_key_value() {
    let mut map = BTreeMap::new();

    assert!(map.is_empty());
    assert_eq!(map.get_key_value(&1), None);
    assert_eq!(map.get_key_value(&2), None);

    map.insert(1, 10);
    map.insert(2, 20);
    map.insert(3, 30);

    assert_eq!(map.len(), 3);
    assert_eq!(map.get_key_value(&1), Some((&1, &10)));
    assert_eq!(map.get_key_value(&3), Some((&3, &30)));
    assert_eq!(map.get_key_value(&4), None);

    map.remove(&3);

    assert_eq!(map.len(), 2);
    assert_eq!(map.get_key_value(&3), None);
    assert_eq!(map.get_key_value(&2), Some((&2, &20)));
}

#[test]
fn test_into_keys() {
    let map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let keys = Vec::from_iter(map.into_keys());

    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&1));
    assert!(keys.contains(&2));
    assert!(keys.contains(&3));
}

#[test]
fn test_into_values() {
    let map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let values = Vec::from_iter(map.into_values());

    assert_eq!(values.len(), 3);
    assert!(values.contains(&'a'));
    assert!(values.contains(&'b'));
    assert!(values.contains(&'c'));
}

#[test]
fn from_array() {
    let map = BTreeMap::from([(1, 2), (3, 4)]);
    let unordered_duplicates = BTreeMap::from([(3, 4), (1, 2), (1, 2)]);
    assert_eq!(map, unordered_duplicates);
}

/* ToDo : re-enable these tests once cursor is fully implemented.
#[test]
fn test_cursor() {
    let map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);

    let mut cur = map.lower_bound(Bound::Unbounded);
    assert_eq!(cur.peek_next(), Some((&1, &'a')));
    assert_eq!(cur.peek_prev(), None);
    assert_eq!(cur.prev(), None);
    assert_eq!(cur.next(), Some((&1, &'a')));

    assert_eq!(cur.next(), Some((&2, &'b')));

    assert_eq!(cur.peek_next(), Some((&3, &'c')));
    assert_eq!(cur.prev(), Some((&2, &'b')));
    assert_eq!(cur.peek_prev(), Some((&1, &'a')));

    let mut cur = map.upper_bound(Bound::Excluded(&1));
    assert_eq!(cur.peek_prev(), None);
    assert_eq!(cur.next(), Some((&1, &'a')));
    assert_eq!(cur.prev(), Some((&1, &'a')));
}

#[test]
fn test_cursor_mut() {
    let mut map = BTreeMap::from([(1, 'a'), (3, 'c'), (5, 'e')]);
    let mut cur = map.lower_bound_mut(Bound::Excluded(&3));
    assert_eq!(cur.peek_next(), Some((&5, &mut 'e')));
    assert_eq!(cur.peek_prev(), Some((&3, &mut 'c')));

    cur.insert_before(4, 'd').unwrap();
    assert_eq!(cur.peek_next(), Some((&5, &mut 'e')));
    assert_eq!(cur.peek_prev(), Some((&4, &mut 'd')));

    assert_eq!(cur.next(), Some((&5, &mut 'e')));
    assert_eq!(cur.peek_next(), None);
    assert_eq!(cur.peek_prev(), Some((&5, &mut 'e')));
    cur.insert_before(6, 'f').unwrap();
    assert_eq!(cur.peek_next(), None);
    assert_eq!(cur.peek_prev(), Some((&6, &mut 'f')));
    assert_eq!(cur.remove_prev(), Some((6, 'f')));
    assert_eq!(cur.remove_prev(), Some((5, 'e')));
    assert_eq!(cur.remove_next(), None);
    assert_eq!(map, BTreeMap::from([(1, 'a'), (3, 'c'), (4, 'd')]));

    let mut cur = map.upper_bound_mut(Bound::Included(&5));
    assert_eq!(cur.peek_next(), None);
    assert_eq!(cur.prev(), Some((&4, &mut 'd')));
    assert_eq!(cur.peek_next(), Some((&4, &mut 'd')));
    assert_eq!(cur.peek_prev(), Some((&3, &mut 'c')));
    assert_eq!(cur.remove_next(), Some((4, 'd')));
    assert_eq!(map, BTreeMap::from([(1, 'a'), (3, 'c')]));
}

#[test]
fn test_cursor_mut_key() {
    let mut map = BTreeMap::from([(1, 'a'), (3, 'c'), (5, 'e')]);
    let mut cur = unsafe { map.lower_bound_mut(Bound::Excluded(&3)).with_mutable_key() };
    assert_eq!(cur.peek_next(), Some((&mut 5, &mut 'e')));
    assert_eq!(cur.peek_prev(), Some((&mut 3, &mut 'c')));

    cur.insert_before(4, 'd').unwrap();
    assert_eq!(cur.peek_next(), Some((&mut 5, &mut 'e')));
    assert_eq!(cur.peek_prev(), Some((&mut 4, &mut 'd')));

    assert_eq!(cur.next(), Some((&mut 5, &mut 'e')));
    assert_eq!(cur.peek_next(), None);
    assert_eq!(cur.peek_prev(), Some((&mut 5, &mut 'e')));
    cur.insert_before(6, 'f').unwrap();
    assert_eq!(cur.peek_next(), None);
    assert_eq!(cur.peek_prev(), Some((&mut 6, &mut 'f')));
    assert_eq!(cur.remove_prev(), Some((6, 'f')));
    assert_eq!(cur.remove_prev(), Some((5, 'e')));
    assert_eq!(cur.remove_next(), None);
    assert_eq!(map, BTreeMap::from([(1, 'a'), (3, 'c'), (4, 'd')]));

    let mut cur = unsafe { map.upper_bound_mut(Bound::Included(&5)).with_mutable_key() };
    assert_eq!(cur.peek_next(), None);
    assert_eq!(cur.prev(), Some((&mut 4, &mut 'd')));
    assert_eq!(cur.peek_next(), Some((&mut 4, &mut 'd')));
    assert_eq!(cur.peek_prev(), Some((&mut 3, &mut 'c')));
    assert_eq!(cur.remove_next(), Some((4, 'd')));
    assert_eq!(map, BTreeMap::from([(1, 'a'), (3, 'c')]));
}

#[test]
fn test_cursor_empty() {
    let mut map = BTreeMap::new();
    let mut cur = map.lower_bound_mut(Bound::Excluded(&3));
    assert_eq!(cur.peek_next(), None);
    assert_eq!(cur.peek_prev(), None);
    cur.insert_after(0, 0).unwrap();
    assert_eq!(cur.peek_next(), Some((&0, &mut 0)));
    assert_eq!(cur.peek_prev(), None);
    assert_eq!(map, BTreeMap::from([(0, 0)]));
}

#[test]
fn test_cursor_mut_insert_before_1() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_before(0, 'd').unwrap_err();
}

#[test]
fn test_cursor_mut_insert_before_2() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_before(1, 'd').unwrap_err();
}

#[test]
fn test_cursor_mut_insert_before_3() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_before(2, 'd').unwrap_err();
}

#[test]
fn test_cursor_mut_insert_before_4() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_before(3, 'd').unwrap_err();
}

#[test]
fn test_cursor_mut_insert_after_1() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_after(1, 'd').unwrap_err();
}

#[test]
fn test_cursor_mut_insert_after_2() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_after(2, 'd').unwrap_err();
}

#[test]
fn test_cursor_mut_insert_after_3() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_after(3, 'd').unwrap_err();
}

#[test]
fn test_cursor_mut_insert_after_4() {
    let mut map = BTreeMap::from([(1, 'a'), (2, 'b'), (3, 'c')]);
    let mut cur = map.upper_bound_mut(Bound::Included(&2));
    cur.insert_after(4, 'd').unwrap_err();
}

#[test]
fn cursor_peek_prev_agrees_with_cursor_mut() {
    let mut map = BTreeMap::from([(1, 1), (2, 2), (3, 3)]);

    let cursor = map.lower_bound(Bound::Excluded(&3));
    assert!(cursor.peek_next().is_none());

    let prev = cursor.peek_prev();
    assert_matches!(prev, Some((&3, _)));

    // Shadow names so the two parts of this test match.
    let mut cursor = map.lower_bound_mut(Bound::Excluded(&3));
    assert!(cursor.peek_next().is_none());

    let prev = cursor.peek_prev();
    assert_matches!(prev, Some((&3, _)));
}
*/
