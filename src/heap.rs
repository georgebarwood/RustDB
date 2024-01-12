use crate::Ordering;

/// Vector indexed by U.
pub struct VecU<T>(Vec<T>);

impl<T, U> std::ops::Index<U> for VecU<T>
where
    usize: TryFrom<U>,
{
    type Output = T;
    fn index(&self, x: U) -> &Self::Output {
        let x = usize::try_from(x).ok().unwrap();
        perf_assert!(x < self.0.len());
        &self.0[x]
    }
}

impl<T, U> std::ops::IndexMut<U> for VecU<T>
where
    usize: TryFrom<U>,
{
    fn index_mut(&mut self, x: U) -> &mut Self::Output {
        let x = usize::try_from(x).ok().unwrap();
        perf_assert!(x < self.0.len());
        &mut self.0[x]
    }
}

/// Heap Node.
pub struct HeapNode<K, T, U> {
    /// Index of node from heap position.
    pub x: U,
    /// Heap position of this node.
    pub pos: U,
    /// Node id.
    pub id: T,
    /// Node key.
    pub key: K,
}

/// Generic heap with keys that can be modified for tracking least used page.
pub struct GHeap<K, T, U> {
    /// Number of heap nodes, not including free nodes.
    pub n: U,
    /// 1 + Index of start of free list.
    pub free: U,
    /// Vector of heap nodes.
    pub v: VecU<HeapNode<K, T, U>>,
}

impl<K, T, U> Default for GHeap<K, T, U>
where
    U: From<u8>,
{
    fn default() -> Self {
        Self {
            n: 0.into(),
            free: 0.into(),
            v: VecU(Vec::default()),
        }
    }
}

impl<K, T, U> GHeap<K, T, U>
where
    K: Ord,
    T: Default,
    U: Copy
        + From<u8>
        + std::cmp::PartialOrd
        + std::ops::AddAssign
        + std::ops::Add<Output = U>
        + std::ops::Sub<Output = U>
        + std::ops::SubAssign
        + std::ops::Mul<Output = U>
        + std::ops::Div<Output = U>,
    usize: TryFrom<U>,
{
    /// Insert id into heap with specified key (usage). Result is index of heap node.
    pub fn insert(&mut self, id: T, key: K) -> U {
        let pos = self.n;
        if pos * 2.into() + 2.into() <= pos {
            panic!("GHeap overflow");
        }
        self.n += 1.into();
        let x = if self.free == 0.into() {
            let x = pos;
            self.v.0.push(HeapNode { x, pos, id, key });
            x
        } else {
            let x = self.free - 1.into();
            self.free = self.v[x].pos;
            self.v[pos].x = x;
            self.v[x].pos = pos;
            self.v[x].id = id;
            self.v[x].key = key;
            x
        };
        self.move_up(pos, x);
        x
    }

    /// Modify key of specified heap node.
    pub fn modify(&mut self, x: U, newkey: K) {
        assert!(usize::try_from(x).ok().unwrap() < self.v.0.len());
        let pos = self.v[x].pos;
        let cf = newkey.cmp(&self.v[x].key);
        self.v[x].key = newkey;

        match cf {
            Ordering::Greater => self.move_down(pos, x),
            Ordering::Less => self.move_up(pos, x),
            Ordering::Equal => (),
        }
    }

    /// Remove heap node with smallest key, returning the associated id.
    /// Note: index of heap node is no longer valid.
    pub fn pop(&mut self) -> T {
        let zero = 0.into();
        let one = 1.into();
        assert!(self.n > zero);
        self.n -= one;
        let xmin = self.v[zero].x; // Node with smallest key.
        let xlast = self.v[self.n].x; // Last node in heap.
        self.v[xlast].pos = zero; // Make last node first.
        self.v[zero].x = xlast;
        self.move_down(zero, xlast);

        // De-allocate popped node
        self.v[xmin].pos = self.free;
        self.free = xmin + one;

        std::mem::take(&mut self.v[xmin].id)
    }

    fn move_up(&mut self, mut c: U, cx: U) {
        while c > 0.into() {
            let p = (c - 1.into()) / 2.into();
            let px = self.v[p].x;
            if self.v[cx].key >= self.v[px].key {
                return;
            }
            // Swap parent(p) and child(c).
            self.v[p].x = cx;
            self.v[cx].pos = p;
            self.v[c].x = px;
            self.v[px].pos = c;
            c = p;
        }
    }

    fn move_down(&mut self, mut p: U, px: U) {
        loop {
            let mut c = p * 2.into() + 1.into();
            if c >= self.n {
                return;
            }
            let mut cx = self.v[c].x;
            let mut ck = &self.v[cx].key;
            let c2 = c + 1.into();
            if c2 < self.n {
                let cx2 = self.v[c2].x;
                let ck2 = &self.v[cx2].key;
                if ck2 < ck {
                    c = c2;
                    cx = cx2;
                    ck = ck2;
                }
            }
            if ck >= &self.v[px].key {
                return;
            }
            // Swap parent(p) and child(c).
            self.v[p].x = cx;
            self.v[cx].pos = p;
            self.v[c].x = px;
            self.v[px].pos = c;
            p = c;
        }
    }
}

#[test]
pub fn test() {
    let mut h = GHeap::<u64, u64, u32>::default();
    let _h5 = h.insert(5, 10);
    let _h8 = h.insert(8, 1);
    let _h13 = h.insert(13, 2);
    h.modify(_h8, 15);
    assert!(h.pop() == 13);
    let _h22 = h.insert(22, 9);
    assert!(h.pop() == 22);
    assert!(h.pop() == 5);
    assert!(h.pop() == 8);
}

#[test]
pub fn test2() {
    use rand::Rng;
    let mut rng = rand::thread_rng();

    let mut h = GHeap::<u64, u64, u32>::default();
    let mut pages = crate::HashMap::default();
    for _i in 0..1000000 {
        let pnum = rng.gen::<u64>() % 100;
        let usage = rng.gen::<u64>() % 100;
        let action = rng.gen::<u8>() % 3;
        if action == 0 {
            let x = h.insert(pnum, usage);
            pages.insert(pnum, x);
        } else if action == 1 {
            if let Some(x) = pages.get(&pnum) {
                h.modify(*x, usage);
            }
        } else if action == 2 && h.n > 0 {
            let pnum = h.pop();
            pages.remove(&pnum);
        }
    }
}
