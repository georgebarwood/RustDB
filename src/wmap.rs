use crate::{BTreeMap, Data, Storage};
use std::cmp::min;

#[derive(Default)]
/// Slice of Data to be written to storage.
pub struct DataSlice {
    /// Slice data.
    pub data: Data,
    /// Start of slice.
    pub off: usize,
    /// Length of slice.
    pub len: usize,
}

impl DataSlice {
    /// Get reference to the whole slice.
    pub fn all(&self) -> &[u8] {
        &self.data[self.off..self.off + self.len]
    }
    /// Get reference to part of slice.
    pub fn part(&self, off: usize, len: usize) -> &[u8] {
        &self.data[self.off + off..self.off + off + len]
    }
    /// Trim specified amount from start of slice.
    pub fn trim(&mut self, trim: usize) {
        self.off += trim;
        self.len -= trim;
    }
    /// Take the data.
    pub fn take(&mut self) -> Data {
        std::mem::take(&mut self.data)
    }
}

#[derive(Default)]
/// Updateable storage based on some underlying storage.
pub struct WMap {
    /// Map of writes. Key is the end of the slice.
    map: BTreeMap<u64, DataSlice>,
}

impl WMap {
    /// Is the map empty?
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Number of key-value pairs in the map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Take the map and convert it to a Vec.
    pub fn to_vec(&mut self) -> Vec<(u64, DataSlice)> {
        let map = std::mem::take(&mut self.map);
        let mut result = Vec::with_capacity(map.len());
        for (end, v) in map {
            let start = end - v.len as u64;
            result.push((start, v));
        }
        result
    }

    /// Write the map into storage.
    pub fn to_storage(&self, stg: &mut dyn Storage) {
        for (end, v) in self.map.iter() {
            let start = end - v.len as u64;
            stg.write_data(start, v.data.clone(), v.off, v.len);
        }
    }

    #[cfg(not(feature = "btree_experiment"))]
    /// Write to storage, existing writes which overlap with new write need to be trimmed or removed.
    pub fn write(&mut self, start: u64, data: Data, off: usize, len: usize) {
        if len != 0 {
            let (mut insert, mut remove) = (Vec::new(), Vec::new());
            let end = start + len as u64;
            for (ee, v) in self.map.range_mut(start + 1..) {
                let ee = *ee;
                let es = ee - v.len as u64; // Existing write Start.
                if es >= end {
                    // Existing write starts after end of new write, nothing to do.
                    break;
                } else if start <= es {
                    if end < ee {
                        // New write starts before existing write, but doesn't subsume it. Trim existing write.
                        v.trim((end - es) as usize);
                        break;
                    }
                    // New write subsumes existing write entirely, remove existing write.
                    remove.push(ee);
                } else if end < ee {
                    // New write starts in middle of existing write, ends before end of existing write,
                    // put start of existing write in insert list, trim existing write.
                    insert.push((es, v.data.clone(), v.off, (start - es) as usize));
                    v.trim((end - es) as usize);
                    break;
                } else {
                    // New write starts in middle of existing write, ends after existing write,
                    // put start of existing write in insert list, remove existing write.
                    insert.push((es, v.take(), v.off, (start - es) as usize));
                    remove.push(ee);
                }
            }
            for end in remove {
                self.map.remove(&end);
            }
            for (start, data, off, len) in insert {
                self.map
                    .insert(start + len as u64, DataSlice { data, off, len });
            }
            self.map
                .insert(start + len as u64, DataSlice { data, off, len });
        }
    }

    #[cfg(feature = "btree_experiment")]
    /// Write to storage, existing writes which overlap with new write need to be trimmed or removed.
    pub fn write(&mut self, start: u64, data: Data, off: usize, len: usize) {
        if len != 0 {
            let (mut insert, mut remove) = (Vec::new(), Vec::new());
            let end = start + len as u64;
            let mut c = unsafe{ self.map.lower_bound_mut(std::ops::Bound::Included(&start)).with_mutable_key() };
            while let Some((eend, v)) = c.next()
            {
                    let ee = *eend;
                    let es = ee - v.len as u64; // Existing write Start.
                    if es >= end {
                        // Existing write starts after end of new write, nothing to do.
                        break;
                    } else if start <= es {
                        if end < ee {
                            // New write starts before existing write, but doesn't subsume it. Trim existing write.
                            v.trim((end - es) as usize);
                            break;
                        }
                        // New write subsumes existing write entirely, remove existing write.
                        remove.push(ee);
                    } else if end < ee {
                        // New write starts in middle of existing write, ends before end of existing write,
                        // put start of existing write in insert list, trim existing write.
                        insert.push((es, v.data.clone(), v.off, (start - es) as usize));
                        v.trim((end - es) as usize);
                        break;
                    } else {
                        // New write starts in middle of existing write, ends after existing write,
                        // Trim existing write ( modifies key, but this is ok as ordering is not affected ).
                        v.len = (start - es) as usize;
                        *eend = es + v.len as u64;
                    }
            }
            for end in remove {
                self.map.remove(&end);
            }
            for (start, data, off, len) in insert {
                self.map
                    .insert(start + len as u64, DataSlice { data, off, len });
            }
            self.map
                .insert(start + len as u64, DataSlice { data, off, len });
        }
    }

    #[cfg(not(feature = "btree_experiment"))]
    /// Read from storage, taking map of existing writes into account. Unwritten ranges are read from underlying storage.
    pub fn read(&self, start: u64, data: &mut [u8], u: &dyn Storage) {
        let len = data.len();
        if len != 0 {
            let mut done = 0;
            for (&end, v) in self.map.range(start + 1..) {
                let es = end - v.len as u64; // Existing write Start.
                let doff = start + done as u64;
                if es > doff {
                    // Read from underlying storage.
                    let a = min(len - done, (es - doff) as usize);
                    u.read(doff, &mut data[done..done + a]);
                    done += a;
                    if done == len {
                        return;
                    }
                }
                // Use existing write.
                let skip = (start + done as u64 - es) as usize;
                let a = min(len - done, v.len - skip);
                data[done..done + a].copy_from_slice(v.part(skip, a));
                done += a;
                if done == len {
                    return;
                }
            }
            u.read(start + done as u64, &mut data[done..]);
        }
    }

    #[cfg(feature = "btree_experiment")]
    /// Read from storage, taking map of existing writes into account. Unwritten ranges are read from underlying storage.
    pub fn read(&self, start: u64, data: &mut [u8], u: &dyn Storage) {
        let len = data.len();
        if len != 0 {
            let mut done = 0;
            for (end, v) in self.map.range(start..)
            {
                let end = *end;
                let es = end - v.len as u64; // Existing write Start.
                let doff = start + done as u64;
                if es > doff {
                    // Read from underlying storage.
                    let a = min(len - done, (es - doff) as usize);
                    u.read(doff, &mut data[done..done + a]);
                    done += a;
                    if done == len {
                        return;
                    }
                }
                // Use existing write.
                let skip = (start + done as u64 - es) as usize;
                let a = min(len - done, v.len - skip);
                data[done..done + a].copy_from_slice(v.part(skip, a));
                done += a;
                if done == len { return; }
            }
            if done < len {
                u.read(start + done as u64, &mut data[done..]);
            }
        }
    }
}
