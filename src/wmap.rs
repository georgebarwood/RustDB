use crate::{BTreeMap, Data, Storage};
use std::cmp::min;

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
    pub fn data(&self) -> &[u8] {
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
    /// Map of writes. Key is the storage offset of the last byte.
    pub map: BTreeMap<u64, DataSlice>,
}

impl WMap {
    /// Read from storage, taking map of exisiting writes into account. Unwritten ranges are read from underlying storage.
    pub fn read(&self, start: u64, data: &mut [u8], u: &dyn Storage) {
        let len = data.len();
        if len != 0 {
            let mut done = 0;
            for (&k, v) in self.map.range(start..) {
                let es = k + 1 - v.len as u64; // Existing write Start.
                let doff = start + done as u64;
                if es > doff {
                    // Read from underlying storage.
                    let a = min(len - done, (es - doff) as usize);
                    u.read(doff, &mut data[done..done + a]);
                    done += a;
                    if done == len {
                        break;
                    }
                }
                // Use existing write.
                let skip = (start + done as u64 - es) as usize;
                let a = min(len - done, v.len - skip);
                data[done..done + a].copy_from_slice(v.part(skip, a));
                done += a;
                if done == len {
                    break;
                }
            }
            if done < len {
                u.read(start + done as u64, &mut data[done..len]);
            }
        }
    }

    /// Write to storage, existing writes which overlap with new write need to be trimmed or removed.
    pub fn write(&mut self, start: u64, data: Data, off: usize, len: usize) {
        if len == 0 {
            return;
        }
        let (mut insert, mut remove) = (Vec::new(), Vec::new());
        let end = start + len as u64;

        for (&k, v) in self.map.range_mut(start..) {
            let ee = k + 1; // Existing write End.
            let es = ee - v.len as u64; // Existing write Start.

            // (a) Existing write starts after end of new write, nothing to do.
            if es >= end {
                break;
            }
            // (b) New write subsumes existing write entirely, remove existing write.
            else if start <= es && end >= ee {
                remove.push(ee - 1);
            }
            // (c) New write starts before existing write, but doesn't subsume it. Trim existing write.
            else if start <= es {
                v.trim((end - es) as usize);
                break;
            }
            // (d) New write starts in middle of existing write, ends before end of existing write,
            // put start of existing write in insert list, trim existing write.
            else if end < ee {
                insert.push((es, v.data.clone(), v.off, (start - es) as usize));
                v.trim((end - es) as usize);
                break;
            }
            // (e) New write starts in middle of existing write, ends after existing write,
            // put start of existing write in insert list, remove existing write.
            else {
                insert.push((es, v.take(), v.off, (start - es) as usize));
                remove.push(ee - 1);
            }
        }
        for k in remove {
            self.map.remove(&k);
        }
        for (start, data, off, len) in insert {
            self.map
                .insert(start + len as u64 - 1, DataSlice { data, off, len });
        }
        self.map
            .insert(start + len as u64 - 1, DataSlice { data, off, len });
    }
}
