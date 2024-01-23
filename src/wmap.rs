use crate::{BTreeMap, Data, Storage};
use std::cmp::min;

/// Slice of Data to be written to storage.
#[derive(Clone)]
pub struct DataSlice {
    ///
    pub off: usize,
    ///
    pub len: usize,
    ///
    pub data: Data,
}

/// Map of outstanding writes which have not yet been written to the underlying file.
pub struct WMap {
    ///
    pub map: BTreeMap<u64, DataSlice>,
}

impl Default for WMap {
    /// Construct a new WMap
    fn default() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
}

impl WMap {
    ///
    pub fn read(&self, start: u64, data: &mut [u8], u: &dyn Storage) {
        let mut todo: usize = data.len();
        if todo == 0 {
            return;
        }
        let mut done: usize = 0;

        for (&k, v) in self.map.range(start..) {
            let estart = k + 1 - v.len as u64;
            if estart > start + done as u64 {
                let lim = (estart - (start + done as u64)) as usize;
                let amount = min(todo, lim);
                u.read(start + done as u64, &mut data[done..done + amount]);
                done += amount;
                todo -= amount;
            }
            if estart > start + data.len() as u64 {
                break;
            } else {
                let skip = (start + done as u64 - estart) as usize;
                let amount = min(todo, v.len - skip);
                data[done..done + amount]
                    .copy_from_slice(&v.data[v.off + skip..v.off + skip + amount]);
                done += amount;
                todo -= amount;
            }
            if todo == 0 {
                break;
            }
        }
        if todo > 0 {
            u.read(start + done as u64, &mut data[done..done + todo]);
        }
    }

    ///
    pub fn write(&mut self, start: u64, data: Data, off: usize, len: usize) {
        if len == 0 {
            return;
        }

        // Existing writes which overlap with new write need to be trimmed or removed.
        let mut remove = Vec::new();
        let mut add = Vec::new();
        let end = start + len as u64;

        for (&k, v) in self.map.range_mut(start..) {
            let eend = k + 1; // end of existing write.
            let estart = eend - v.len as u64; // start of existing write.

            // (a) New write ends before existing write.
            if end <= estart {
                break;
            }
            // (b) New write subsumes existing write entirely, remove existing write.
            else if start <= estart && end >= eend {
                remove.push(eend - 1);
            }
            // (c) New write starts before existing write, but doesn't subsume it. Trim existing write.
            else if start <= estart {
                let trim = (end - estart) as usize;
                v.len -= trim;
                v.off += trim;
            }
            // (d) New write starts in middle of existing write, ends before end of existing write...
            // .. put start of existing write in add list, trim existing write.
            else if start > estart && end < eend {
                let remain = (start - estart) as usize;
                add.push((estart, v.data.clone(), v.off, remain));

                let trim = (end - estart) as usize;
                v.len -= trim;
                v.off += trim;
            }
            // (e) New write starts in middle of existing write, ends after existing write...
            // ... put start of existing write in add list, remove existing write,
            else {
                let remain = (start - estart) as usize;
                add.push((estart, v.data.clone(), v.off, remain));

                remove.push(eend - 1);
            }
        }
        for k in remove {
            self.map.remove(&k);
        }
        for (start, data, off, len) in add {
            self.map
                .insert(start + len as u64 - 1, DataSlice { data, off, len });
        }

        self.map
            .insert(start + len as u64 - 1, DataSlice { data, off, len });
    }
}
