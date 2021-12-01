use crate::*;
use std::cmp::min;
use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

pub struct DataSlice {
    off: usize,
    len: usize,
    data: Data,
}

/// AtomicFile makes sure that database updates are all-or-nothing.
/// Keeps a list of outstanding writes which have not yet been written to the underlying file.
pub struct AtomicFile {
    /// Map of existing outstanding writes. Note the key is the end of the write minus one.
    pub map: BTreeMap<u64, DataSlice>,
    pub file: Mutex<fs::File>,
}

const TRACE: bool = false;

impl Storage for AtomicFile {
    fn size(&self) -> u64 {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::End(0)).unwrap()
    }

    /// Read from file. Uses map data if available.
    fn read(&self, start: u64, data: &mut [u8]) {
        if TRACE {
            println!("Reading start={} len={}", start, data.len());
        }

        let mut done: usize = 0;
        let mut todo: usize = data.len();

        for (k, v) in self.map.range(start..) {
            let estart = *k - v.len as u64 + 1;
            if estart > start + done as u64 {
                let lim = (estart - (start + done as u64)) as usize;
                let amount = min(todo, lim) as usize;
                if TRACE {
                    println!(
                        "Read from underlying file at {} amount={}",
                        start + done as u64,
                        amount
                    )
                };
                done = done + amount;
                todo = todo - amount;
            }
            if estart > start + data.len() as u64 {
                break;
            } else {
                let skip = (start + done as u64 - estart) as usize;
                let amount = min(todo, v.len - skip);
                self.read0(start + done as u64, &mut data[done..done + amount]);
                if TRACE {
                    println!(
                        "Read from map start = {} amount={} skip={}",
                        start + done as u64,
                        amount,
                        skip
                    )
                };
                done = done + amount;
                todo = todo - amount;
            }
            if todo == 0 {
                break;
            }
        }
        if todo > 0 {
            self.read0(start + done as u64, &mut data[done..done + todo]);
            if TRACE {
                println!(
                    "Read from underlying file at {} amount={}",
                    start + done as u64,
                    todo
                )
            };
        }
    }

    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        if TRACE {
            println!("write_data start={} len={}", start, len);
        }

        // Existing writes which are overlap with new write need to be trimmed or removed.
        let mut remove = Vec::new();
        let mut add = Vec::new();
        let end = start + len as u64;

        for (k, v) in self.map.range_mut(start..) {
            let eend = *k + 1; // end of existing write.
            let estart = eend - v.len as u64; // start of existing write.

            // (a) New write ends before existing write.
            if end <= estart {
                if TRACE {
                    println!("Case (a) end={} estart={}", end, estart);
                }
                break;
            }
            // (b) New write starts after existing write. Should not happen due to range condition.
            else if start > eend {
                if TRACE {
                    println!("{} > {} so panic", end, eend);
                }
                panic!();
            }
            // (c) New write subsumes existing write entirely, remove existing write.
            else if start <= estart && end >= eend {
                remove.push(eend - 1);
                if TRACE {
                    println!("Case (c), removing {}", estart);
                }
            }
            // (d) New write starts before existing write, but doesn't subsume it. Trim existing write.
            else if start < estart {
                let trim = (end - estart) as usize;
                v.len = v.len - trim;
                v.off = v.off + trim;
                if TRACE {
                    println!("Case (d), estart={} v.len={}", estart, v.len);
                }
            }
            // (e) New write starts in middle of existing write, ends before end of existing write...
            // .. put start of existing write in add list, trim existing write.
            else if start > estart && end < eend {
                let remain = (start - estart) as usize;
                add.push((estart, v.data.clone(), v.off, remain));

                let trim = (end - estart) as usize;
                v.len = v.len - trim;
                v.off = v.off + trim;

                if TRACE {
                    println!(
                        "Case (e), estart={} remain={} v.len={}",
                        estart, remain, v.len
                    );
                }
            }
            // (f) New write starts in middle of existing write, ends after existing write...
            // ... put start of existing write in add list, remove existing write,
            else {
                let remain = (start - estart) as usize;
                add.push((estart, v.data.clone(), v.off, remain));

                remove.push(eend - 1);

                if TRACE {
                    println!("Case (f), estart={} remain={}", estart, remain);
                }
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

    fn write(&mut self, start: u64, data: &[u8]) {
        let len = data.len();
        let d = Arc::new(data.to_vec());
        self.write_data(start, d, 0, len);
    }

    fn commit(&mut self, size: u64) {
        self.flush();
        let f = self.file.lock().unwrap();
        f.set_len(size).unwrap();
    }
}

impl AtomicFile {
    pub fn new(filename: &str) -> Self {
        Self {
            map: BTreeMap::new(),
            file: Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(filename)
                    .unwrap(),
            ),
        }
    }

    pub fn read0(&self, off: u64, bytes: &mut [u8]) {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        let _x = f.read_exact(bytes);
    }
    pub fn write0(&self, off: u64, bytes: &[u8]) {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        let _x = f.write(bytes);
    }

    pub fn flush(&mut self) {
        // ToDo : first write the updates to a special file (for roll forward in case of an abort/crash).
        for (k, v) in &self.map {
            let start = k - v.len as u64 + 1;
            self.write0(start, &v.data[v.off..v.off + v.len]);
        }
        self.map.clear();
    }

    pub fn commit() {}
}

pub fn test() {
    // Write ranges 5..15 and 20..25 */
    let mut af = AtomicFile::new("test.atomfile");
    let d = b"0123456789";
    af.write(5, d);
    let d = b"ABCDE";
    af.write(20, d);

    // println!("Map of writes = {:?}", af.map);

    // Read range 10..40.
    let mut d = vec![0; 30];
    af.read(10, &mut d);

    // Write range 8..13
    let d = b"ABCDE";
    af.write(8, d);

    // Write range 18..23
    let d = b"FEGHI";
    af.write(18, d);

    // Write range 24..29
    let d = b"JKLMN";
    af.write(24, d);

    // Write range 8..13
    let d = b"ABCDE";
    af.write(8, d);

    // println!("Map of writes = {:?}", af.map);

    // Read range 10..40.
    let mut d = vec![0; 30];
    af.read(10, &mut d);
}

#[test]
fn run_atomic_file_test() {
    test();
}
