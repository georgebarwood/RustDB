use crate::stg::Storage;

const BUF_SIZE: usize = 1024 * 1024;

/// Write Buffer.
pub struct WriteBuffer {
    ix: usize,
    pos: u64,
    ///
    pub stg: Box<dyn Storage>,
    buf: Vec<u8>,
    #[cfg(feature = "log")]
    log: Log,
}

#[cfg(feature = "log")]
struct Log {
    write: u64,
    flush: u64,
    total: u64,
    first_flush_time: std::time::Instant,
}

impl WriteBuffer {
    ///
    pub fn new(stg: Box<dyn Storage>) -> Self {
        Self {
            ix: 0,
            pos: u64::MAX,
            stg,
            buf: vec![0; BUF_SIZE],
            #[cfg(feature = "log")]
            log: Log {
                write: 0,
                flush: 0,
                total: 0,
                first_flush_time: std::time::Instant::now(),
            },
        }
    }

    ///
    pub fn write(&mut self, off: u64, data: &[u8]) {
        if self.pos + self.ix as u64 != off {
            self.flush(off);
        }
        let mut done: usize = 0;
        let mut todo: usize = data.len();
        #[cfg(feature = "log")]
        {
            self.log.write += 1;
            self.log.total += todo as u64;
        }
        while todo > 0 {
            let mut n: usize = BUF_SIZE - self.ix;
            if n == 0 {
                self.flush(off + done as u64);
                n = BUF_SIZE;
            }
            if n > todo {
                n = todo;
            }
            self.buf[self.ix..self.ix + n].copy_from_slice(&data[done..done + n]);
            todo -= n;
            done += n;
            self.ix += n;
        }
    }

    fn flush(&mut self, new_pos: u64) {
        if self.ix > 0 {
            self.stg.write(self.pos, &self.buf[0..self.ix]);
            #[cfg(feature = "log")]
            {
                if self.log.flush == 0 {
                    self.log.first_flush_time = std::time::Instant::now();
                }
                self.log.flush += 1;
            }
        }
        self.ix = 0;
        self.pos = new_pos;
    }

    ///
    pub fn commit(&mut self, size: u64) {
        self.flush(u64::MAX);
        self.stg.commit(size);
        #[cfg(feature = "log")]
        {
            if size > 0 {
                println!(
                    "WriteBuffer commit size={size} write={} flush={} total={} time(micros)={}",
                    self.log.write,
                    self.log.flush,
                    self.log.total,
                    self.log.first_flush_time.elapsed().as_micros()
                );
            }
            self.log.write = 0;
            self.log.flush = 0;
            self.log.total = 0;
        }
    }

    ///
    pub fn write_u64(&mut self, start: u64, value: u64) {
        self.write(start, &value.to_le_bytes());
    }
}

use crate::Mutex;

/// ReadBufStg buffers small (< 50 byte) reads to the underlying storage. Only supported functions are read and reset.
///
/// See implementation of AtomicFile for how this is used in conjunction with WMap.
pub struct ReadBufStg {
    stg: Box<dyn Storage>,
    inner: Mutex<ReadBuffer>,
}

impl ReadBufStg {
    ///
    pub fn new(stg: Box<dyn Storage>) -> Box<Self> {
        Box::new(Self {
            stg,
            inner: Mutex::new(ReadBuffer::new()),
        })
    }
}

impl Storage for ReadBufStg {
    fn size(&self) -> u64 {
        panic!()
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        if data.len() < 50 {
            self.inner.lock().unwrap().read(&*self.stg, start, data);
        } else {
            self.stg.read(start, data);
        }
    }

    fn write(&mut self, _start: u64, _data: &[u8]) {
        panic!();
    }

    fn commit(&mut self, _size: u64) {
        panic!();
    }

    fn reset(&mut self) {
        self.inner.lock().unwrap().reset();
    }
}

use crate::HashMap;
use std::cmp::min;

const BSIZE: usize = 256;

struct ReadBuffer {
    map: HashMap<u64, Box<[u8; BSIZE]>>,
    hits: u64,
    miss: u64,
}

impl Drop for ReadBuffer {
    fn drop(&mut self) {
        // println!("ReadBuffer drop hits={} misses={}", self.hits, self.miss);
    }
}

impl ReadBuffer {
    fn new() -> Self {
        Self {
            map: HashMap::default(),
            hits: 0,
            miss: 0,
        }
    }

    ///
    fn reset(&mut self) {
        self.map.clear();
    }

    fn read(&mut self, stg: &dyn Storage, off: u64, data: &mut [u8]) {
        let mut done = 0;
        while done < data.len() {
            let off = off + done as u64;
            let sector = off / BSIZE as u64;
            let disp = (off % BSIZE as u64) as usize;
            let amount = min(data.len() - done, BSIZE - disp);
            if let Some(p) = self.map.get(&sector) {
                data[done..done + amount].copy_from_slice(&p[disp..disp + amount]);
                self.hits += 1;
            } else {
                let mut p: Box<[u8; BSIZE]> = vec![0; 256].try_into().unwrap();
                stg.read(sector * BSIZE as u64, &mut *p);
                data[done..done + amount].copy_from_slice(&p[disp..disp + amount]);
                self.map.insert(sector, p);
                self.miss += 1;
            }
            done += amount;
        }
        if self.map.len() > 1000 {
            self.map.clear();
        }
    }
}
