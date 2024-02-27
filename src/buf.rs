use crate::{stg::Storage, HashMap};
use std::cmp::min;

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
    pub fn new(stg: Box<dyn Storage>, buf_size: usize) -> Self {
        Self {
            ix: 0,
            pos: u64::MAX,
            stg,
            buf: vec![0; buf_size],
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
            let mut n: usize = self.buf.len() - self.ix;
            if n == 0 {
                self.flush(off + done as u64);
                n = self.buf.len();
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

/// ReadBufStg buffers small (up to limit) reads to the underlying storage using multiple buffers. Only supported functions are read and reset.
///
/// See implementation of AtomicFile for how this is used in conjunction with WMap.
///
/// N is buffer size.

pub struct ReadBufStg<const N: usize> {
    stg: Box<dyn Storage>,
    buf: Mutex<ReadBuffer<N>>,
    limit: usize,
}

impl<const N: usize> Drop for ReadBufStg<N> {
    fn drop(&mut self) {
        self.reset();
    }
}

impl<const N: usize> ReadBufStg<N> {
    /// limit is the size of a read that is considered "small", max_buf is the maximum number of buffers used.
    pub fn new(stg: Box<dyn Storage>, limit: usize, max_buf: usize) -> Box<Self> {
        Box::new(Self {
            stg,
            buf: Mutex::new(ReadBuffer::<N>::new(max_buf)),
            limit,
        })
    }
}

impl<const N: usize> Storage for ReadBufStg<N> {
    /// Read data from storage.
    fn read(&self, start: u64, data: &mut [u8]) {
        if data.len() <= self.limit {
            self.buf.lock().unwrap().read(&*self.stg, start, data);
        } else {
            self.stg.read(start, data);
        }
    }

    /// Clears the buffers.
    fn reset(&mut self) {
        self.buf.lock().unwrap().reset();
    }

    /// Panics.
    fn size(&self) -> u64 {
        panic!()
    }

    /// Panics.
    fn write(&mut self, _start: u64, _data: &[u8]) {
        panic!();
    }

    /// Panics.
    fn commit(&mut self, _size: u64) {
        panic!();
    }
}

struct ReadBuffer<const N: usize> {
    map: HashMap<u64, Box<[u8; N]>>,
    max_buf: usize,
    hits: u64,
}

impl<const N: usize> ReadBuffer<N> {
    fn new(max_buf: usize) -> Self {
        Self {
            map: HashMap::default(),
            max_buf,
            hits: 0,
        }
    }

    fn reset(&mut self) {
        #[cfg(feature = "log")]
        println!(
            "ReadBuffer reset entries={} hits={}",
            self.map.len(),
            self.hits
        );

        self.hits = 0;
        self.map.clear();
    }

    fn read(&mut self, stg: &dyn Storage, off: u64, data: &mut [u8]) {
        let mut done = 0;
        while done < data.len() {
            let off = off + done as u64;
            let sector = off / N as u64;
            let disp = (off % N as u64) as usize;
            let amount = min(data.len() - done, N - disp);
            if let Some(p) = self.map.get(&sector) {
                data[done..done + amount].copy_from_slice(&p[disp..disp + amount]);
                self.hits += 1;
            } else {
                let mut p: Box<[u8; N]> = vec![0; N].try_into().unwrap();
                stg.read(sector * N as u64, &mut *p);
                data[done..done + amount].copy_from_slice(&p[disp..disp + amount]);
                if self.map.len() >= self.max_buf {
                    self.reset();
                }
                self.map.insert(sector, p);
            }
            done += amount;
        }
    }
}
