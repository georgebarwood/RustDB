use crate::stg::Storage;

const BUF_SIZE: usize = 1024 * 1024;

/// Write Buffer.
pub struct WriteBuffer {
    ix: usize,
    pos: u64,
    ///
    pub stg: Box<dyn Storage>,
    write: u64,
    flush: u64,
    total: u64,
    buf: Vec<u8>,
}

impl WriteBuffer {
    ///
    pub fn new(stg: Box<dyn Storage>) -> Self {
        Self {
            ix: 0,
            pos: u64::MAX,
            stg,
            write: 0,
            flush: 0,
            total: 0,
            buf: vec![0; BUF_SIZE],
        }
    }

    ///
    pub fn write(&mut self, off: u64, data: &[u8]) {
        if self.pos + self.ix as u64 != off {
            self.flush(off);
        }
        let mut done: usize = 0;
        let mut todo: usize = data.len();
        self.write += 1;
        self.total += todo as u64;
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
            // println!("WriterBuffer flush pos={} size={}", self.pos, self.ix);
            self.stg.write(self.pos, &self.buf[0..self.ix]);
            self.flush += 1;
        }
        self.ix = 0;
        self.pos = new_pos;
    }

    ///
    pub fn commit(&mut self, size: u64) {
        self.flush(u64::MAX);
        #[cfg(feature = "log")]
        if size > 0 {
            println!(
                "WriteBuffer commit size={size} write={} flush={} total={}",
                self.write, self.flush, self.total
            );
        }
        self.stg.commit(size);
        self.write = 0;
        self.flush = 0;
        self.total = 0;
    }

    ///
    pub fn write_u64(&mut self, start: u64, value: u64) {
        self.write(start, &value.to_le_bytes());
    }
}
