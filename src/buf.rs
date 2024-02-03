use crate::stg::Storage;

///
pub struct WriteBuffer {
    ///
    pub stg: Box<dyn Storage>,
    pos: u64, // Current writing position
    ix: usize,
    buf: [u8; BUF_SIZE],
    _write_count: u64,
    _flush_count: u64,
}

const BUF_SIZE: usize = 64 * 1024;

impl WriteBuffer {
    ///
    pub fn new(stg: Box<dyn Storage>) -> Self {
        Self {
            stg,
            pos: u64::MAX,
            ix: 0,
            buf: [0; BUF_SIZE],
            _write_count: 0,
            _flush_count: 0,
        }
    }

    ///
    pub fn write(&mut self, off: u64, data: &[u8]) {
        if self.pos + self.ix as u64 != off {
            self.flush(off);
        }
        let mut done: usize = 0;
        let mut todo: usize = data.len();
        while todo > 0 {
            let mut n: usize = BUF_SIZE - self.ix;
            if n == 0 {
                self.flush(off + done as u64);
                n = BUF_SIZE;
            }
            if n > todo {
                n = todo;
            }
            let buf = self.buf.as_mut_slice();
            buf[self.ix..self.ix + n].copy_from_slice(&data[done..done + n]);
            todo -= n;
            done += n;
            self.ix += n;
        }
        self._write_count += 1;
    }

    ///
    fn flush(&mut self, pos: u64) {
        if self.ix > 0 {
            self.stg.write(self.pos, &self.buf[0..self.ix]);
            self._flush_count += 1;
        }
        self.ix = 0;
        self.pos = pos;
    }

    ///
    pub fn commit(&mut self, size: u64) {
        self.flush(u64::MAX);
        self.stg.commit(size);
        // if size > 0 { println!("commit size={size} write_count={} flush_count={}", self._write_count, self._flush_count); }
        self._write_count = 0;
        self._flush_count = 0;
    }

    ///
    pub fn write_u64(&mut self, start: u64, value: u64) {
        self.write(start, &value.to_le_bytes());
    }
}
