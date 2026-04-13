use crate::share::{Error, SharedState, Trans, U_COUNT, U_CPU, U_READ, U_WRITE, UseInfo};
use rustdb::BTreeMap;
use rustdb::alloc::Perm;
use rustdb::gentrans::GenQuery;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Process http request.
pub async fn process(
    mut stream: tokio::net::TcpStream,
    ip: String,
    ss: Arc<SharedState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (r, mut w) = stream.split();
    let mut r = Buffer::new(r, ss.clone(), ip);

    let h = Headers::get(&mut r).await;

    let h = match h {
        Ok(h) => h,
        Err(e) => {
            if e.code == 0 {
                return Ok(());
            }
            return Err(e)?;
        }
    };

    let (hdrs, outp) = {
        let mut t = Trans::new_with_state(ss.clone(), r.uid.clone());
        let readonly =
            h.method == b"GET" && !h.args.contains_key("save") || h.args.contains_key("readonly");

        t.x.qy.path = h.path;
        t.x.qy.params = h.args;
        t.x.qy.cookies = h.cookies;
        let (ct, clen) = (&h.content_type, h.content_length);

        // Set limits based on login info etc.
        t.readonly = true;
        let save = t.x.qy.sql.clone();
        t.x.qy.sql = Arc::new("EXEC web.SetUser()".to_string());
        t = ss.process(t).await;
        t.x.qy.sql = save;
        r.u.limit = ss.u_budget(t.uid.clone());
        t.readonly = false;

        if ct.is_empty() {
            // No body.
        } else if ct == b"application/x-www-form-urlencoded" {
            let clen: usize = clen.parse()?;
            let bytes = r.read(clen).await?;
            t.x.qy.form = serde_urlencoded::from_bytes(&bytes)?;
        } else if is_multipart(ct) {
            get_multipart(&mut r, &mut t.x.qy).await?;
        } else {
            t.x.rp.status_code = 501;
        }
        r.read_complete();

        if t.x.rp.status_code == 200 {
            t.readonly = readonly;
            t = ss.process(t).await;
            r.uid = t.uid.clone();
            r.u.used[U_CPU] = t.run_time.as_micros() as u64;
            if ss.tracetime {
                println!(
                    "run time={}µs updates={} readonly={} path={} args={:?}",
                    t.run_time.as_micros(),
                    t.updates,
                    readonly,
                    t.x.qy.path,
                    t.x.qy.params,
                );
            }
            if ss.tracemem {
                let s = ss.spd.stash.lock().unwrap();
                println!(
                    "stash limit={}K used={}K free={}K pages={} cached={} read={} misses={} allocs={}",
                    s.mem_limit / 1024,
                    s.total / 1024,
                    (s.mem_limit as i64 - s.total) / 1024,
                    s.pages.len(),
                    s.cached(),
                    s.read,
                    s.miss,
                    Perm::alloc_count()
                );
                println!( "Perm::info = {:?}", Perm::info() );
            }
        }
        (header(&t), t.x.rp.output)
    };

    let budget = r.u.limit[U_WRITE];
    write(&mut w, &hdrs, budget, &mut r.u.used[U_WRITE]).await?;
    write(&mut w, &outp, budget, &mut r.u.used[U_WRITE]).await?;
    Ok(())
}

/// Get response header.
fn header(t: &Trans) -> Vec<u8> {
    let mut h = Vec::with_capacity(4096);
    let status_line = format!("HTTP/1.1 {}\r\n", t.x.rp.status_code);
    h.extend_from_slice(status_line.as_bytes());
    for (name, value) in &t.x.rp.headers {
        h.extend_from_slice(name.as_bytes());
        h.push(b':');
        h.extend_from_slice(value.as_bytes());
        h.push(13);
        h.push(10);
    }
    let clen = t.x.rp.output.len();
    let x = format!("Content-Length: {clen}\r\n\r\n");
    h.extend_from_slice(x.as_bytes());
    h
}

/// Header parsing.
#[derive(Default)]
struct Headers {
    method: Vec<u8>,
    path: String,
    args: BTreeMap<String, String>,
    host: String,
    cookies: BTreeMap<String, String>,

    content_type: Vec<u8>,
    content_length: String,
}

impl Headers {
    async fn get<'a>(br: &mut Buffer<'a>) -> Result<Headers, Error> {
        let mut r = Self::default();
        br.read_until(b' ', &mut r.method).await?;
        r.method.pop(); // Remove trailing space.

        let mut pq = Vec::new();
        br.read_until(b' ', &mut pq).await?;
        pq.pop(); // Remove trailing space.
        r.split_pq(&pq)?;

        let mut protocol = Vec::new();
        br.read_until(b'\n', &mut protocol).await?;

        let mut line0 = Vec::new();
        loop {
            let n = br.read_until(b'\n', &mut line0).await?;
            if n <= 2 {
                break;
            }
            let line = &line0[0..n - 2];
            if line.len() >= 2 {
                let b0 = lower(line[0]);
                let b2 = lower(line[2]);
                match (b0, b2) {
                    (b'c', b'o') => {
                        if let Some(line) = line_is(line, b"cookie") {
                            r.cookies = cookie_map(line)?;
                        }
                    }
                    (b'c', b'n') => {
                        if let Some(line) = line_is(line, b"content-type") {
                            r.content_type = line.to_vec();
                        } else if let Some(line) = line_is(line, b"content-length") {
                            r.content_length = tos(line)?;
                        }
                    }
                    (b'h', b's') => {
                        if let Some(line) = line_is(line, b"host") {
                            r.host = tos(line)?;
                        }
                    }
                    (b'x', b'r') => {
                        if let Some(line) = line_is(line, b"x-real-ip") {
                            let ip = tos(line)?;
                            br.u.limit = br.ss.u_budget(ip.clone());
                            br.uid = ip;
                            if br.u.limit[U_COUNT] == 0 {
                                return Err(tmr());
                            }
                        }
                    }
                    _ => {}
                }
            }
            line0.clear();
        }
        Ok(r)
    }

    /// Split the path and args by finding '?'.
    fn split_pq(&mut self, pq: &[u8]) -> Result<(), Error> {
        let n = pq.len();
        let mut i = 0;
        let mut q = n;
        while i < n {
            if pq[i] == b'?' {
                q = i;
                break;
            }
            i += 1;
        }
        self.path = tos(&pq[0..q])?;
        if q != n {
            q += 1;
        }
        let qs = &pq[q..n];
        self.args = serde_urlencoded::from_bytes(qs)?;
        Ok(())
    }
}

/// Check whether current line is named header.
fn line_is<'a>(line: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let n = name.len();
    if line.len() < n + 1 {
        return None;
    }
    if line[n] != b':' {
        return None;
    }
    for i in 0..n {
        if lower(line[i]) != name[i] {
            return None;
        }
    }
    let mut skip = n + 1;
    let n = line.len();
    while skip < n && line[skip] == b' ' {
        skip += 1;
    }
    Some(&line[skip..n])
}

/// Map upper case char to lower case.
fn lower(mut b: u8) -> u8 {
    if b.is_ascii_uppercase() {
        b += 32;
    }
    b
}

/// Convert byte slice into string.
fn tos(s: &[u8]) -> Result<String, Error> {
    Ok(std::str::from_utf8(s)?.to_string())
}

/// Not enough input.
fn eof() -> Error {
    Error { code: 0 }
}

/// Too many requests.
fn tmr() -> Error {
    Error { code: 429 }
}

/// Some other error.
fn bad() -> Error {
    Error { code: 400 }
}

/// Parse cookie header to a map of cookies.
fn cookie_map(s: &[u8]) -> Result<BTreeMap<String, String>, Error> {
    let mut map = BTreeMap::new();
    let n = s.len();
    let mut i = 0;

    while i < n {
        while i < n && s[i] == b' ' {
            i += 1;
        }
        let start = i;
        while i < n && s[i] != b'=' {
            i += 1;
        }
        let name = tos(&s[start..i])?;
        if i < n {
            i += 1;
        }
        let start = i;
        while i < n && s[i] != b';' {
            i += 1;
        }
        let value = tos(&s[start..i])?;
        i += 1;
        map.insert(name, value);
    }
    Ok(map)
}

/// Check content-type is multipart.
fn is_multipart(s: &[u8]) -> bool {
    let temp = b"multipart/form-data";
    let n = temp.len();
    s.len() >= n && temp == &s[0..n]
}

/// Extract name and file_name from content-disposition header.
fn split_cd(s: &[u8]) -> Option<(String, String)> {
    /* Expected input:
       form-data; name="file"; filename="logo.png"
    */
    if let Ok(s) = std::str::from_utf8(s) {
        let s = "multipart/".to_string() + s;
        let (mut name, mut filename) = ("", "");
        let m: mime::Mime = s.parse().ok()?;
        if m.subtype() != mime::FORM_DATA {
            return None;
        }
        if let Some(n) = m.get_param("name") {
            name = n.as_str()
        }
        if let Some(n) = m.get_param("filename") {
            filename = n.as_str()
        }
        Some((name.to_string(), filename.to_string()))
    } else {
        None
    }
}

/*
Parts are delimited by boundary lines.
Each boundary line starts with --
The final boundary line has an extra --
Each part has headers, typically Content-Disposition and Content-Type.
Example:
------WebKitFormBoundaryAhgB6VordnzCD84Z
Content-Disposition: form-data; name="file"; filename=""
Content-Type: application/octet-stream


------WebKitFormBoundaryAhgB6VordnzCD84Z
Content-Disposition: form-data; name="submit"

Upload
------WebKitFormBoundaryAhgB6VordnzCD84Z--
*/

use rustdb::Part;

/// Parse multipart body.
async fn get_multipart<'a>(br: &mut Buffer<'a>, q: &mut GenQuery) -> Result<(), Error> {
    let mut boundary = Vec::new();
    let n = br.read_until(10, &mut boundary).await?;
    if n < 4 {
        return Err(eof())?;
    }

    let bn = boundary.len() - 2;
    boundary.truncate(bn);

    let mut got_last = false;
    while !got_last {
        let mut part = Part::default();
        // Read headers
        let mut line0 = Vec::new();
        loop {
            let n = br.read_until(10, &mut line0).await?;
            if n <= 2 {
                break;
            }
            let line = &line0[0..n - 2];
            if let Some(line) = line_is(line, b"content-type") {
                part.content_type = tos(line)?;
                // Note: if part content-type is multipart, maybe it should be parsed.
            } else if let Some(line) = line_is(line, b"content-disposition")
                && let Some((name, file_name)) = split_cd(line)
            {
                part.name = name;
                part.file_name = file_name;
            }
            line0.clear();
        }
        // Read lines into data looking for boundary.
        let mut data = Vec::new();
        loop {
            let n = br.read_until(10, &mut data).await?;
            if n == bn + 2 || n == bn + 4 {
                let start = data.len() - n;
                if data[start..start + bn] == boundary {
                    got_last = n == bn + 4;
                    data.truncate(start - 2);
                    break;
                }
            }
        }
        if part.content_type.is_empty() {
            let value = tos(&data)?;
            q.form.insert(part.name, value);
        } else {
            part.data = Arc::new(data);
            q.parts.push(part);
        }
    }
    Ok(())
}

/// Buffer size.
const BUFFER_SIZE: usize = 2048;

/// Buffer for reading tcp input stream, with budget check.
struct Buffer<'a> {
    stream: tokio::net::tcp::ReadHalf<'a>,
    buf: [u8; BUFFER_SIZE],
    i: usize,
    n: usize,
    total: u64,
    u: UseInfo,
    timer: std::time::SystemTime,
    ss: Arc<SharedState>,
    uid: String,
}

impl<'a> Drop for Buffer<'a> {
    fn drop(&mut self) {
        self.read_complete();
        self.ss.u_inc(&self.uid, self.u.used);
    }
}

impl<'a> Buffer<'a> {
    /// Create a new Buffer.
    fn new(stream: tokio::net::tcp::ReadHalf<'a>, ss: Arc<SharedState>, uid: String) -> Self {
        let limit = ss.u_budget(uid.clone());
        let mut result = Self {
            stream,
            buf: [0; 2048],
            i: 0,
            n: 0,
            total: 0,
            timer: std::time::SystemTime::now(),
            ss,
            u: UseInfo::default(),
            uid,
        };
        result.u.used[U_COUNT] = 1;
        result.u.limit = limit;
        result
    }

    /// Update used read counter based on total bytes read (KB) and elapsed time (milli-seconds).
    fn read_complete(&mut self) {
        if self.total != 0 {
            let elapsed = 1 + self.timer.elapsed().unwrap().as_millis() as u64;
            self.u.used[U_READ] = elapsed * (self.total >> 10);
            self.total = 0;
        }
    }

    /// Fill the buffer. A timeout is set based on the total already read and the buffer size (KB).
    async fn fill(&mut self) -> Result<(), Error> {
        self.i = 0;
        let lim = self.u.limit[U_READ] / ((self.total + BUFFER_SIZE as u64) >> 10);
        let bm = core::time::Duration::from_millis(lim);
        let used = self.timer.elapsed().unwrap();
        if used >= bm {
            return Err(tmr());
        }
        let timeout = bm - used;

        tokio::select! {
            _ = tokio::time::sleep(timeout) =>
            {
               Err(tmr())?
            }
            rd = self.stream.read(&mut self.buf) =>
            {
                match rd
                {
                   Ok(n) =>
                   {
                     if n == 0 {
                        Err(eof())?
                     }
                     self.n = n;
                     self.total += n as u64;
                   }
                   Err(e) => { Err(e)? }
                }
            }

        }
        Ok(())
    }

    /// Read until delim is found. Returns eof error if input is closed.
    async fn read_until(&mut self, delim: u8, to: &mut Vec<u8>) -> Result<usize, Error> {
        let start = to.len();
        loop {
            if self.i == self.n {
                self.fill().await?;
            }
            let b = self.buf[self.i];
            self.i += 1;
            to.push(b);
            if b == delim {
                return Ok(to.len() - start);
            }
        }
    }

    /// Read specified number of bytes.
    async fn read(&mut self, n: usize) -> Result<Vec<u8>, Error> {
        let mut to = Vec::new();
        loop {
            if self.i == self.n {
                self.fill().await?;
            }
            let b = self.buf[self.i];
            self.i += 1;
            to.push(b);
            if to.len() == n {
                return Ok(to);
            }
        }
    }
}

/// Function to write response, with budget-based timeout.
async fn write<'a>(
    w: &mut tokio::net::tcp::WriteHalf<'a>,
    data: &[u8],
    budget: u64,
    used: &mut u64,
) -> Result<(), Error> {
    let mut result = Ok(());
    if !data.is_empty() {
        let timer = std::time::SystemTime::now();
        let lim = (budget - *used) / ((data.len() >> 10) + 1) as u64;
        let timeout = core::time::Duration::from_millis(lim);
        tokio::select! {
            _ = tokio::time::sleep(timeout) =>
                {
                    result = Err(tmr());
                }
            x = w.write_all(data) =>
                {
                    if let Err(_e) = x { result = Err(bad()); }
                }
        }
        let elapsed = timer.elapsed().unwrap();
        *used += elapsed.as_millis() as u64 * (data.len() as u64 >> 10);
    }
    result
}
