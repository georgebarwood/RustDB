use crate::{panic, util, HashMap, Transaction, Rc, Value};
use std::{io::Read, io::Write, net::TcpStream};

/// Response content is accumulated in result.
///
/// ToDo : cookies, files.
pub struct WebTransaction {
    pub method: Rc<String>,
    pub path: Rc<String>,
    pub query: Map,
    pub form: Map,
    pub cookies: Map,
    pub parts: Vec<Part>,
    pub err: String,
    pub output: Vec<u8>,
    pub status_code: String,
    pub headers: String,
    pub now: i64, // Micro-seconds since January 1, 1970 0:00:00 UTC
}

/// Map for query, form, cookies.
pub type Map = HashMap<String, Rc<String>>;

/// Path and Query.
pub type Target = (Rc<String>, Map);

/// Method, Path, Query, Version.
pub type Request = (Rc<String>, Rc<String>, Map, String);

#[derive(Debug)]
pub enum WebErr {
    Io(std::io::Error),
    Utf8(std::string::FromUtf8Error),
    Eof,
    NewlineExpected,
}

fn from_utf8(bytes: Vec<u8>) -> Result<String, WebErr> {
    match String::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(e) => Err(WebErr::Utf8(e)),
    }
}

impl WebTransaction {
    /// Reads the http request from the TCP stream into a new WebTransaction.
    pub fn new(s: &TcpStream) -> Result<Self, WebErr> {
        let mut hp = HttpRequestParser::new(s);
        let (method, path, query, _version) = hp.read_request()?;
        let cookies = hp.read_headers()?;
        let mut form = HashMap::new();
        let mut parts = Vec::new();
        // println!("content_type='{}'", hp.content_type);
        if hp.content_type == "application/x-www-form-urlencoded" {
            form = hp.read_form()?;
        } else if hp.content_type.starts_with("multipart/form-data") {
            parts = hp.read_multipart()?;
        } else {
            let _content = hp.read_content()?;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        let now = now.as_micros() as i64;
        let output = Vec::with_capacity(10000);
        let headers = String::with_capacity(1000);
        let status_code = "200 OK".to_string();
        Ok(Self {
            status_code,
            output,
            headers,
            method,
            path,
            query,
            form,
            cookies,
            parts,
            err: String::new(),
            now,
        })
    }
    pub fn trace(&self) {
        println!(
            "method={} path={} query={:?} input cookies={:?}",
            self.method, self.path, self.query, self.cookies,
        );
    }
    /// Writes the http response to the TCP stream.
    pub fn write(&mut self, tcps: &mut TcpStream) -> Result<(), std::io::Error> {
        let contents = &self.output;
        let status_line = "HTTP/1.1 ".to_string() + &self.status_code;
        let response = format!(
            "{}\r\n{}Content-Length: {}\r\n\r\n",
            status_line,
            self.headers,
            contents.len()
        );
        tcps.write_all(response.as_bytes())?;
        tcps.write_all(contents)?;
        tcps.flush()?;
        Ok(())
    }
    /// Append string to output.
    fn push_str(&mut self, s: &str) {
        self.output.extend_from_slice(s.as_bytes());
    }
}
impl Transaction for WebTransaction {
    fn arg(&mut self, kind: i64, s: &str) -> Rc<String> {
        match kind {
            99 => self.method.clone(),
            0 => self.path.clone(),
            1 => {
                if let Some(s) = self.query.get(s) {
                    s.clone()
                } else {
                    Rc::new(String::new())
                }
            }
            2 => {
                if let Some(s) = self.form.get(s) {
                    s.clone()
                } else {
                    Rc::new(String::new())
                }
            }
            3 => {
                if let Some(s) = self.cookies.get(s) {
                    s.clone()
                } else {
                    Rc::new(String::new())
                }
            }
            10 => {
                self.headers.push_str(s);
                self.headers.push_str("\r\n");
                Rc::new(String::new())
            }
            11 => {
                self.status_code = s.to_string();
                Rc::new(String::new())
            }
            _ => panic!(),
        }
    }
    fn global(&self, kind: i64) -> i64 {
        match kind {
            0 => self.now,
            _ => panic!(),
        }
    }
    fn selected(&mut self, values: &[Value]) {
        for v in values {
            match v {
                Value::String(s) => {
                    self.push_str(s);
                }
                Value::Int(x) => {
                    self.push_str(&x.to_string());
                }
                Value::Float(x) => {
                    self.push_str(&x.to_string());
                }
                Value::RcBinary(x) => {
                    self.output.extend_from_slice(x);
                }
                Value::ArcBinary(x) => {
                    self.output.extend_from_slice(x);
                }
                _ => {
                    panic!("Unexpected value selected")
                }
            }
        }
    }
    fn set_error(&mut self, err: String) {
        self.err = err;
    }
    fn get_error(&mut self) -> String {
        let result = self.err.to_string();
        self.err = String::new();
        result
    }
    fn status_code(&mut self, code: i64) {
        self.status_code = code.to_string();
    }

    fn header(&mut self, name: &str, value: &str) {
        let hdr = name.to_string() + ": " + value + "\r\n";
        self.headers.push_str(&hdr);
    }
}
/// Parser for http request.
///
/// A http request starts with a line with the method, target and protocol version.
/// This is followed by header lines, terminated by a blank line.
/// Each header line has the form name: value.
///
/// Headers define the content-type, content-length and cookies.
///
/// The content (optional) comes after the blank line that terminates the headers.
///
/// The target and content may also be parsed.
///
/// Supported content-types are ( or will be once multipart is done! )
///
/// (1) application/x-www-form-urlencoded : default encoding for forms when method is put.
///
/// Consists of name=value pairs, for example name1=value1&name2=value2....
///
/// [Reserved and] non-alphanumeric characters are replaced by '%HH', a percent sign and two hexadecimal digits representing the ASCII code of the character.
///
/// target uses the same encoding for the "query string", the portion of the target that follows the path, and begins with "?".
///
/// See <https://url.spec.whatwg.org/#application/x-www-form-urlencoded>
///
/// (2) multipart/form-data - typically for uploading files, see <https://www.ietf.org/rfc/rfc2388.txt> and <https://www.w3.org/TR/html401/interact/forms.html>.
pub struct HttpRequestParser<'a> {
    buffer: [u8; 512],
    stream: &'a TcpStream,
    index: usize,       // Into buffer
    count: usize,       // Number of valid bytes in buffer
    base: usize,        // Absolute input position = base+index.
    end_content: usize, // End of content.
    content_length: usize,
    content_type: String,
    eof: bool,
}
/*
  Note: this code needs attention, probably doesn't cope with all error possibilities accurately.
  Also cookies are todo and multipart may not work for multiple files ( or at all ).
*/
impl<'a> HttpRequestParser<'a> {
    pub fn new(stream: &'a TcpStream) -> Self {
        Self {
            stream,
            buffer: [0; 512],
            // buffer: unsafe { std::mem::MaybeUninit::uninit().assume_init() },
            count: 0,
            index: 0,
            base: 0,
            end_content: usize::MAX,
            content_length: 0,
            content_type: String::new(),
            eof: false,
        }
    }

    fn get_byte(&mut self) -> Result<u8, WebErr> {
        if self.eof || self.base + self.index >= self.end_content {
            self.index = 1;
            self.eof = true;
            return Ok(b' ');
        }
        if self.index >= self.count {
            self.base += self.count;

            self.count = match self.stream.read(&mut self.buffer) {
                Ok(n) => n,
                Err(e) => {
                    return Err(WebErr::Io(e));
                }
            };
            assert!(self.count <= self.buffer.len());
            self.index = 0;
            if self.count == 0 {
                return Err(WebErr::Eof);
            }
        }
        let result = self.buffer[self.index];
        self.index += 1;
        Ok(result)
    }

    fn skip_white_space(&mut self) -> Result<(), WebErr> {
        loop {
            let b = self.get_byte()?;
            if b != b' ' && b != 9 {
                self.index -= 1;
                break;
            }
        }
        Ok(())
    }

    fn read_to_bytes(&mut self, to: u8) -> Result<Vec<u8>, WebErr> {
        let mut result = Vec::new();
        loop {
            let b = self.get_byte()?;
            if b == to {
                break;
            }
            if b == 13 || self.eof {
                self.index -= 1;
                break;
            }
            result.push(b);
        }
        Ok(result)
    }

    fn read_to(&mut self, to: u8) -> Result<String, WebErr> {
        let bytes = self.read_to_bytes(to)?;
        from_utf8(bytes)
    }

    fn decode(&mut self, b: u8) -> Result<u8, WebErr> {
        Ok(if b == b'%' {
            let h1 = self.get_byte()?;
            let h2 = self.get_byte()?;
            util::hex(h1) * 16 + util::hex(h2)
        } else if b == b'+' {
            b' '
        } else {
            b
        })
    }

    fn read_coded_str(&mut self, to: u8) -> Result<String, WebErr> {
        let mut bytes = Vec::new();
        loop {
            let b = self.get_byte()?;
            if b == to {
                break;
            }
            if b == b' ' || b == 13 {
                self.index -= 1;
                break;
            }
            bytes.push(self.decode(b)?);
        }
        from_utf8(bytes)
    }

    fn read_map(&mut self) -> Result<Map, WebErr> {
        let mut result = HashMap::new();
        loop {
            let b = self.get_byte()?;
            if b == b' ' {
                break;
            }
            self.index -= 1;
            if b == 13 {
                break;
            }
            let name = self.read_coded_str(b'=')?;
            let value = self.read_coded_str(b'&')?;
            result.insert(name, Rc::new(value));
        }
        Ok(result)
    }

    fn read_target(&mut self) -> Result<Target, WebErr> {
        let mut path = Vec::new();
        let mut query = HashMap::new();
        loop {
            let b = self.get_byte()?;
            if b == b' ' {
                break;
            }
            if b == 13 {
                self.index -= 1;
                break;
            }
            if b == b'?' {
                query = self.read_map()?;
                break;
            }
            path.push(self.decode(b)?);
        }
        let path = from_utf8(path)?;
        let path = Rc::new(path);
        Ok((path, query))
    }

    /// Get Method, path, query and protocol version.
    pub fn read_request(&mut self) -> Result<Request, WebErr> {
        let method = Rc::new(self.read_to(b' ')?);
        let (path, query) = self.read_target()?;
        let version = self.read_to(13)?;
        Ok((method, path, query, version))
    }

    pub fn read_headers(&mut self) -> Result<Map, WebErr> {
        let mut cookies = HashMap::new();
        loop {
            if self.get_byte()? != 10 {
                break;
            }
            let name = self.read_to(b':')?;
            if name.is_empty() {
                break;
            }
            self.skip_white_space()?;

            if name == "Cookie" {
                cookies = self.read_map()?;
                if self.get_byte()? != 13
                {
                  return Err(WebErr::NewlineExpected);
                }
            } else {
                let value = self.read_to(13)?;
                if name == "Content-Type" {
                    self.content_type = value.clone();
                } else if name == "Content-Length" {
                    self.content_length = value.parse::<usize>().unwrap();
                }
            }
        }
        // Read CR/LF
        if self.get_byte()? != 13 || self.get_byte()? != 10 {
            Err(WebErr::NewlineExpected)
        } else {
            Ok(cookies)
        }
    }

    pub fn read_content(&mut self) -> Result<String, WebErr> {
        let mut result = Vec::new();
        let mut n = self.content_length;
        while n > 0 {
            result.push(self.get_byte()?);
            n -= 1;
        }
        from_utf8(result)
    }

    pub fn read_form(&mut self) -> Result<Map, WebErr> {
        self.end_content = self.base + self.index + self.content_length;
        self.read_map()
    }
    pub fn read_multipart(&mut self) -> Result<Vec<Part>, WebErr> {
        /* Typical multipart body would be:
        ------WebKitFormBoundaryVXXOTFUWdfGpOcFK
        Content-Disposition: form-data; name="f1"; filename="test.txt"
        Content-Type: text/plain

        Hello there

        ------WebKitFormBoundaryVXXOTFUWdfGpOcFK
        Content-Disposition: form-data; name="submit"

        Upload
        ------WebKitFormBoundaryVXXOTFUWdfGpOcFK--
        */
        self.end_content = self.base + self.index + self.content_length;

        let result = Vec::new();
        let seperator = self.read_to_bytes(13)?;

        /* Now need to repeated read multipart headers, then body.
           Each body is terminated by the seperator.
        */
        let mut ok = true;
        while ok {
            self.read_headers()?;
            // println!("headers={:?}", _headers);
            let mut body = Vec::new();
            ok = false;
            while !self.eof {
                let b = self.get_byte()?;
                body.push(b);
                // Check to see if we matched separator
                if ends_with(&body, &seperator) {
                    println!("Got seperator");
                    let b = self.get_byte()?;
                    ok = b == 13;
                    break;
                }
            }
        }
        while !self.eof {
            self.get_byte()?;
        }
        Ok(result)
    }
}

fn ends_with(body: &[u8], sep: &[u8]) -> bool {
    let bn = body.len();
    let sn = sep.len();
    if bn < sn {
        return false;
    }
    if &body[bn - sn..bn] == sep {
        return true;
    }
    false
}

pub struct Part {
    pub filename: Rc<String>,
    pub data: Rc<Vec<u8>>,
}
