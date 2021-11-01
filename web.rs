use crate::*;
use std::{io::Read, io::Write, net::TcpStream};

/// Response content is accumulated in result.
///
/// ToDo : cookies, files.
pub struct WebQuery
{
  pub method: Rc<String>,
  pub path: Rc<String>,
  pub query: HashMap<String, Rc<String>>,
  pub form: HashMap<String, Rc<String>>,

  pub err: String,
  pub output: Vec<u8>,
  pub status_code: String,
  pub headers: String,
  pub now: i64, // Micro-seconds since January 1, 1970 0:00:00 UTC
}

impl WebQuery
{
  /// Reads the http request from the TCP stream into a new WebQuery.
  pub fn new(s: &TcpStream) -> Self
  {
    let mut hp = HttpRequestParser::new(s);
    let (method, path, query, _version) = hp.read_request();
    let _input_headers = hp.read_headers();

    let mut form = HashMap::new();
    if hp.content_type == "application/x-www-form-urlencoded"
    {
      form = hp.read_form();
    }
    else
    {
      let _content = hp.read_content();
    }

    let now = std::time::SystemTime::now()
      .duration_since(std::time::SystemTime::UNIX_EPOCH)
      .unwrap();
    let now = now.as_micros() as i64;
    let output = Vec::with_capacity(10000);
    let headers = String::with_capacity(1000);
    let status_code = "200 OK".to_string();

    Self { status_code, output, headers, method, path, query, form, err: String::new(), now }
  }

  pub fn trace(&self)
  {
    println!("method={} path={} query={:?}", self.method, self.path, self.query);
  }

  /// Writes the http response to the TCP stream.
  pub fn write(&mut self, tcps: &mut TcpStream)
  {
    let contents = &self.output;
    let status_line = "HTTP/1.1 ".to_string() + &self.status_code;

    let response = format!(
      "{}\r\n{}Content-Length: {}\r\n\r\n",
      status_line,
      self.headers,
      contents.len()
    );

    // println!( "status line={}", status_line );
    // println!( "response={}", response );

    tcps.write_all(response.as_bytes()).unwrap();
    tcps.write_all(contents).unwrap();
    tcps.flush().unwrap();
  }

  /// Append string to output.
  fn push_str(&mut self, s: &str)
  {
    self.output.extend_from_slice(s.as_bytes());
  }
}

impl Query for WebQuery
{
  fn arg(&mut self, kind: i64, s: &str) -> Rc<String>
  {
    match kind
    {
      99 => self.method.clone(),
      0 => self.path.clone(),
      1 =>
      {
        if let Some(s) = self.query.get(s)
        {
          s.clone()
        }
        else
        {
          Rc::new(String::new())
        }
      }
      2 =>
      {
        if let Some(s) = self.form.get(s)
        {
          s.clone()
        }
        else
        {
          Rc::new(String::new())
        }
      }
      10 =>
      {
        self.headers.push_str(s);
        self.headers.push_str("\r\n");
        Rc::new(String::new())
      }
      11 =>
      {
        self.status_code = s.to_string();
        Rc::new(String::new())
      }
      _ => panic!(),
    }
  }

  fn global(&self, kind: i64) -> i64
  {
    match kind
    {
      0 => self.now,
      _ => panic!(),
    }
  }

  fn push(&mut self, values: &[Value])
  {
    for v in values
    {
      match v
      {
        Value::String(s) =>
        {
          self.push_str(s);
        }
        Value::Int(x) =>
        {
          self.push_str(&x.to_string());
        }
        Value::Float(x) =>
        {
          self.push_str(&x.to_string());
        }
        Value::Binary(x) =>
        {
          self.output.extend_from_slice(x);
        }
        _ =>
        {
          panic!("push bad value={:?}", v)
        }
      }
    }
  }

  fn set_error(&mut self, err: String)
  {
    self.err = err;
  }

  fn get_error(&mut self) -> String
  {
    self.err.to_string()
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

struct HttpRequestParser<'a>
{
  buffer: [u8; 512],
  stream: &'a TcpStream,
  index: usize,       // Into buffer
  count: usize,       // Number of valid bytes in buffer
  base: usize,        // Absolute input position = base+index.
  end_content: usize, // End of content.
  content_length: usize,
  content_type: String,
}

impl<'a> HttpRequestParser<'a>
{
  pub fn new(stream: &'a TcpStream) -> Self
  {
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
    }
  }

  fn get_byte(&mut self) -> u8
  {
    if self.base + self.index == self.end_content
    {
      self.index += 1;
      return b' ';
    }
    if self.index == self.count
    {
      self.base += self.count;
      self.count = self.stream.read(&mut self.buffer).unwrap();
      self.index = 0;
    }
    let result = self.buffer[self.index];
    self.index += 1;
    result
  }

  fn skip_white_space(&mut self)
  {
    loop
    {
      let b = self.get_byte();
      if b != 32 && b != 9
      {
        self.index -= 1;
        break;
      }
    }
  }

  fn read_to(&mut self, to: u8) -> String
  {
    let mut result = Vec::new();
    loop
    {
      let b = self.get_byte();
      if b == to
      {
        break;
      }
      if b == 13
      {
        self.index -= 1;
        break;
      }
      result.push(b);
    }
    String::from_utf8(result).unwrap()
  }

  fn decode(&mut self, b: u8) -> u8
  {
    if b == b'%'
    {
      let h1 = self.get_byte();
      let h2 = self.get_byte();
      util::hex(h1) * 16 + util::hex(h2)
    }
    else if b == b'+'
    {
      b' '
    }
    else
    {
      b
    }
  }

  fn read_coded_str(&mut self, to: u8) -> String
  {
    let mut result = Vec::new();
    loop
    {
      let b = self.get_byte();
      if b == to
      {
        break;
      }
      if b == b' ' || b == 13
      {
        self.index -= 1;
        break;
      }
      result.push(self.decode(b));
    }
    String::from_utf8(result).unwrap()
  }

  fn read_map(&mut self) -> HashMap<String, Rc<String>>
  {
    let mut result = HashMap::new();
    loop
    {
      let b = self.get_byte();
      if b == b' '
      {
        break;
      }
      self.index -= 1;
      if b == 13
      {
        break;
      }
      let name = self.read_coded_str(b'=');
      let value = self.read_coded_str(b'&');
      result.insert(name, Rc::new(value));
    }
    result
  }

  fn read_target(&mut self) -> (Rc<String>, HashMap<String, Rc<String>>)
  {
    let mut path = Vec::new();
    let mut query = HashMap::new();
    loop
    {
      let b = self.get_byte();
      if b == b' '
      {
        break;
      }
      if b == 13
      {
        self.index -= 1;
        break;
      }
      if b == b'?'
      {
        query = self.read_map();
        break;
      }
      path.push(self.decode(b));
    }
    let path = Rc::new(String::from_utf8(path).unwrap());
    (path, query)
  }

  /// Get Method, path, query and protocol version.
  pub fn read_request(&mut self) -> (Rc<String>, Rc<String>, HashMap<String, Rc<String>>, String)
  {
    let method = Rc::new(self.read_to(b' '));
    let (path, query) = self.read_target();
    let version = self.read_to(13);
    (method, path, query, version)
  }

  fn read_header(&mut self) -> Option<(String, String)>
  {
    assert!(self.get_byte() == 10);
    let name = self.read_to(b':');
    if name.is_empty()
    {
      return None;
    }
    self.skip_white_space();
    let value = self.read_to(13);
    if name == "Content-Type"
    {
      self.content_type = value.clone();
    }
    else if name == "Content-Length"
    {
      self.content_length = value.parse::<usize>().unwrap();
    }
    Some((name, value))
  }

  pub fn read_headers(&mut self) -> Vec<(String, String)>
  {
    let mut result = Vec::new();
    while let Some(pair) = self.read_header()
    {
      result.push(pair);
    }
    assert!(self.get_byte() == 13);
    assert!(self.get_byte() == 10);
    result
  }

  pub fn read_content(&mut self) -> String
  {
    let mut result = Vec::new();
    let mut n = self.content_length;
    while n > 0
    {
      result.push(self.get_byte());
      n -= 1;
    }
    String::from_utf8(result).unwrap()
  }

  pub fn read_form(&mut self) -> HashMap<String, Rc<String>>
  {
    self.end_content = self.base + self.index + self.content_length;
    self.read_map()
  }
}
