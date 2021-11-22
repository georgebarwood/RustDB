use crate::{panic, HashMap, Query, Rc, Value};

/// General Query.
pub struct GenQuery {
    pub path: String,
    pub params: HashMap<String, String>,
    pub form: HashMap<String, String>,
    pub cookies: HashMap<String, String>,
    pub parts: Vec<Part>,
    pub err: String,
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub output: Vec<u8>,
    pub now: i64, // Micro-seconds since January 1, 1970 0:00:00 UTC
}

impl GenQuery {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        let now = now.as_micros() as i64;
        let output = Vec::with_capacity(10000);
        let headers = Vec::new();
        let status_code = 200;
        Self {
            path: String::new(),
            params: HashMap::new(),
            form: HashMap::new(),
            cookies: HashMap::new(),
            parts: Vec::new(),
            err: String::new(),
            output,
            status_code,
            headers,
            now,
        }
    }
    /// Append string to output.
    fn push_str(&mut self, s: &str) {
        self.output.extend_from_slice(s.as_bytes());
    }
}

impl Query for GenQuery {
    fn arg(&mut self, kind: i64, s: &str) -> Rc<String> {
        let result: &str = match kind {
            0 => &self.path,
            1 => {
                if let Some(s) = self.params.get(s) {
                    s
                } else {
                    ""
                }
            }
            2 => {
                if let Some(s) = self.form.get(s) {
                    s
                } else {
                    ""
                }
            }
            3 => {
                if let Some(s) = self.cookies.get(s) {
                    s
                } else {
                    ""
                }
            }
            _ => "",
        };
        Rc::new(result.to_string())
    }

    fn status_code(&mut self, code: i64) {
        self.status_code = code as u16;
    }

    fn header(&mut self, name: &str, value: &str) {
        self.headers.push((name.to_string(), value.to_string()));
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
                Value::Binary(x) => {
                    self.output.extend_from_slice(x);
                }
                _ => {
                    panic!()
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
    fn file_attr(&mut self, k: i64, x: i64) -> Rc<String> {
        let k = k as usize;
        let result: &str = {
            if k >= self.parts.len() {
                ""
            } else {
                let p = &self.parts[k];
                match x {
                    0 => &p.name,
                    1 => &p.content_type,
                    2 => &p.file_name,
                    3 => &p.text,
                    _ => panic!(),
                }
            }
        };
        Rc::new(result.to_string())
    }
    fn file_content(&mut self, k: i64) -> Rc<Vec<u8>> {
        let bytes = std::mem::take(&mut self.parts[k as usize].data);
        Rc::new(bytes)
    }
}

impl Default for GenQuery {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Part {
    pub name: String,
    pub file_name: String,
    pub content_type: String,
    pub text: String,
    pub data: Vec<u8>,
}
