use crate::*;

pub struct GenQuery {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub form: HashMap<String, String>,
    pub parts: Vec<Part>,
    pub err: String,
    pub output: Vec<u8>,
    pub status_code: String,
    pub headers: String,
    pub now: i64, // Micro-seconds since January 1, 1970 0:00:00 UTC
}

impl GenQuery {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        let now = now.as_micros() as i64;
        let output = Vec::with_capacity(10000);
        let headers = String::with_capacity(1000);
        let status_code = "200 OK".to_string();
        Self {
            method: String::new(),
            path: String::new(),
            query: HashMap::new(),
            form: HashMap::new(),
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
            99 => &self.method,
            0 => &self.path,
            1 => {
                if let Some(s) = self.query.get(s) {
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
            10 => {
                self.headers.push_str(s);
                self.headers.push_str("\r\n");
                ""
            }
            11 => {
                self.status_code = s.to_string();
                ""
            }
            _ => panic!(),
        };
        Rc::new(result.to_string())
    }
    fn global(&self, kind: i64) -> i64 {
        match kind {
            0 => self.now,
            _ => panic!(),
        }
    }
    fn push(&mut self, values: &[Value]) {
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
}

impl Default for GenQuery {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Part {
    pub name: String,
    pub filename: String,
    pub contenttype: String,
    pub text: String,
    pub data: Vec<u8>,
}
