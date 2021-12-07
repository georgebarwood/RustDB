use crate::{panic, Arc, Any, BTreeMap, Data, Rc, Transaction, Value};

use serde::{Deserialize, Serialize};

/// General Query.
#[derive(Serialize, Deserialize, Debug)]
pub struct GenQuery {
    pub path: String,
    pub params: BTreeMap<String, String>,
    pub form: BTreeMap<String, String>,
    pub cookies: BTreeMap<String, String>,
    pub parts: Vec<Part>,
    pub now: i64, // Micro-seconds since January 1, 1970 0:00:00 UTC
}

/// General Response.
pub struct GenResponse {
    pub err: String,
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub output: Vec<u8>,
}

/// Query + Response, implements Transaction.
pub struct GenTransaction {
    pub qy: GenQuery,
    pub rp: GenResponse,
    pub ext: Arc<dyn Any+Send+Sync>,
}

/// Part of multipart data ( uploaded files ).
#[derive(Serialize, Deserialize, Debug)]
pub struct Part {
    pub name: String,
    pub file_name: String,
    pub content_type: String,
    pub text: String,
    pub data: Data,
}

impl GenTransaction {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        let now = now.as_micros() as i64;
        let output = Vec::with_capacity(10000);
        let headers = Vec::new();
        let status_code = 200;
        Self {
            qy: GenQuery {
                path: String::new(),
                params: BTreeMap::new(),
                form: BTreeMap::new(),
                cookies: BTreeMap::new(),
                parts: Vec::new(),
                now,
            },
            rp: GenResponse {
                err: String::new(),
                output,
                status_code,
                headers,
            },
            ext: Arc::new(0)
        }
    }
    /// Append string to output.
    fn push_str(&mut self, s: &str) {
        self.rp.output.extend_from_slice(s.as_bytes());
    }
}

impl Transaction for GenTransaction {
    fn arg(&mut self, kind: i64, s: &str) -> Rc<String> {
        let result: &str = match kind {
            0 => &self.qy.path,
            1 => {
                if let Some(s) = self.qy.params.get(s) {
                    s
                } else {
                    ""
                }
            }
            2 => {
                if let Some(s) = self.qy.form.get(s) {
                    s
                } else {
                    ""
                }
            }
            3 => {
                if let Some(s) = self.qy.cookies.get(s) {
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
        self.rp.status_code = code as u16;
    }

    fn header(&mut self, name: &str, value: &str) {
        self.rp.headers.push((name.to_string(), value.to_string()));
    }

    fn global(&self, kind: i64) -> i64 {
        match kind {
            0 => self.qy.now,
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
                Value::Bool(x) => {
                    self.push_str(&x.to_string());
                }
                Value::Float(x) => {
                    self.push_str(&x.to_string());
                }
                Value::RcBinary(x) => {
                    self.rp.output.extend_from_slice(x);
                }
                Value::ArcBinary(x) => {
                    self.rp.output.extend_from_slice(x);
                }
                _ => {
                    panic!()
                }
            }
        }
    }
    fn set_error(&mut self, err: String) {
        self.rp.err = err;
    }
    fn get_error(&mut self) -> String {
        let result = self.rp.err.to_string();
        self.rp.err = String::new();
        result
    }
    fn file_attr(&mut self, k: i64, x: i64) -> Rc<String> {
        let k = k as usize;
        let result: &str = {
            if k >= self.qy.parts.len() {
                ""
            } else {
                let p = &self.qy.parts[k];
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

    fn file_content(&mut self, k: i64) -> Data {
        self.qy.parts[k as usize].data.clone()
    }

    fn get_extension(&self) -> Arc<dyn Any+Send+Sync>
    {
      self.ext.clone()
    }
}

impl Default for GenTransaction {
    fn default() -> Self {
        Self::new()
    }
}
