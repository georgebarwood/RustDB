use crate::{panic, Any, Arc, BTreeMap, Data, Rc, Transaction, Value};

use serde::{Deserialize, Serialize};

/// General Query.
#[derive(Serialize, Deserialize, Debug)]
pub struct GenQuery {
    /// The SQL query string.
    pub sql: Arc<String>,
    /// The path argument for the query.
    pub path: String,
    /// Query parameters.
    pub params: BTreeMap<String, String>,
    /// Query form.
    pub form: BTreeMap<String, String>,
    /// Query cookies.
    pub cookies: BTreeMap<String, String>,
    /// Querey parts ( files ).
    pub parts: Vec<Part>,
    /// Micro-seconds since January 1, 1970 0:00:00 UTC
    pub now: i64,
}

/// General Response.
pub struct GenResponse {
    /// Error string.
    pub err: String,
    /// Response status code.
    pub status_code: u16,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// Reponse body.
    pub output: Vec<u8>,
}

/// Query + Response, implements Transaction.
pub struct GenTransaction {
    /// Transaction Query.
    pub qy: GenQuery,
    /// Transaction Response.
    pub rp: GenResponse,
    /// Transacation extension data.
    pub ext: Box<dyn Any + Send + Sync>,
}

/// Part of multipart data ( uploaded files ).
#[derive(Serialize, Deserialize, Debug)]
pub struct Part {
    /// Part name.
    pub name: String,
    /// Part filename.
    pub file_name: String,
    /// Part contenttype.
    pub content_type: String,
    ///
    pub text: String,
    ///
    pub data: Data,
}

impl GenTransaction {
    ///
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
                sql: Arc::new("EXEC web.Main()".to_string()),
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
            ext: Box::new(()),
        }
    }

    /// Append string to output.
    fn push_str(&mut self, s: &str) {
        self.rp.output.extend_from_slice(s.as_bytes());
    }
}

impl Transaction for GenTransaction {
    fn arg(&mut self, kind: i64, s: &str) -> Rc<String> {
        let s = match kind {
            0 => Some(&self.qy.path),
            1 => self.qy.params.get(s),
            2 => self.qy.form.get(s),
            3 => self.qy.cookies.get(s),
            _ => None,
        };
        let s = if let Some(s) = s { s } else { "" };
        Rc::new(s.to_string())
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
                Value::RcBinary(x) => {
                    self.rp.output.extend_from_slice(x);
                }
                Value::ArcBinary(x) => {
                    self.rp.output.extend_from_slice(x);
                }
                _ => {
                    self.push_str(&v.str());
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

    fn set_extension(&mut self, ext: Box<dyn Any + Send + Sync>) {
        self.ext = ext;
    }

    fn get_extension(&mut self) -> Box<dyn Any + Send + Sync> {
        std::mem::replace(&mut self.ext, Box::new(()))
    }
}

impl Default for GenTransaction {
    fn default() -> Self {
        Self::new()
    }
}
