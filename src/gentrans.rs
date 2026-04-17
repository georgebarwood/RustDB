use crate::{Any, Arc, GBTreeMap, GString, GVec, LRc, LString, Transaction, Value, panic};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// General Query.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub struct GenQuery {
    /// The SQL query string.
    pub sql: Arc<String>,
    /// The path argument for the query.
    pub path: GString,
    /// Query parameters.
    pub params: GBTreeMap<GString, GString>,
    /// Query form.
    pub form: GBTreeMap<GString, GString>,
    /// Query cookies.
    pub cookies: GBTreeMap<GString, GString>,
    /// Query parts ( files ).
    pub parts: GVec<Part>,
    /// Micro-seconds since January 1, 1970 0:00:00 UTC
    pub now: i64,
}

/// General Response.
#[non_exhaustive]
pub struct GenResponse {
    /// Error string.
    pub err: GString,
    /// Response status code.
    pub status_code: u16,
    /// Response headers.
    pub headers: GVec<(GString, GString)>,
    /// Reponse body.
    pub output: Vec<u8>,
}

/// Query + Response, implements Transaction.
#[non_exhaustive]
pub struct GenTransaction {
    /// Transaction Query.
    pub qy: GenQuery,
    /// Transaction Response.
    pub rp: GenResponse,
    /// Transaction extension data.
    pub ext: Box<dyn Any + Send + Sync>,
}

/// Part of multipart data ( uploaded files ).
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Default)]
#[non_exhaustive]
pub struct Part {
    /// Part name.
    pub name: GString,
    /// Part filename.
    pub file_name: GString,
    /// Part contenttype.
    pub content_type: GString,
    /// Text.
    pub text: GString,
    /// Data.
    pub data: Arc<GVec<u8>>,
}

impl GenTransaction {
    /// Construct.
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        let now = now.as_micros() as i64;
        let output = Vec::with_capacity(64 * 1024);
        let headers = GVec::new();
        Self {
            qy: GenQuery {
                sql: Arc::new("EXEC web.Main()".to_string()),
                path: GString::new(),
                params: GBTreeMap::new(),
                form: GBTreeMap::new(),
                cookies: GBTreeMap::new(),
                parts: GVec::new(),
                now,
            },
            rp: GenResponse {
                err: GString::new(),
                output,
                status_code: 200,
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
    fn arg(&mut self, kind: i64, s: &str) -> LRc<LString> {
        let s: Option<&str> = match kind {
            0 => Some(&self.qy.path),
            1 => self.qy.params.get(s).as_ref().map(|x| x.as_str()),
            2 => self.qy.form.get(s).as_ref().map(|x| x.as_str()),
            3 => self.qy.cookies.get(s).as_ref().map(|x| x.as_str()),
            _ => None,
        };
        let s = s.unwrap_or_default();
        LRc::new(LString::from_str(s))
    }

    fn status_code(&mut self, code: i64) {
        self.rp.status_code = code as u16;
    }

    fn header(&mut self, name: &str, value: &str) {
        self.rp
            .headers
            .push((GString::from_str(name), GString::from_str(value)));
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

    fn set_error(&mut self, err: &str) {
        self.rp.err = GString::from_str(err);
    }

    fn get_error(&mut self) -> String {
        let result = self.rp.err.to_string();
        self.rp.err = GString::new();
        result
    }

    fn file_attr(&mut self, k: i64, x: i64) -> LRc<LString> {
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
        LRc::new(LString::from_str(result))
    }

    fn file_content(&mut self, k: i64) -> Arc<GVec<u8>> {
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
