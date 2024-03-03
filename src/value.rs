use crate::*;

#[derive(Clone, Copy)]
/// Code for variable length values.
pub struct Code {
    /// ByteStorage Id.
    pub id: u64,
    /// Fragment type.
    pub ft: usize,
}

#[derive(Clone)]
/// Simple value ( Binary, String, Int, Float, Bool ).
///
/// When stored in a database record, binary(n) and string(n) values are allocated (n+1) bytes (8<=n<=249).
/// If the value is more than n bytes, the first (n-8) bytes are stored inline, and the rest are coded.
#[non_exhaustive]
pub enum Value {
    ///
    None,
    ///
    RcBinary(Rc<Vec<u8>>),
    ///
    ArcBinary(Arc<Vec<u8>>),
    ///
    String(Rc<String>),
    ///
    Int(i64),
    ///
    Float(f64),
    ///
    Bool(bool),
    ///
    For(Rc<RefCell<run::ForState>>),
    ///
    ForSort(Rc<RefCell<run::ForSortState>>),
}

impl Value {
    /// Get the default Value for a DataType.
    pub fn default(t: DataType) -> Value {
        match data_kind(t) {
            DataKind::Bool => Value::Bool(false),
            DataKind::Float => Value::Float(0.0),
            DataKind::String => Value::String(Rc::new(String::new())),
            DataKind::Binary => Value::RcBinary(Rc::new(Vec::new())),
            _ => Value::Int(0),
        }
    }

    /// Get a Value from byte data.
    pub fn load(db: &DB, typ: DataType, data: &[u8], off: usize) -> (Value, Code) {
        let mut code = Code {
            id: u64::MAX,
            ft: 0,
        };
        let size = data_size(typ);
        let val = match data_kind(typ) {
            DataKind::Binary => {
                let (bytes, u) = get_bytes(db, &data[off..], size);
                code = u;
                Value::RcBinary(Rc::new(bytes))
            }
            DataKind::String => {
                let (bytes, u) = get_bytes(db, &data[off..], size);
                code = u;
                let str = String::from_utf8(bytes).unwrap();
                Value::String(Rc::new(str))
            }
            DataKind::Bool => Value::Bool(data[off] != 0),
            DataKind::Float => {
                let f = if size == 4 {
                    util::getf32(data, off) as f64
                } else {
                    util::getf64(data, off)
                };
                Value::Float(f)
            }
            _ => Value::Int(util::iget(data, off, size) as i64),
        };
        (val, code)
    }

    /// Save a Value to byte data.
    pub fn save(&self, typ: DataType, data: &mut [u8], off: usize, code: Code) {
        let size = data_size(typ);
        match self {
            Value::Bool(x) => {
                data[off] = if *x { 1 } else { 0 };
            }
            Value::Int(x) => util::iset(data, off, *x, size),
            Value::Float(x) => {
                if size == 8 {
                    let bytes = (*x).to_le_bytes();
                    data[off..off + 8].copy_from_slice(&bytes);
                } else {
                    debug_assert!(size == 4);
                    let val = *x as f32;
                    let bytes = val.to_le_bytes();
                    data[off..off + 4].copy_from_slice(&bytes);
                }
            }
            Value::String(s) => {
                save_bytes(s.as_bytes(), &mut data[off..], code, size);
            }
            Value::RcBinary(b) => {
                save_bytes(b, &mut data[off..], code, size);
            }
            Value::ArcBinary(b) => {
                save_bytes(b, &mut data[off..], code, size);
            }
            _ => {}
        }
    }

    /// Convert a Value to a String.
    pub fn str(&self) -> Rc<String> {
        match self {
            Value::String(s) => s.clone(),
            Value::Int(x) => Rc::new(x.to_string()),
            Value::Bool(x) => Rc::new(x.to_string()),
            Value::Float(x) => Rc::new(x.to_string()),
            Value::RcBinary(x) => Rc::new(util::to_hex(x)),
            Value::ArcBinary(x) => Rc::new(util::to_hex(x)),
            _ => panic!("str not implemented"),
        }
    }

    /// Get integer value.
    pub fn int(&self) -> i64 {
        match self {
            Value::Int(x) => *x,
            _ => panic!(),
        }
    }

    /// Get float value.
    pub fn float(&self) -> f64 {
        match self {
            Value::Float(x) => *x,
            _ => panic!(),
        }
    }

    /// Append to a String.
    pub fn append(&mut self, val: &Value) {
        if let Value::String(s) = self {
            let val = val.str();
            if let Some(ms) = Rc::get_mut(s) {
                ms.push_str(&val);
            } else {
                let mut ns = String::with_capacity(s.len() + val.len());
                ns.push_str(s);
                ns.push_str(&val);
                *self = Value::String(Rc::new(ns));
            }
        } else {
            panic!()
        }
    }

    /// Inc an integer or float.
    pub fn inc(&mut self, val: &Value) {
        match self {
            Value::Int(x) => *x += val.int(),
            Value::Float(x) => *x += val.float(),
            _ => panic!(),
        }
    }

    /// Dec an integer or float.
    pub fn dec(&mut self, val: &Value) {
        match self {
            Value::Int(x) => *x -= val.int(),
            Value::Float(x) => *x -= val.float(),
            _ => panic!(),
        }
    }

    /// Convert a Value to a Binary.
    pub fn bin(&self) -> Rc<Vec<u8>> {
        match self {
            Value::ArcBinary(x) => Rc::new(x.to_vec()),
            Value::RcBinary(x) => x.clone(),
            Value::String(s) => Rc::new(s.as_bytes().to_vec()),
            Value::Float(x) => Rc::new(x.to_le_bytes().to_vec()),
            Value::Int(x) => Rc::new(x.to_le_bytes().to_vec()),
            _ => panic!("bin not implemented"),
        }
    }

    /// Borrow address of Binary value.
    pub fn bina(&self) -> &[u8]
    {
       match self {
            Value::RcBinary(data) => data,
            Value::ArcBinary(data) => data,
            _ => panic!(),
       }
    }
}

/// Value comparison.
impl std::cmp::Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            Value::String(s1) => {
                if let Value::String(s2) = other {
                    return s1.cmp(s2);
                }
            }
            Value::Int(x1) => {
                if let Value::Int(x2) = other {
                    return x1.cmp(x2);
                }
            }
            Value::Float(x1) => {
                if let Value::Float(x2) = other {
                    return x1.partial_cmp(x2).unwrap();
                }
            }
            Value::RcBinary(b1) => {
                if let Value::RcBinary(b2) = other {
                    return b1.cmp(b2);
                }
            }
            _ => {}
        }
        panic!()
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        if let Some(eq) = self.partial_cmp(other) {
            eq == Ordering::Equal
        } else {
            false
        }
    }
}

impl Eq for Value {}

/// Decode bytes. Result is bytes and code ( or u64::MAX if no code ).
pub fn get_bytes(db: &DB, data: &[u8], size: usize) -> (Vec<u8>, Code) {
    let n = data[0] as usize;
    if n < size {
        let mut bytes = vec![0_u8; n];
        bytes[0..n].copy_from_slice(&data[1..=n]);
        (
            bytes,
            Code {
                id: u64::MAX,
                ft: 0,
            },
        )
    } else {
        let id = util::getu64(data, size - 8);
        let ft = 255 - n;
        let code = Code { id, ft };
        let mut bytes = db.decode(code, size - 9);
        bytes[0..size - 9].copy_from_slice(&data[1..size - 8]);
        (bytes, code)
    }
}

/// Save bytes.
pub fn save_bytes(bytes: &[u8], data: &mut [u8], code: Code, size: usize) {
    let n = bytes.len();
    if n < size {
        data[0] = n as u8;
        data[1..=n].copy_from_slice(&bytes[0..n]);
    } else {
        // Store first (size-9) bytes and code.
        data[0] = 255 - code.ft as u8;
        data[1..size - 8].copy_from_slice(&bytes[0..size - 9]);
        util::setu64(&mut data[size - 8..], code.id);
    }
}
