use crate::*;
/// Simple value ( Binary, String, Int, Float, Bool ).
///
/// Binary and String values are allocated 16 bytes.
/// If the value is more than 15 bytes, the first 7 bytes are stored inline, and the rest are coded.

#[derive(Clone)]
pub enum Value {
    None,
    Binary(Rc<Vec<u8>>),
    String(Rc<String>),
    Int(i64),
    Float(f64),
    Bool(bool),
    For(Rc<RefCell<run::ForState>>),
    ForSort(Rc<RefCell<run::ForSortState>>),
}
impl Value {
    /// Get the default Value for a DataType.
    pub fn default(t: DataType) -> Value {
        match data_kind(t) {
            DataKind::Bool => Value::Bool(false),
            DataKind::Float => Value::Float(0.0),
            DataKind::String => Value::String(Rc::new(String::new())),
            DataKind::Binary => Value::Binary(Rc::new(Vec::new())),
            _ => Value::Int(0),
        }
    }
    /// Get a Value from byte data.
    pub fn load(db: &DB, typ: DataType, data: &[u8], off: usize) -> (Value, u64) {
        let mut code = u64::MAX;
        let val = match data_kind(typ) {
            DataKind::Bool => Value::Bool(data[off] != 0),
            DataKind::String => {
                let (bytes, u) = get_bytes(db, &data[off..]);
                code = u;
                let str = String::from_utf8(bytes).unwrap();
                Value::String(Rc::new(str))
            }
            DataKind::Binary => {
                let (bytes, u) = get_bytes(db, &data[off..]);
                code = u;
                Value::Binary(Rc::new(bytes))
            }
            _ => {
                let size = data_size(typ);
                Value::Int(util::iget(data, off, size) as i64)
            }
        };
        (val, code)
    }
    /// Save a Value to byte data.
    pub fn save(&self, typ: DataType, data: &mut [u8], off: usize, code: u64) {
        let size = data_size(typ);
        match self {
            Value::Bool(x) => {
                data[off] = if *x { 1 } else { 0 };
            }
            Value::Int(x) => util::set(data, off, *x as u64, size),
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
                save_bytes(s.as_bytes(), &mut data[off..], code);
            }
            Value::Binary(b) => {
                save_bytes(b, &mut data[off..], code);
            }
            _ => {}
        }
    }
    /// Convert a Value to a String.
    pub fn str(&self) -> Rc<String> {
        match self {
            Value::String(s) => s.clone(),
            Value::Int(x) => Rc::new(x.to_string()),
            Value::Float(x) => Rc::new(x.to_string()),
            Value::Binary(x) => Rc::new(util::to_hex(x)),
            _ => panic!("str not implemented"),
        }
    }
    /// Append a String.
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
pub fn get_bytes(db: &DB, data: &[u8]) -> (Vec<u8>, u64) {
    let n = data[0] as usize;
    if n <= 15 {
        let mut bytes = vec![0_u8; n];
        bytes[0..n].copy_from_slice(&data[1..=n]);
        (bytes, u64::MAX)
    } else {
        let code = util::getu64(data, 8);
        let mut bytes = db.decode(code);
        bytes[0..7].copy_from_slice(&data[1..8]);
        (bytes, code)
    }
}
/// Save bytes. If more than 15 bytes, a code is needed.
pub fn save_bytes(bytes: &[u8], data: &mut [u8], code: u64) {
    let n = bytes.len();
    if n <= 15 {
        data[0] = n as u8;
        data[1..=n].copy_from_slice(&bytes[0..n]);
    } else {
        // Store first 7 bytes and code.
        data[0] = 255;
        data[1..8].copy_from_slice(&bytes[0..7]);
        util::setu64(&mut data[8..], code);
    }
}
