use crate::*;

/// Wrap a type in Rc + RefCell.
pub fn new<T>(x: T) -> std::rc::Rc<std::cell::RefCell<T>>
{
  Rc::new(RefCell::new(x))
}

/// Construct a new map wrapped in a RefCell.
pub fn newmap<K, T>() -> RefCell<HashMap<K, T>>
{
  RefCell::new(HashMap::new())
}

/// Extract u64 from byte data.
pub fn getu64(data: &[u8], off: usize) -> u64
{
  let data = &data[off..off + 8];
  u64::from_le_bytes(data.try_into().unwrap())
}

/// Store u64 to byte data.
pub fn setu64(data: &mut [u8], val: u64)
{
  data[0..8].copy_from_slice(&val.to_le_bytes());
}

/// Extract f64 from byte data.
pub fn getf64(data: &[u8], off: usize) -> f64
{
  let data = &data[off..off + 8];
  f64::from_le_bytes(data.try_into().unwrap())
}

/// Extract f32 from byte data.
pub fn getf32(data: &[u8], off: usize) -> f32
{
  let data = &data[off..off + 4];
  f32::from_le_bytes(data.try_into().unwrap())
}

/// Extract unsigned value of n bytes from data.
pub fn get(data: &[u8], off: usize, n: usize) -> u64
{
  let mut x: u64 = 0;
  for i in 0..n
  {
    x = (x << 8) + data[off + n - i - 1] as u64;
  }
  x
}

/// Extract signed value of n bytes from data.
pub fn iget(data: &[u8], off: usize, n: usize) -> i64
{
  let mut x: u64 = 0;
  for i in 0..n
  {
    x = (x << 8) + data[off + n - i - 1] as u64;
  }
  if n < 8
  {
    let sign_bit: u64 = 1_u64 << (n * 8 - 1);
    if (sign_bit & x) != 0
    {
      x += 0xffff_ffff_ffff_ffff << (n * 8);
    }
  }
  x as i64
}

/// Store unsigned value of n bytes to data.
pub fn set(data: &mut [u8], off: usize, val: u64, n: usize)
{
  /*
    for i in 0..n
    {
      data[ off + i ] = ( val & 255 ) as u8;
      val >>= 8;
    }
  */
  let bytes = val.to_le_bytes();
  data[off..off + n].copy_from_slice(&bytes[0..n]);
}

// Bitfield  macros

/// The mask to extract $len bits at bit offset $off.
macro_rules! bitmask {
  ( $off: expr, $len: expr ) => {
    ((1 << $len) - 1) << $off
  };
}

/// Extract $len bits from $val at bit offset $off.
macro_rules! getbits {
  ( $val: expr, $off: expr, $len: expr ) => {
    ($val & bitmask!($off, $len)) >> $off
  };
}

/// Update $len bits in $var at bit offset $off to $val.
macro_rules! setbits {
  ( $var: expr, $off: expr, $len: expr, $val: expr ) => {
    $var = ($var & !bitmask!($off, $len)) | (($val << $off) & bitmask!($off, $len))
  };
}

/// Convert a hex char byte to a byte in range 0..15.
pub fn hex(c: u8) -> u8 //
{
  match c
  {
    | b'0'..=b'9' => c - b'0',
    | b'A'..=b'F' => c + 10 - b'A',
    | b'a'..=b'f' => c + 10 - b'a',
    | _ =>
    {
      panic!()
    }
  }
}

/// Convert hex literal to bytes.
pub fn parse_hex(s: &[u8]) -> Vec<u8>
{
  let n = s.len() / 2;
  let mut result = Vec::<u8>::with_capacity(n);
  for i in 0..n
  {
    result.push(hex(s[i]) * 16 + hex(s[i + 1]));
  }
  result
}
