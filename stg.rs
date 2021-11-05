pub trait Storage
{
  fn read(&mut self, off: u64, bytes: &mut [u8]);
  fn write(&mut self, off: u64, bytes: &[u8]);
  fn truncate(&mut self, off: u64);
  fn size(&mut self) -> u64;
}

use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

pub struct SimpleFileStorage
{
  file: fs::File,
}

impl SimpleFileStorage
{
  pub fn new(filename: &str) -> Self
  {
    Self { file: OpenOptions::new().read(true).write(true).create(true).open(filename).unwrap() }
  }
}

impl Storage for SimpleFileStorage
{
  fn read(&mut self, off: u64, bytes: &mut [u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.read_exact(bytes);
  }

  fn write(&mut self, off: u64, bytes: &[u8])
  {
    self.file.seek(SeekFrom::Start(off)).unwrap();
    let _x = self.file.write(bytes);
  }

  fn size(&mut self) -> u64
  {
    self.file.seek(SeekFrom::End(0)).unwrap()
  }

  fn truncate(&mut self, off: u64)
  {
    self.file.set_len(off).unwrap();
  }
}
