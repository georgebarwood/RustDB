use crate::stg::Storage;

use windows::{
    core::Handle,
    Win32::Foundation::{CloseHandle, HANDLE},
    Win32::Storage::FileSystem::{
        CreateFileA, // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea
        GetFileSizeEx, // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfilesizeex
        ReadFile, // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-readfile
        SetEndOfFile,
        SetFilePointerEx,
        WriteFile, // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-writefile
        FILE_BEGIN,
        FILE_FLAG_OVERLAPPED,
        FILE_GENERIC_READ,
        FILE_GENERIC_WRITE,
        FILE_SHARE_READ,
        OPEN_ALWAYS,
    },
    Win32::System::Threading::{
        CreateEventA, // See https://docs.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-createeventa
        WaitForSingleObject, // https://docs.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitforsingleobject
        WAIT_OBJECT_0,
    },
    Win32::System::IO::{/*GetOverlappedResult,*/ OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0},
};

// See also https://docs.microsoft.com/en-us/windows/win32/fileio/synchronous-and-asynchronous-i-o

pub struct WinEvent {
    event: HANDLE,
}
impl Drop for WinEvent {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.event);
        }
    }
}

pub struct WinFileStorage {
    pub file: HANDLE,
}

impl WinFileStorage {
    pub fn new(filename: &str) -> Self {
        unsafe {
            let file = CreateFileA(
                filename,
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_SHARE_READ,
                std::ptr::null_mut(),
                OPEN_ALWAYS,
                FILE_FLAG_OVERLAPPED,
                None,
            );

            if let Err(err) = file.ok() 
            {
               panic!("{} Filename= {}", err, filename);
            }
            Self { file }
        }
    }

    pub fn start_read(&self, off: u64, buffer: &mut [u8]) -> WinEvent {
        unsafe {
            let event: HANDLE = CreateEventA(std::ptr::null_mut(), true, false, None);

            let mut overlapped = OVERLAPPED {
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: off as u32,
                        OffsetHigh: (off >> 32) as u32,
                    },
                },
                hEvent: event,
                Internal: 0,
                InternalHigh: 0,
            };

            let blen = buffer.len();

            let _ok = ReadFile(
                self.file,
                buffer.as_mut_ptr() as _,
                blen as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            );

            WinEvent { event }
        }
    }

    pub fn start_write(&mut self, off: u64, buffer: &[u8]) -> WinEvent {
        unsafe {
            let event: HANDLE = CreateEventA(std::ptr::null_mut(), true, false, None);

            let mut overlapped = OVERLAPPED {
                Anonymous: OVERLAPPED_0 {
                    Anonymous: OVERLAPPED_0_0 {
                        Offset: off as u32,
                        OffsetHigh: (off >> 32) as u32,
                    },
                },
                hEvent: event,
                Internal: 0,
                InternalHigh: 0,
            };

            let blen = buffer.len();

            let _ok = WriteFile(
                self.file,
                buffer.as_ptr() as _,
                blen as u32,
                std::ptr::null_mut(),
                &mut overlapped,
            );

            WinEvent { event }
        }
    }

    pub fn wait(&self, x: WinEvent) {
        unsafe {
            let wait_ok = WaitForSingleObject(x.event, u32::MAX);
            debug_assert!(wait_ok == WAIT_OBJECT_0);
        }
    }

    pub fn truncate(&mut self, size: u64) {
        unsafe {
            let mut pos = 0;
            SetFilePointerEx(self.file, size as i64, &mut pos, FILE_BEGIN);
            SetEndOfFile(self.file);
        }
    }
}

impl Storage for WinFileStorage {
    fn size(&self) -> u64 {
        unsafe {
            let mut result: i64 = 0;
            GetFileSizeEx(self.file, &mut result);
            result as u64
        }
    }

    fn read(&self, off: u64, buffer: &mut [u8]) {
        let e = self.start_read(off, buffer);
        self.wait(e);
    }

    fn write(&mut self, off: u64, buffer: &[u8]) {
        let e = self.start_write(off, buffer);
        self.wait(e);
    }

    fn commit(&mut self, size: u64) {
        self.truncate(size);
    }

    /// Read multiple ranges. List is (file offset, data offset, data size).
    fn read_multiple(&self, list: &[(u64, usize, usize)], data: &mut [u8]) {
        let mut events = Vec::new();
        for (addr, off, size) in list {
            let data = &mut data[*off..off + *size];
            events.push(self.start_read(*addr, data));
        }
        for e in events {
            self.wait(e);
        }
    }
}

impl Drop for WinFileStorage {
    fn drop(&mut self) {
        unsafe {
            let closed_ok = CloseHandle(self.file);
            assert!(closed_ok.as_bool());
        }
    }
}
