// src/writer_envelope.rs
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::sync::atomic::{Ordering, compiler_fence};

pub const HEADER_SIZE: usize = 4096;

/// Envelope binary layout (all little-endian):
/// u16 rtype
/// u16 flags
/// u64 ts_ms
/// u32 src_id
/// u32 inst_id
/// u32 len
/// u32 pad
/// Total stored bytes: ENVELOPE_RAW_SIZE (32)
pub const ENVELOPE_RAW_SIZE: usize = 32;

/// Small set of record types used in the writer/reader.
/// Add more as needed. Keep numeric values stable across readers/writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RType {
    BookSnapshot = 1,
    TradePrint = 2,
    CombinedTick = 3,
    GlobalOrderBook = 4,
    GlobalOrderBookJson = 5, // human-readable JSON copy
}

impl From<u16> for RType {
    fn from(v: u16) -> Self {
        match v {
            1 => RType::BookSnapshot,
            2 => RType::TradePrint,
            3 => RType::CombinedTick,
            4 => RType::GlobalOrderBook,
            5 => RType::GlobalOrderBookJson,
            // Unknown/unsupported rtypes default to BookSnapshot to keep callers simple.
            // If you prefer explicit error handling, replace with TryFrom in the call sites.
            _ => RType::BookSnapshot,
        }
    }
}

/// Thin envelope struct representing the on-disk header for each record.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub rtype: u16,
    pub flags: u16,
    pub ts_ms: u64,
    pub src_id: u32,
    pub inst_id: u32,
    pub len: u32,
    pub pad: u32,
}

impl Envelope {
    pub fn zeroed() -> Self {
        Self {
            rtype: 0,
            flags: 0,
            ts_ms: 0,
            src_id: 0,
            inst_id: 0,
            len: 0,
            pad: 0,
        }
    }

    /// Serialize into a little-endian fixed-size array (ENVELOPE_RAW_SIZE).
    pub fn to_bytes_le(&self) -> [u8; ENVELOPE_RAW_SIZE] {
        let mut buf = [0u8; ENVELOPE_RAW_SIZE];
        {
            let mut w = Cursor::new(&mut buf[..]);
            w.write_u16::<LittleEndian>(self.rtype).unwrap();
            w.write_u16::<LittleEndian>(self.flags).unwrap();
            w.write_u64::<LittleEndian>(self.ts_ms).unwrap();
            w.write_u32::<LittleEndian>(self.src_id).unwrap();
            w.write_u32::<LittleEndian>(self.inst_id).unwrap();
            w.write_u32::<LittleEndian>(self.len).unwrap();
            w.write_u32::<LittleEndian>(self.pad).unwrap();
            // remaining bytes are zeroed
        }
        buf
    }

    /// Read an Envelope from a little-endian byte slice (must be at least ENVELOPE_RAW_SIZE).
    pub fn from_bytes_le(b: &[u8]) -> Self {
        let mut r = Cursor::new(b);
        let rtype = r.read_u16::<LittleEndian>().unwrap_or(0);
        let flags = r.read_u16::<LittleEndian>().unwrap_or(0);
        let ts_ms = r.read_u64::<LittleEndian>().unwrap_or(0);
        let src_id = r.read_u32::<LittleEndian>().unwrap_or(0);
        let inst_id = r.read_u32::<LittleEndian>().unwrap_or(0);
        let len = r.read_u32::<LittleEndian>().unwrap_or(0);
        let pad = r.read_u32::<LittleEndian>().unwrap_or(0);
        Self {
            rtype,
            flags,
            ts_ms,
            src_id,
            inst_id,
            len,
            pad,
        }
    }

    /// Atomically set the VALID flag (u16 = 1) in the mmap region for the envelope at `env_off`.
    ///
    /// SAFETY:
    /// - The caller must ensure `mmap` is the slice that backs the memory-mapped file,
    ///   and that `env_off + 2` is a valid writable offset.
    /// - We use `compiler_fence` to order writes relative to prior writes the caller made
    ///   (e.g. payload and other header fields) so readers see consistent state.
    pub fn set_valid_flag_in_mmap(mmap: &mut [u8], env_off: usize) {
        let flag_off = env_off + 2;
        // Ensure prior writes to payload/header are ordered before setting the valid flag.
        compiler_fence(Ordering::SeqCst);
        unsafe {
            // write little-endian u16=1 unaligned
            let p = mmap.as_mut_ptr().add(flag_off) as *mut u16;
            // write_unaligned expects the native-endian value; convert to LE so on LE machines it's correct.
            p.write_unaligned(1u16.to_le());
        }
        compiler_fence(Ordering::SeqCst);
    }
}
