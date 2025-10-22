// src/mmap_writer.rs
use anyhow::{Result, anyhow};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::interval;

use crate::config::MmapCfg;
use crate::writer_envelope::{ENVELOPE_RAW_SIZE, Envelope, HEADER_SIZE};

#[derive(Debug)]
pub struct WriterMsg {
    pub env: Envelope,
    pub payload: Vec<u8>,
}

pub struct GlobalWriter {
    cfg: MmapCfg,
    file_len: usize,
    file: std::fs::File,
    mmap: MmapMut,
    write_offset: Arc<Mutex<usize>>,
    header_size: usize,
    bytes_since_flush: usize,
}

impl GlobalWriter {
    pub async fn new(cfg: &MmapCfg, _catalog: crate::config::Catalog) -> Result<Self> {
        if cfg.mode.to_ascii_lowercase() != "ring" {
            return Err(anyhow!("only 'ring' supported"));
        }
        if cfg.format.to_ascii_lowercase() != "bincode" {
            return Err(anyhow!("only 'bincode' supported"));
        }

        let path = Path::new(&cfg.path);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        // cfg.bytes is expected to be u64 in config; set_len takes u64
        file.set_len(cfg.bytes)?;
        file.seek(SeekFrom::Start(0))?;

        // Write zeroed header (HEADER_SIZE)
        let header_size = HEADER_SIZE as usize;
        file.write_all(&vec![0u8; header_size])?;
        file.sync_all()?;

        let file_len = cfg.bytes as usize;
        let mmap = unsafe { MmapOptions::new().len(file_len).map_mut(&file)? };

        Ok(Self {
            cfg: cfg.clone(),
            file_len,
            file,
            mmap,
            write_offset: Arc::new(Mutex::new(header_size)),
            header_size,
            bytes_since_flush: 0,
        })
    }

    pub async fn run(&mut self, mut rx: Receiver<WriterMsg>) -> Result<()> {
        let mut tick = interval(Duration::from_millis(self.cfg.flush.interval_ms));
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    self.append(msg)?;
                    // cfg.flush.bytes might be u64 in config — cast to usize for comparison
                    if self.bytes_since_flush >= (self.cfg.flush.bytes as usize) {
                        self.flush()?;
                    }
                }
                _ = tick.tick() => {
                    self.flush()?;
                }
            }
        }
    }

    fn append(&mut self, msg: WriterMsg) -> Result<()> {
        // Build envelope bytes with flags=0 initially.
        let mut env = msg.env;
        env.flags = 0u16;
        env.len = msg.payload.len() as u32;
        let env_bytes = env.to_bytes_le();

        let payload_len = msg.payload.len();
        let total_len = ENVELOPE_RAW_SIZE + payload_len;
        if total_len > self.file_len {
            return Err(anyhow!("record too large for mmap"));
        }

        // Acquire offset
        let mut off = *self.write_offset.lock();

        // trace before wrap so we can see where we started writing from
        tracing::trace!(target: "mmap_writer", "append start: off={} total_len={} header_size={} file_len={} seq={}", off, total_len, self.header_size, self.file_len, env.pad);

        // wrap if not enough space until end-of-file
        if off + total_len > self.file_len {
            tracing::trace!(target: "mmap_writer", "append wrapping: off={} -> header_size={}", off, self.header_size);
            off = self.header_size;
        }

        // bounds check
        if off + total_len > self.file_len {
            return Err(anyhow!("not enough contiguous space even after wrap"));
        }

        // write envelope bytes (flags=0)
        {
            let env_slice = &mut self.mmap[off..off + ENVELOPE_RAW_SIZE];
            env_slice.copy_from_slice(&env_bytes);
        }

        // write payload
        let payload_off = off + ENVELOPE_RAW_SIZE;
        {
            let payload_slice = &mut self.mmap[payload_off..payload_off + payload_len];
            payload_slice.copy_from_slice(&msg.payload);
        }

        // Memory fence + set VALID flag
        // Use helper to set the flag in mmap at offset `off` (flag at off + 2)
        Envelope::set_valid_flag_in_mmap(&mut self.mmap[..], off);

        // update offset
        let new_off = off + total_len;
        *self.write_offset.lock() = new_off;
        self.bytes_since_flush += total_len;

        tracing::trace!(target: "mmap_writer", "append wrote: seq={} off={} total_len={} new_off={}", env.pad, off, total_len, new_off);

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.mmap.flush_async()?;
        tracing::trace!(target: "mmap_writer", "flush: write_offset={} bytes_since_flush={}", *self.write_offset.lock(), self.bytes_since_flush);
        self.bytes_since_flush = 0;
        Ok(())
    }
}
