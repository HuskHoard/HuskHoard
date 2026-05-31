//storage.rs
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::process::{Command, Stdio};
use std::os::unix::net::UnixStream;
use crate::hardware::{AlignedBuffer, send_mtio_cmd, MTWEOF};
use crate::config::ALIGNMENT;

// ---------------------------------------------------------
// 3. The "RawWrite" Archiver Logic (With Multiplexed Replication)
// ---------------------------------------------------------
pub struct MultiTapeWriter<'a> {
    tapes: Vec<&'a mut dyn std::io::Write>, 
    buffer: AlignedBuffer,
    pub cursor: usize,
    pub bytes_written: u64,
}

impl<'a> MultiTapeWriter<'a> {
    pub fn new(tapes: Vec<&'a mut dyn std::io::Write>) -> Self {
        // Buffer 256KB in RAM to force optimal LTO Hardware Block writes
        Self { tapes, buffer: AlignedBuffer::new(262144), cursor: 0, bytes_written: 0 }
    }
    pub fn pad_and_flush(&mut self) -> std::io::Result<()> {
        if self.cursor > 0 {
            // Only pad forward to the nearest 4KB alignment, not the whole 256KB buffer
            let padded_cursor = if self.cursor % ALIGNMENT == 0 {
                self.cursor
            } else {
                self.cursor + ALIGNMENT - (self.cursor % ALIGNMENT)
            };
            
            self.buffer.as_mut_slice()[self.cursor..padded_cursor].fill(0);
            for tape in &mut self.tapes {
                tape.write_all(&self.buffer.as_slice()[..padded_cursor])?;
            }
            self.bytes_written += padded_cursor as u64;
            self.cursor = 0;
        }
        Ok(())
    }
}

impl<'a> std::io::Write for MultiTapeWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut written = 0;
        while written < buf.len() {
            let space = self.buffer.capacity - self.cursor;
            let chunk = std::cmp::min(buf.len() - written, space);
            self.buffer.as_mut_slice()[self.cursor..self.cursor + chunk].copy_from_slice(&buf[written..written + chunk]);
            self.cursor += chunk;
            written += chunk;

            if self.cursor == self.buffer.capacity {
                for tape in &mut self.tapes {
                    tape.write_all(self.buffer.as_slice())?;
                }
                self.bytes_written += self.buffer.capacity as u64;
                self.cursor = 0;
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

// ---------------------------------------------------------
// Rclone Storage Backend Handlers
// ---------------------------------------------------------
pub enum StorageBackend {
    Local(File),
    Tape(File), 
    Rclone { 
        child: std::process::Child,
        stdin: std::process::ChildStdin,
    },
    Grid(UnixStream), 
}

impl std::io::Write for StorageBackend {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            StorageBackend::Local(f) => f.write(buf),
            StorageBackend::Tape(f) => f.write(buf),
            StorageBackend::Rclone { stdin, .. } => stdin.write(buf),
            StorageBackend::Grid(stream) => stream.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            StorageBackend::Local(f) => f.flush(),
            StorageBackend::Tape(f) => f.flush(),
            StorageBackend::Rclone { stdin, .. } => stdin.flush(),
            StorageBackend::Grid(stream) => stream.flush(),
        }
    }
}

impl StorageBackend {
    pub fn seek_to(&mut self, offset: u64) -> std::io::Result<()> {
        match self {
            StorageBackend::Local(f) => { f.seek(SeekFrom::Start(offset))?; Ok(()) },
            StorageBackend::Tape(_) => Ok(()), 
            StorageBackend::Rclone { .. } => Ok(()), 
            StorageBackend::Grid(_) => Ok(()), 
        }
    }
    pub fn close(self) -> std::io::Result<()> {
        match self {
            StorageBackend::Local(f) => f.sync_all(),
            StorageBackend::Tape(f) => {
                f.sync_all()?;
                send_mtio_cmd(f.as_raw_fd(), MTWEOF, 1)?;
                Ok(())
            },
            StorageBackend::Rclone { mut child, stdin } => {
                drop(stdin); 
                let status = child.wait()?; 
                if !status.success() {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "rclone failed"));
                }
                Ok(())
            },
            StorageBackend::Grid(mut stream) => {
                stream.flush()?;
                Ok(())
            }
        }
    }
}

// Spawns: `rclone rcat remote:path` and returns the writable stdin pipe
pub fn spawn_rclone_writer(remote_path: &str) -> std::io::Result<StorageBackend> {
    let mut child = Command::new("rclone")
        .arg("rcat")
        .arg(remote_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit()) 
        .spawn()?;

    let stdin = child.stdin.take().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "Failed to open rclone stdin")
    })?;

    Ok(StorageBackend::Rclone { child, stdin })
}

#[allow(dead_code)]
pub enum StorageReader {
    Local(File),
    Rclone(std::process::ChildStdout, std::process::Child),
    Grid(UnixStream), // NEW: Multi-Node Read Hand-off
}

impl std::io::Read for StorageReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            StorageReader::Local(f) => f.read(buf),
            StorageReader::Rclone(stdout, _) => stdout.read(buf),
            StorageReader::Grid(stream) => stream.read(buf),
        }
    }
}

#[allow(dead_code)]
pub struct ActiveTape {
    pub backend: StorageBackend,
    pub dev_path: String,
    pub uuid_hex: String,
    pub volume_uuid: [u8; 16],
    pub start_offset: u64,
    pub is_append_only: bool, // Unified boolean for Cloud AND Physical Tapes
}
