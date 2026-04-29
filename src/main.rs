use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, BufReader, BufRead};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, MetadataExt};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::Path;
use log::{info, error}; 
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::thread;

use std::ffi::CString;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::process::{Command, Stdio};
use blake3::Hasher;
use uuid::Uuid;
use bytemuck::{Pod, Zeroable};
use crc32fast::Hasher as Crc32Hasher;
use rusqlite::{params, Connection, Result as SqlResult};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::sync::mpsc;
use serde_json::json;
use std::os::unix::net::UnixStream;

const ALIGNMENT: usize = 4096;

fn default_no_compress() -> Vec<String> {
    vec![
        "mp4".into(), "mkv".into(), "avi".into(), "mov".into(), "zip".into(),
        "tar".into(), "gz".into(), "rar".into(), "7z".into(), "jpg".into(),
        "png".into(), "iso".into()
    ]
}

#[derive(Deserialize, Clone, Debug)]
pub struct HuskConfig {
    pub hot_tier: String,
    pub db_path: String,
    pub primary_volumes: Vec<String>,
    pub failover_volumes: Vec<String>,
    pub replication_volumes: Vec<String>,
    pub replicas: usize,
    pub janitor_schedule_time: Option<String>,
    pub janitor_interval_secs: u64,           
    pub max_age_days: u64,
    pub max_versions: u32,
    pub exclude_dirs: Vec<String>,
    pub temp_extensions: Vec<String>,
    pub immediate_archive_extensions: Vec<String>,
    pub immediate_archive_dirs: Vec<String>,
    #[serde(default = "default_no_compress")]
    pub no_compress_extensions: Vec<String>, 
    pub log_level: String,
    pub http_port: Option<u16>,
    pub sidecar_socket_path: Option<String>, 
}

// ---------------------------------------------------------
// NEW: Enterprise Sidecar IPC Bridge
// ---------------------------------------------------------
pub struct SidecarBridge {
    socket_path: Option<String>,
}

impl SidecarBridge {
    pub fn new(config: &Arc<HuskConfig>) -> Self {
        Self { socket_path: config.sidecar_socket_path.clone() }
    }

    pub fn send_event(&self, payload: serde_json::Value) {
        if let Some(ref path) = self.socket_path {
            if let Ok(mut stream) = UnixStream::connect(path) {
                let _ = stream.write_all(payload.to_string().as_bytes());
                let _ = stream.write_all(b"\n");
            }
        }
    }

    pub fn wake_volume(&self, tape_uuid: &str, device_path: &str, location_hint: &str) -> std::io::Result<()> {
        let Some(ref path) = self.socket_path else { return Ok(()); };
        let mut stream = UnixStream::connect(path)?;
        let _ = stream.set_read_timeout(Some(Duration::from_secs(60))); 
        
        let msg = json!({
            "action": "WAKE_VOLUME",
            "tape_uuid": tape_uuid,
            "device_path": device_path,
            "location_hint": location_hint
        });
        
        stream.write_all(msg.to_string().as_bytes())?;
        stream.write_all(b"\n")?;
        
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf)?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() == "READY" {
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Sidecar hardware timeout or failure"))
        }
    }
}

const DEFAULT_TOML: &str = r#"# ==========================================
# Husk - Hybrid User-Space Storage Kernel
# ==========================================

# --- Core Paths ---
hot_tier = "hot_tier"
db_path = "husk_catalog.db"
log_level = "info" # Options: debug, info, warn, error
http_port = 8080   # HTTP Streaming Gateway Port

# --- Volume Tiering ---
primary_volumes = ["master_archive_tape.img"]
failover_volumes = ["failover_tape.img"]
replication_volumes = ["rclone:s3:my-bucket"]
replicas = 1

# --- Policy Engine (The Janitor) ---
# Set to a specific time for production (e.g. "02:00" for 2 AM). 
# Set to "none" to use the testing interval below.
janitor_schedule_time = "none" 
# Testing interval: How often the scanner wakes up (in seconds).
janitor_interval_secs = 60

max_age_days = 30
max_versions = 3

# --- Exclusions & Immediate Rules ---
exclude_dirs = ["/.git/", "/node_modules/", "/__pycache__/"]
temp_extensions = [".swp", ".tmp", ".crdownload", "~", ".part"]
immediate_archive_extensions = ["mp4", "mov", "iso", "zip", "tar", "gz"]
immediate_archive_dirs = ["/ArchiveDrop/"]
no_compress_extensions = ["mp4", "mkv", "avi", "mov", "zip", "tar", "gz", "rar", "7z", "jpg", "png", "iso"]
"#;


#[derive(Parser)]
#[command(name = "Husk", version = "1.0", about = "Husk Archiver: Hybrid User-Space Storage Kernel")]
struct Cli {
    #[arg(short, long, default_value = "husk_config.toml", global = true)]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the USTD daemon (Interceptor + Queue Worker)
    Daemon,
    /// Stream a file from tape directly to Standard Output (Zero-Disk extraction)
    Cat {
        #[arg(long)]
        file_path: String,
        #[arg(long, default_value = "0")]
        offset: u64,
        #[arg(long)]
        length: Option<u64>,
        #[arg(long)] 
        tape_uuid: Option<String>,
    },
    /// Format a raw block device, writing the Volume Header to LBA 0
    Format {
        #[arg(long)]
        tape_dev: String,
    },
    /// Deep scan a raw tape drive to rebuild a lost catalog DB
    Rebuild {
        #[arg(long)]
        tape_dev: String,
        #[arg(long, default_value = "husk_recovered.db")]
        output_db: String, // Output name for the recovered database
    },
    /// Check the capacity and usage of a tape (Tank Gauge)
    Info {
        #[arg(long)]
        tape_dev: Option<String>, // Optional. If omitted, checks Primary Volume.
    },
    /// Scrub a tape to verify BLAKE3 data integrity and detect bit-rot
    Scrub {
        #[arg(long)]
        tape_dev: Option<String>, // Optional. If omitted, checks Primary Volume.
    },
    /// Manually extract a specific historic version of a file (Point-in-Time Rollback)
    Restore {
        #[arg(long)]
        file_path: String,
        #[arg(long)]
        dest_path: String,
        #[arg(long)]
        version: Option<u32>,
    },
    /// Reclaim tape space by copying only the latest active files to a new tape (Garbage Collection)
    Repack {
        #[arg(long)]
        source_tape: String,
        #[arg(long)]
        dest_tape: String,
    }
}
// ---------------------------------------------------------
// 1. The 4KB Object Header Definition (Padding Fixed!)
// ---------------------------------------------------------
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ObjectHeader {
    pub magic_bytes: [u8; 8],     // offset 0
    pub format_version: u16,      // offset 8
    pub flags: u16,               // offset 10
    pub compression_type: u8,     // offset 12 (NEW: 0=None, 1=Zstd)
    pub reserved_pad: [u8; 3],    // offset 13 (Padding Fix)
    pub payload_size: u64,        // offset 16 (Uncompressed size)
    pub compressed_size: u64,     // offset 24 (NEW: Compressed length)
    pub padded_size: u64,         // offset 32 (Aligned on tape)
    pub object_uuid: [u8; 16],    // offset 40
    pub tape_uuid: [u8; 16],      // offset 56
    pub mtime: i64,               // offset 72
    pub ctime: i64,               // offset 80
    pub posix_mode: u32,          // offset 88
    pub uid: u32,                 // offset 92
    pub gid: u32,                 // offset 96
    pub data_checksum: [u8; 32],  // offset 100
    pub header_crc32: u32,        // offset 132
    // Fixed header total exactly 136 bytes.
    
    pub tlv_data: [u8; 3960],     // offset 136 -> 4096 bytes total
}

unsafe impl Zeroable for ObjectHeader {}
unsafe impl Pod for ObjectHeader {}

// ---------------------------------------------------------
// 1.5 The 4KB Volume Header (LBA 0)
// ---------------------------------------------------------
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct VolumeHeader {
    pub magic_bytes: [u8; 8],     // offset 0: "USTDVOL\0"
    pub format_version: u16,      // offset 8
    pub flags: u16,               // offset 10
    pub reserved_pad1: u32,       // offset 12: EXPLICIT PADDING FIX
    pub volume_uuid: [u8; 16],    // offset 16: Unique Tape ID
    pub created_at: i64,          // offset 32
    pub label: [u8; 32],          // offset 40: Human readable label
    pub reserved_pad2: [u8; 4024],// offset 72 -> strictly pads to 4096 bytes
}

unsafe impl Zeroable for VolumeHeader {}
unsafe impl Pod for VolumeHeader {}

// ---------------------------------------------------------
// Helper: Exclusion Zones (Goal 3.6)
// ---------------------------------------------------------
fn is_path_excluded(path: &str, config: &Arc<HuskConfig>) -> bool {
    if path.contains(".ustd_catalog.db") || path.contains(".img") {
        return true; // Hardcoded safety exclusions
    }
    if config.exclude_dirs.iter().any(|e| path.contains(e)) {
        return true;
    }
    if config.temp_extensions.iter().any(|ext| path.ends_with(ext)) {
        return true;
    }
    false
}

// ---------------------------------------------------------
// Tank Gauge: Capacity and Status Check
// ---------------------------------------------------------
fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;
    let b = bytes as f64;
    if b >= TB { format!("{:.2} TB", b / TB) }
    else if b >= GB { format!("{:.2} GB", b / GB) }
    else if b >= MB { format!("{:.2} MB", b / MB) }
    else if b >= KB { format!("{:.2} KB", b / KB) }
    else { format!("{} B", bytes) }
}

fn check_tape_gauge(tape_dev: &str, db_path: &str) -> std::io::Result<(u64, u64, u64)> {
    // 1. Rclone Cloud Targets
    if tape_dev.starts_with("rclone:") {
        let mut used_capacity = ALIGNMENT as u64;
        let mut active_data = 0;
        if Path::new(db_path).exists() {
            if let Ok(conn) = Connection::open(db_path) {
                let mut hasher = blake3::Hasher::new();
                hasher.update(tape_dev.as_bytes());
                let uuid_bytes: [u8; 16] = hasher.finalize().as_bytes()[0..16].try_into().unwrap();
                let tape_uuid_hex = uuid_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                
                let query_eof = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog WHERE tape_uuid = ?1";
                if let Ok(max_used) = conn.query_row(query_eof, params![tape_uuid_hex], |row| row.get::<_, i64>(0)) { used_capacity = max_used as u64; }

                let query_active = "SELECT COALESCE(SUM(((compressed_size + 4095) / 4096) * 4096 + 4096), 0) FROM catalog c1 INNER JOIN (SELECT file_path, MAX(version) as max_ver FROM catalog GROUP BY file_path) c2 ON c1.file_path = c2.file_path AND c1.version = c2.max_ver WHERE tape_uuid = ?1";
                if let Ok(act_data) = conn.query_row(query_active, params![tape_uuid_hex], |row| row.get::<_, i64>(0)) { active_data = act_data as u64; }
            }
        }
        return Ok((used_capacity, 1_125_899_906_842_624, active_data)); 
    }

    let meta = std::fs::metadata(tape_dev).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::NotFound, format!("Tape '{}' not found: {}", tape_dev, e))
    })?;

    let is_block_dev = (meta.mode() & libc::S_IFMT) == libc::S_IFBLK;
    let is_char_dev = (meta.mode() & libc::S_IFMT) == libc::S_IFCHR;

    // 2. Physical SCSI Tape Targets (Character Device)
    if is_char_dev {
        // Mock LTO-8 capacity (12TB) for now. Future updates will read hardware specs via sg_inq.
        let lto_capacity = 12_000_000_000_000; 
        let mut used_capacity = ALIGNMENT as u64;
        let active_data = 0; // Unused for now to prevent compiler warnings
        
        if Path::new(db_path).exists() {
            if let Ok(conn) = Connection::open(db_path) {
                let query = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog";
                if let Ok(max_used) = conn.query_row(query, [], |row| row.get::<_, i64>(0)) { used_capacity = max_used as u64; }
            }
        }
        return Ok((used_capacity, lto_capacity, active_data));
    }

    // 3. Local Block Devices (.img files, /dev/sdb)
    let total_capacity = if is_block_dev {
        let mut file = File::open(tape_dev)?;
        file.seek(SeekFrom::End(0)).unwrap_or(meta.len())
    } else {
        meta.len()
    };

    let mut used_capacity = ALIGNMENT as u64; 
    let mut active_data = 0;

    if Path::new(db_path).exists() {
        if let Ok(conn) = Connection::open(db_path) {
            let mut file = File::open(tape_dev)?;
            let mut vol_buf = [0u8; ALIGNMENT];
            if file.read_exact(&mut vol_buf).is_ok() {
                let vol_header: VolumeHeader = *bytemuck::from_bytes(&vol_buf);
                if &vol_header.magic_bytes == b"USTDVOL\0" {
                    let tape_uuid_hex = vol_header.volume_uuid.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                    
                    let query_eof = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog WHERE tape_uuid = ?1";
                    if let Ok(max_used) = conn.query_row(query_eof, params![tape_uuid_hex], |row| row.get::<_, i64>(0)) { used_capacity = max_used as u64; }

                    let query_active = "SELECT COALESCE(SUM(((compressed_size + 4095) / 4096) * 4096 + 4096), 0) FROM catalog c1 INNER JOIN (SELECT file_path, MAX(version) as max_ver FROM catalog GROUP BY file_path) c2 ON c1.file_path = c2.file_path AND c1.version = c2.max_ver WHERE tape_uuid = ?1";
                    if let Ok(act_data) = conn.query_row(query_active, params![tape_uuid_hex], |row| row.get::<_, i64>(0)) { active_data = act_data as u64; }
                }
            }
        }
    } else if !is_block_dev {
        used_capacity = std::cmp::max(meta.len(), ALIGNMENT as u64);
        active_data = used_capacity; 
    }

    used_capacity = std::cmp::min(used_capacity, total_capacity);
    Ok((used_capacity, total_capacity, active_data))
}

fn print_tape_gauge(tape_dev: &str, db_path: &str) {
    match check_tape_gauge(tape_dev, db_path) {
        Ok((used, total, active)) => {
            let percent = if total > 0 { (used as f64 / total as f64) * 100.0 } else { 0.0 };
            let bar_len = 40;
            let filled = ((percent / 100.0) * bar_len as f64).round() as usize;
            let filled = filled.clamp(0, bar_len);
            let empty = bar_len - filled;
            let bar = format!("[{}{}]", "█".repeat(filled), "░".repeat(empty));
            
            let wasteland = used.saturating_sub(active);
            let wasteland_pct = if used > 0 { (wasteland as f64 / used as f64) * 100.0 } else { 0.0 };

            info!("Volume Health ({}):", tape_dev);
            info!("   {} {:.2}% Written", bar, percent);
            info!("   Capacity: {} / {} total", format_bytes(used), format_bytes(total));
            info!("   Reclaimable Space: {} ({:.1}% of used space is deleted/old versions)", format_bytes(wasteland), wasteland_pct);
            
            if percent >= 95.0 {
                error!("⚠️ WARNING: Tape capacity is critically low!");
            }
            if wasteland_pct >= 40.0 && used > (total / 4) {
                info!(" TIP: Reclaimable Space is high. Consider running a Repacker to reclaim space.");
            }
        }
        Err(e) => error!("❌ Failed to read Volume Health: {}", e),
    }
}

// ---------------------------------------------------------
// Linux MTIO (Magnetic Tape I/O) Definitions
// ---------------------------------------------------------
#[repr(C)]
pub struct mtop {
    pub mt_op: libc::c_short,
    pub mt_count: libc::c_int,
}

pub const MTIOCTOP: libc::c_ulong = 0x40086d01;
pub const MTREW: libc::c_short = 1;   // Rewind tape
pub const MTWEOF: libc::c_short = 4;  // Write filemark
pub const MTFSF: libc::c_short = 11;  // Forward space file
pub const MTEOM: libc::c_short = 12;  // Space to end of recorded data

pub fn send_mtio_cmd(fd: i32, op: libc::c_short, count: libc::c_int) -> std::io::Result<()> {
    let mut mt_cmd = mtop { mt_op: op, mt_count: count };
    let ret = unsafe { libc::ioctl(fd, MTIOCTOP, &mut mt_cmd) };
    if ret < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------
// Helper: Safe Tape Opener with O_DIRECT Fallback
// ---------------------------------------------------------
fn open_tape_device(tape_dev: &str, read: bool, write: bool, create: bool, use_direct_io: bool) -> std::io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(read).write(write).create(create);

    // Detect character devices (e.g., /dev/nst0 physical SCSI tape)
    let is_char_dev = std::fs::metadata(tape_dev)
        .map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR)
        .unwrap_or(false);

    // O_DIRECT on Linux `st` character devices requires exact block size matching.
    // We disable it here to let the kernel handle SCSI frame buffering safely.
    let effective_direct = if is_char_dev { false } else { use_direct_io };

    if effective_direct {
        opts.custom_flags(libc::O_DIRECT);
        match opts.open(tape_dev) {
            Ok(file) => return Ok(file),
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                error!("⚠️ O_DIRECT is unsupported on '{}'. Falling back to buffered I/O. (Set DISABLE_O_DIRECT=1 to silence this)", tape_dev);
                let mut fallback_opts = OpenOptions::new();
                fallback_opts.read(read).write(write).create(create);
                return fallback_opts.open(tape_dev);
            }
            Err(e) => return Err(e), // Bubble up other errors (e.g., Permission Denied)
        }
    }

    opts.open(tape_dev)
}

fn format_tape(tape_dev: &str, use_direct_io: bool) -> std::io::Result<()> {
    info!("Formatting Volume '{}'...", tape_dev);
    let mut tape = open_tape_device(tape_dev, false, true, true, use_direct_io)?;

    let is_char_dev = std::fs::metadata(tape_dev)
        .map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR)
        .unwrap_or(false);

    if is_char_dev {
        info!("📼 Physical Tape Drive detected. Issuing SCSI Rewind (MTREW)...");
        if let Err(e) = send_mtio_cmd(tape.as_raw_fd(), MTREW, 1) {
            error!("⚠️ Tape rewind failed: {}. Ensure device is ready.", e);
        }
    }

    let new_uuid = Uuid::new_v4();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

    let vol_header = VolumeHeader {
        magic_bytes: *b"USTDVOL\0",
        format_version: 1,
        flags: 0,
        reserved_pad1: 0,
        volume_uuid: *new_uuid.as_bytes(),
        created_at: now,
        label: [0; 32], // Blank label for now
        reserved_pad2: [0; 4024],
    };

    let mut io_buf = AlignedBuffer::new(ALIGNMENT);
    io_buf.as_mut_slice().copy_from_slice(bytemuck::bytes_of(&vol_header));

    tape.seek(SeekFrom::Start(0))?;
    tape.write_all(io_buf.as_slice())?;
    tape.sync_all()?;

    info!("Volume formatted successfully! UUID: {}", new_uuid);
    Ok(())
}

// ---------------------------------------------------------
// 2. Aligned Memory Buffer (Crucial for O_DIRECT)
// ---------------------------------------------------------
pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    capacity: usize,
}

impl AlignedBuffer {
    pub fn new(capacity: usize) -> Self {
        let capacity = if capacity % ALIGNMENT == 0 {
            capacity
        } else {
            capacity + ALIGNMENT - (capacity % ALIGNMENT)
        };

        let layout = Layout::from_size_align(capacity, ALIGNMENT).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Self { ptr, layout, capacity }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.capacity) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) }
    }
}


// ---------------------------------------------------------
// 3. The "RawWrite" Archiver Logic (With Multiplexed Replication)
// ---------------------------------------------------------
struct MultiTapeWriter<'a> {
    tapes: Vec<&'a mut dyn std::io::Write>, // Changed to accept ANY generic Writer
    buffer: AlignedBuffer,
    cursor: usize,
    pub bytes_written: u64,
}

impl<'a> MultiTapeWriter<'a> {
    fn new(tapes: Vec<&'a mut dyn std::io::Write>) -> Self {
        // Buffer 256KB in RAM to force optimal LTO Hardware Block writes
        Self { tapes, buffer: AlignedBuffer::new(262144), cursor: 0, bytes_written: 0 }
    }
    fn pad_and_flush(&mut self) -> std::io::Result<()> {
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
            StorageBackend::Grid(_) => Ok(()), // Grid tunnels handle tracking internally
        }
    }
    pub fn sync(&mut self) -> std::io::Result<()> {
        match self {
            StorageBackend::Local(f) => f.sync_all(),
            StorageBackend::Tape(f) => f.sync_all(),
            StorageBackend::Rclone { stdin, .. } => stdin.flush(),
            StorageBackend::Grid(stream) => stream.flush(),
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

pub struct ActiveTape {
    pub backend: StorageBackend,
    pub dev_path: String,
    pub uuid_hex: String,
    pub volume_uuid: [u8; 16],
    pub start_offset: u64,
    pub is_append_only: bool, // Unified boolean for Cloud AND Physical Tapes
}

// Returns a tuple: (Vector of replicas, Vector of StreamGate Frames (UncompressedOffset, CompressedOffset, CompressedSize))
fn archive_file(conn: &Connection, source_path: &str, config: &Arc<HuskConfig>, use_direct_io: bool) -> std::io::Result<(Vec<(u64, u64, u64, u8, String, String, String)>, Vec<(u64, u64, u64)>)> {
    info!("Archiving '{}'...", source_path);
    
    let sidecar = SidecarBridge::new(config);
    let mut src_file = File::open(source_path)?;
    let src_meta = src_file.metadata()?;
    let payload_size = src_meta.len();
    let estimated_need = payload_size + (ALIGNMENT as u64) * 2;

    // Telemetry Broadcast
    sidecar.send_event(json!({
        "event": "ARCHIVE_START", 
        "file": source_path, 
        "size": payload_size
    }));

    let mut active_tapes: Vec<ActiveTape> = Vec::new();

    // Reusable closure to evaluate and attach a drive/remote
    let mut try_attach_tape = |dev_path: &str| -> bool {
        // Multi-Node Remote Hand-off Hook
        if dev_path.starts_with("husk-grid://") {
            let mut hasher = blake3::Hasher::new();
            hasher.update(dev_path.as_bytes());
            let uuid_bytes: [u8; 16] = hasher.finalize().as_bytes()[0..16].try_into().unwrap();
            let uuid_hex = uuid_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
            
            let query = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog WHERE tape_uuid = ?1";
            let logical_eof = conn.query_row(query, params![uuid_hex], |row| row.get::<_, i64>(0)).unwrap_or(4096) as u64;
            let start_offset = if logical_eof % ALIGNMENT as u64 == 0 { logical_eof } else { logical_eof + ALIGNMENT as u64 - (logical_eof % ALIGNMENT as u64) };

            if let Some(ref sock_path) = config.sidecar_socket_path {
                if let Ok(mut stream) = UnixStream::connect(sock_path) {
                    let msg = json!({"action": "GRID_TUNNEL_WRITE", "target": dev_path, "offset": start_offset});
                    let _ = stream.write_all(msg.to_string().as_bytes());
                    let _ = stream.write_all(b"\n");
                    
                    let mut buf = [0u8; 1024];
                    if let Ok(n) = std::io::Read::read(&mut stream, &mut buf) {
                        if String::from_utf8_lossy(&buf[..n]).trim() == "READY" {
                            info!("[Archiver] Selected Grid Target: {}", dev_path);
                            active_tapes.push(ActiveTape { backend: StorageBackend::Grid(stream), dev_path: dev_path.to_string(), uuid_hex, volume_uuid: uuid_bytes, start_offset, is_append_only: true });
                            return true;
                        }
                    }
                }
            }
            return false;
        }
        if dev_path.starts_with("rclone:") {
            let mut hasher = blake3::Hasher::new();
            hasher.update(dev_path.as_bytes());
            let uuid_bytes: [u8; 16] = hasher.finalize().as_bytes()[0..16].try_into().unwrap();
            let uuid_hex = uuid_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();

            let query = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog WHERE tape_uuid = ?1";
            let logical_eof = conn.query_row(query, params![uuid_hex], |row| row.get::<_, i64>(0)).unwrap_or(4096) as u64;
            let start_offset = if logical_eof % ALIGNMENT as u64 == 0 { logical_eof } else { logical_eof + ALIGNMENT as u64 - (logical_eof % ALIGNMENT as u64) };

            let clean_remote = dev_path.strip_prefix("rclone:").unwrap();
            let object_path = format!("{}/husk_{}.bin", clean_remote, start_offset);

            if let Ok(backend) = spawn_rclone_writer(&object_path) {
                info!("[Archiver] Selected Cloud Target: {} (Virtual Offset: {})", dev_path, start_offset);
                active_tapes.push(ActiveTape { backend, dev_path: dev_path.to_string(), uuid_hex, volume_uuid: uuid_bytes, start_offset, is_append_only: true });
                return true;
            }
        } else {
            // SIDE-CAR HARDWARE WAKE-UP HOOK
            let location_hint: String = conn.query_row(
                "SELECT location_hint FROM tapes WHERE device_path = ?1", 
                params![dev_path], |row| row.get(0)
            ).unwrap_or_default();
            
            if let Err(e) = sidecar.wake_volume("UNKNOWN", dev_path, &location_hint) {
                error!("⚠️ Hardware timeout for {}: {}", dev_path, e);
                sidecar.send_event(json!({"event": "HARDWARE_TIMEOUT", "device": dev_path}));
                return false;
            }

            if let Ok(mut tape) = open_tape_device(dev_path, true, true, true, use_direct_io) {
                let tape_meta = tape.metadata().unwrap();
                let is_char_dev = (tape_meta.mode() & libc::S_IFMT) == libc::S_IFCHR;

                if !is_char_dev && tape.seek(SeekFrom::Start(0)).is_err() { return false; }
                if is_char_dev { let _ = send_mtio_cmd(tape.as_raw_fd(), MTREW, 1); }

                let mut vol_buf = AlignedBuffer::new(ALIGNMENT);
                if tape.read_exact(vol_buf.as_mut_slice()).is_err() { return false; }
                
                let vol_header: VolumeHeader = *bytemuck::from_bytes(vol_buf.as_slice());
                if &vol_header.magic_bytes != b"USTDVOL\0" { return false; }
                
                let uuid_hex = vol_header.volume_uuid.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                let query = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog WHERE tape_uuid = ?1";
                let logical_eof = conn.query_row(query, params![uuid_hex], |row| row.get::<_, i64>(0)).unwrap_or(4096) as u64;
                let start_offset = if logical_eof % ALIGNMENT as u64 == 0 { logical_eof } else { logical_eof + ALIGNMENT as u64 - (logical_eof % ALIGNMENT as u64) };

                let is_block_dev = (tape_meta.mode() & libc::S_IFMT) == libc::S_IFBLK;
                let total_capacity = if is_block_dev {
                    let mut f2 = std::fs::File::open(dev_path).unwrap();
                    f2.seek(SeekFrom::End(0)).unwrap_or(0)
                } else if is_char_dev {
                    12_000_000_000_000 // Mock 12TB for Tape
                } else { tape_meta.len() };

                if start_offset + estimated_need <= total_capacity {
                    let backend = if is_char_dev {
                        info!("📼 Appending to End of Data (MTEOM) on {}...", dev_path);
                        let _ = send_mtio_cmd(tape.as_raw_fd(), MTEOM, 1);
                        StorageBackend::Tape(tape)
                    } else {
                        StorageBackend::Local(tape)
                    };

                    info!("[Archiver] Selected Local Target: {} (UUID: {})", dev_path, uuid_hex);
                    active_tapes.push(ActiveTape { backend, dev_path: dev_path.to_string(), uuid_hex, volume_uuid: vol_header.volume_uuid, start_offset, is_append_only: is_char_dev });
                    return true;
                }
            }
        }
        false
    };

    // Tier 1: Primary Volume (Fallback to Failover if full/disconnected)
    let mut primary_secured = false;
    for dev in &config.primary_volumes {
        if try_attach_tape(dev) { primary_secured = true; break; }
    }
    if !primary_secured {
        error!("⚠️ Primary volumes unavailable/full! Attempting Failover Tier...");
        for dev in &config.failover_volumes {
            if try_attach_tape(dev) { break; }
        }
    }

    // Tier 2: Replication Volumes
    let mut replicas_secured = 0;
    for dev in &config.replication_volumes {
        if replicas_secured >= config.replicas { break; }
        if try_attach_tape(dev) { replicas_secured += 1; }
    }

    if active_tapes.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "No valid tape pools available with capacity."));
    }

    // 2. Reserve Headers on all Tapes (Skip seeking for rclone streams)
    let mut header_buf = AlignedBuffer::new(ALIGNMENT);
    for tape in &mut active_tapes {
        tape.backend.seek_to(tape.start_offset)?;
        tape.backend.write_all(header_buf.as_slice())?;
    }

    // 3. Stream File -> Blake3 -> Zstd -> MultiTapeWriter (Simultaneous N-Way write)
    let mut hasher = Hasher::new();
    let final_compressed_size: u64;
    let final_padded_size: u64;

    let mut jump_table: Vec<(u64, u64, u64)> = Vec::new();
    
    // NEW LOGIC: Check config to bypass Zstd compression
    let file_ext = std::path::Path::new(source_path).extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    let use_compression = !config.no_compress_extensions.contains(&file_ext);
    let compression_type_flag: u8 = if use_compression { 1 } else { 0 };

    {
        let tape_refs: Vec<&mut dyn std::io::Write> = active_tapes
            .iter_mut()
            .map(|t| &mut t.backend as &mut dyn std::io::Write)
            .collect();
            
        let mut tape_writer = MultiTapeWriter::new(tape_refs);
        let mut io_buf = vec![0; 1024 * 1024]; // 1MB buffer
        
        if use_compression {
            let chunk_size = 16 * 1024 * 1024; // 16MB Frame boundaries for StreamGate
            let mut current_frame_bytes = 0;
            let mut uncompressed_start = 0;
            let mut compressed_start = 0;

            let mut encoder = zstd::stream::write::Encoder::new(&mut tape_writer, 3)?; 

            loop {
                let n = src_file.read(&mut io_buf)?;
                if n == 0 { break; }
                hasher.update(&io_buf[..n]);
                
                encoder.write_all(&io_buf[..n])?;
                current_frame_bytes += n;
                
                if current_frame_bytes >= chunk_size {
                    encoder.finish()?;
                    let current_compressed_offset = tape_writer.bytes_written + tape_writer.cursor as u64;
                    let compressed_size = current_compressed_offset - compressed_start;
                    jump_table.push((uncompressed_start, compressed_start, compressed_size));
                    
                    uncompressed_start += current_frame_bytes as u64;
                    compressed_start = current_compressed_offset;
                    current_frame_bytes = 0;
                    encoder = zstd::stream::write::Encoder::new(&mut tape_writer, 3)?;
                }
            }
            encoder.finish()?;
            
            let current_compressed_offset = tape_writer.bytes_written + tape_writer.cursor as u64;
            let compressed_size = current_compressed_offset - compressed_start;
            if compressed_size > 0 {
                jump_table.push((uncompressed_start, compressed_start, compressed_size));
            }
            final_compressed_size = current_compressed_offset;
        } else {
            // HIGH-PERFORMANCE RAW COPY (Compression Bypassed)
            loop {
                let n = src_file.read(&mut io_buf)?;
                if n == 0 { break; }
                hasher.update(&io_buf[..n]);
                tape_writer.write_all(&io_buf[..n])?;
            }
            final_compressed_size = tape_writer.bytes_written + tape_writer.cursor as u64;
        }

        tape_writer.pad_and_flush()?;
        final_padded_size = tape_writer.bytes_written;
    }
    
    let data_hash = hasher.finalize();
    let hash_hex = data_hash.to_hex().to_string();

    // 4. Commit Unique Headers per Tape
    let mut results = Vec::new();
    for tape in &mut active_tapes {
        let mut header = ObjectHeader {
            magic_bytes: *b"USTD\x00\x01\x02\x03", format_version: 1, flags: 0, compression_type: compression_type_flag,
            reserved_pad: [0; 3], payload_size, compressed_size: final_compressed_size, padded_size: final_padded_size,
            object_uuid: [0; 16], tape_uuid: tape.volume_uuid,
            mtime: src_meta.mtime(), ctime: src_meta.ctime(), posix_mode: src_meta.mode(), uid: src_meta.uid(), gid: src_meta.gid(),
            data_checksum: *data_hash.as_bytes(), header_crc32: 0, tlv_data: [0; 3960],
        };

        let filename = Path::new(source_path).file_name().unwrap().to_str().unwrap().as_bytes();
        let mut tlv_offset = 0;
        
        // Type 0x01: Pack Filename
        if tlv_offset + 4 + filename.len() <= header.tlv_data.len() {
            header.tlv_data[tlv_offset] = 0x00; header.tlv_data[tlv_offset+1] = 0x01;
            header.tlv_data[tlv_offset+2..tlv_offset+4].copy_from_slice(&(filename.len() as u16).to_be_bytes());
            header.tlv_data[tlv_offset+4..tlv_offset+4 + filename.len()].copy_from_slice(filename);
            tlv_offset += 4 + filename.len();
        }

        // Type 0x02: Pack Extracted Xattrs
        if let Ok(xattrs) = xattr::list(source_path) {
            for attr in xattrs {
                if attr.to_string_lossy().starts_with("trusted.ustd") { continue; } // Skip internal status
                if let Ok(Some(val)) = xattr::get(source_path, &attr) {
                    let attr_bytes = attr.to_string_lossy().as_bytes().to_vec();
                    // Custom Packing Format: [NameLen: 1 byte] [Name] [ValLen: 2 bytes] [Val]
                    let total_len = 1 + attr_bytes.len() + 2 + val.len();
                    if tlv_offset + 4 + total_len <= header.tlv_data.len() {
                        header.tlv_data[tlv_offset] = 0x00; header.tlv_data[tlv_offset+1] = 0x02;
                        header.tlv_data[tlv_offset+2..tlv_offset+4].copy_from_slice(&(total_len as u16).to_be_bytes());
                        
                        let payload_start = tlv_offset + 4;
                        header.tlv_data[payload_start] = attr_bytes.len() as u8;
                        header.tlv_data[payload_start+1 .. payload_start+1+attr_bytes.len()].copy_from_slice(&attr_bytes);
                        
                        let val_len_start = payload_start+1+attr_bytes.len();
                        header.tlv_data[val_len_start .. val_len_start+2].copy_from_slice(&(val.len() as u16).to_be_bytes());
                        header.tlv_data[val_len_start+2 .. val_len_start+2+val.len()].copy_from_slice(&val);
                        
                        tlv_offset += 4 + total_len;
                    }
                }
            }
        }

        // NEW Type 0x03: Pack StreamGate Jump Table (Array of u32 compressed sizes)
        // Note: 16MB frames mean we only need 4 bytes per frame. A 10GB file only needs ~600 frames (2.4KB).
        let frames_payload_len = jump_table.len() * 4;
        if tlv_offset + 4 + frames_payload_len <= header.tlv_data.len() {
            header.tlv_data[tlv_offset] = 0x00; header.tlv_data[tlv_offset+1] = 0x03;
            header.tlv_data[tlv_offset+2..tlv_offset+4].copy_from_slice(&(frames_payload_len as u16).to_be_bytes());
            
            let mut p = tlv_offset + 4;
            for (_, _, c_size) in &jump_table {
                // Compress size fits easily in u32 since max frame is ~16MB
                header.tlv_data[p..p+4].copy_from_slice(&(*c_size as u32).to_be_bytes());
                p += 4;
            }
            // tlv_offset += 4 + frames_payload_len; 
        }

        let mut crc = Crc32Hasher::new();
        crc.update(bytemuck::bytes_of(&header));
        header.header_crc32 = crc.finalize();

        // Write Header. (For rclone, this will be appended at the END of the file as a footer. 
        // For local disks, we seek back and overwrite the zeroed header block at the start.)
        tape.backend.seek_to(tape.start_offset)?;
        header_buf.as_mut_slice().copy_from_slice(bytemuck::bytes_of(&header));
        tape.backend.write_all(header_buf.as_slice())?;
        results.push((tape.start_offset, payload_size, final_compressed_size, compression_type_flag, hash_hex.clone(), tape.uuid_hex.clone(), tape.dev_path.clone()));
    }

    // CRITICAL: Wait for all uploads to finish before returning
    for tape in active_tapes {
        if let Err(e) = tape.backend.close() {
            error!("❌ Cloud finalization failed for {}: {}", tape.dev_path, e);
            return Err(e);
        }
    }

    let destination_names: Vec<String> = results.iter().map(|r| r.6.clone()).collect();
    info!("✅ Replicated {} times to [{}] ({} bytes -> {} bytes)", 
        results.len(), 
        destination_names.join(", "),
        payload_size, 
        final_compressed_size
    );
    
    // Telemetry Success
    sidecar.send_event(json!({
        "event": "ARCHIVE_COMPLETE", 
        "file": source_path, 
        "replicas": results.len()
    }));
    
    Ok((results, jump_table))
}
// ---------------------------------------------------------
// 4. The "RawRead" Restorer Logic (With Zstd Decoder)
// ---------------------------------------------------------
struct HashWriter<W: std::io::Write> {
    inner: W,
    hasher: blake3::Hasher,
}
impl<W: std::io::Write> std::io::Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> { self.inner.flush() }
}
impl<W: std::io::Write> HashWriter<W> {
    fn finalize_hash(self) -> blake3::Hash { self.hasher.finalize() }
}

// ---------------------------------------------------------
// StreamGate: RAM-Buffered Offset Skipper
// ---------------------------------------------------------
struct SkipWriter<W: std::io::Write> {
    inner: W,
    bytes_to_skip: u64,
    bytes_written: u64,
    limit: Option<u64>,
}

impl<W: std::io::Write> std::io::Write for SkipWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut start = 0;
        // 1. Skip bytes if we haven't reached the target offset yet
        if self.bytes_to_skip > 0 {
            let skip = std::cmp::min(self.bytes_to_skip as usize, buf.len());
            self.bytes_to_skip -= skip as u64;
            start = skip;
        }

        // 2. Write the remaining bytes to the destination
        if start < buf.len() {
            let mut to_write = buf.len() - start;
            if let Some(lim) = self.limit {
                if self.bytes_written >= lim {
                    return Ok(buf.len()); // Pretend we wrote it so the Zstd decoder doesn't panic
                }
                to_write = std::cmp::min(to_write, (lim - self.bytes_written) as usize);
            }
            if to_write > 0 {
                self.inner.write_all(&buf[start..start + to_write])?;
                self.bytes_written += to_write as u64;
            }
        }
        Ok(buf.len()) // Always return full length to keep the decompression stream pumping
    }
    fn flush(&mut self) -> std::io::Result<()> { self.inner.flush() }
}

fn stream_file<W: std::io::Write>(config: &Arc<HuskConfig>, db_path: &str, file_path: &str, offset: u64, length: Option<u64>, use_direct_io: bool, target_uuid: Option<&str>, out_handle: &mut W) -> std::io::Result<()> {
    let sidecar = SidecarBridge::new(config);
    sidecar.send_event(json!({"event": "STREAM_START", "file": file_path, "offset": offset}));
    
    let conn = Connection::open(db_path).map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "DB Open Failed"))?;
    
    // Locate the latest version of the file, optionally on a specific tape
    let query = if target_uuid.is_some() {
        "SELECT tape_uuid, tape_offset, compressed_size, compression_type, payload_size, version 
         FROM catalog WHERE file_path = ?1 AND tape_uuid = ?2 ORDER BY version DESC LIMIT 1"
    } else {
        "SELECT tape_uuid, tape_offset, compressed_size, compression_type, payload_size, version 
         FROM catalog WHERE file_path = ?1 ORDER BY version DESC LIMIT 1"
    };

    let (tape_uuid, tape_offset, db_comp, db_type, payload_size, version): (String, u64, u64, u8, u64, u32) = if let Some(uuid) = target_uuid {
        conn.query_row(query, params![file_path, uuid], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)))
    } else {
        conn.query_row(query, params![file_path], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)))
    }.map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "File/Tape combination not found"))?;

    let (tape_dev, location_hint): (String, String) = conn.query_row(
        "SELECT device_path, COALESCE(location_hint, '') FROM tapes WHERE tape_uuid = ?1",
        params![tape_uuid], |row| Ok((row.get(0)?, row.get(1)?))
    ).unwrap_or((String::new(), String::new()));

    let is_char_dev = std::fs::metadata(&tape_dev).map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR).unwrap_or(false);

    // Pre-Flight Wake Hook
    if !tape_dev.starts_with("rclone:") && !tape_dev.starts_with("husk-grid:") {
        if let Err(e) = sidecar.wake_volume(&tape_uuid, &tape_dev, &location_hint) {
            error!("⚠️ Hardware timeout for read {}: {}", tape_dev, e);
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Hardware Wake Timeout"));
        }
    }

    // --- STREAMGATE MATH (Jump Table Lookup) ---
    let req_start = offset;
    let req_end = length.map(|l| offset + l).unwrap_or(payload_size);
    
    let mut stmt = conn.prepare("SELECT uncompressed_offset, compressed_offset, compressed_size FROM object_frames WHERE file_path = ?1 AND version = ?2 ORDER BY uncompressed_offset ASC").unwrap();
    let frames: Vec<(u64, u64, u64)> = stmt.query_map(params![file_path, version], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap().filter_map(Result::ok).collect();

    let mut target_c_offset = 0;
    let mut target_c_len = ((db_comp + 4095) / 4096) * 4096; // Default to reading padded EOF
    let mut skip_bytes = offset;

    info!("[StreamGate] Requested file: {}, version: {}, offset: {}, length: {:?}", file_path, version, offset, length);

    if db_type == 1 && !frames.is_empty() {
        let mut start_idx = 0;
        let mut end_idx = frames.len() - 1;

        for (i, f) in frames.iter().enumerate() {
            if f.0 <= req_start { start_idx = i; }
            if f.0 < req_end { end_idx = i; }
        }

        target_c_offset = frames[start_idx].1;
        let final_frame = &frames[end_idx];
        target_c_len = (final_frame.1 + final_frame.2) - target_c_offset;
        skip_bytes = req_start - frames[start_idx].0;
        
        info!("[StreamGate] Frames found: {}. Selected range {} to {}", frames.len(), start_idx, end_idx);
        info!("[StreamGate] Compressed Target Offset: {}, Target Length: {}", target_c_offset, target_c_len);
        info!("[StreamGate] Uncompressed Bytes to Skip from Frame Start: {}", skip_bytes);
    } else if db_type == 0 {
        // INSTANT O(1) SEEK FOR RAW FILES!
        target_c_offset = req_start;
        target_c_len = req_end - req_start;
        skip_bytes = 0;
        info!("[StreamGate] Raw Uncompressed File. Direct O(1) seek to offset {}. Target Length: {}", target_c_offset, target_c_len);
    }

    let abs_start_offset = tape_offset + 4096 + target_c_offset;

    // --- FIX: O_DIRECT BLOCK ALIGNMENT ---
    // We must seek and read in multiples of 4096.
    let aligned_offset = abs_start_offset - (abs_start_offset % 4096);
    let alignment_skew = (abs_start_offset % 4096) as usize;
    
    info!("[StreamGate] Absolute Start Offset: {} (Aligned: {}, Skew: {})", abs_start_offset, aligned_offset, alignment_skew);
    
    let total_bytes_to_read = alignment_skew as u64 + target_c_len;
    let padded_target_c_len = if total_bytes_to_read % 4096 == 0 {
        total_bytes_to_read
    } else {
        total_bytes_to_read + 4096 - (total_bytes_to_read % 4096)
    };

    // --- STORAGE READER SPAWN ---
    let mut tape: StorageReader = if tape_dev.starts_with("husk-grid://") {
        let mut stream = std::os::unix::net::UnixStream::connect(config.sidecar_socket_path.as_deref().unwrap_or("/tmp/husk_sidecar.sock"))?;
        let msg = json!({"action": "GRID_TUNNEL_READ", "target": tape_dev, "offset": tape_offset, "count": padded_target_c_len});
        let _ = stream.write_all(msg.to_string().as_bytes());
        let _ = stream.write_all(b"\n");
        
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf)?;
        if String::from_utf8_lossy(&buf[..n]).trim() != "READY" {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Grid tunnel failed"));
        }
        StorageReader::Grid(stream)
    } else if tape_dev.starts_with("rclone:") {
        let clean_remote = tape_dev.strip_prefix("rclone:").unwrap();
        let object_path = format!("{}/husk_{}.bin", clean_remote, tape_offset);
        
        // FIX: Rclone objects are individual files, not one giant tape. 
        // We must convert the absolute virtual tape offset into a file-relative offset!
        let file_relative_offset = aligned_offset - tape_offset;
        
        info!("[StreamGate] Spawning Rclone -> cat {} --offset {} --count {}", object_path, file_relative_offset, padded_target_c_len);
        
        let mut cmd = Command::new("rclone");
        cmd.arg("cat")
           .arg("--offset").arg(file_relative_offset.to_string())
           .arg("--count").arg(padded_target_c_len.to_string())
           .arg(&object_path);
           
        let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::null()).spawn()?;
        StorageReader::Rclone(child.stdout.take().unwrap(), child)
        
    } else if is_char_dev {
        let mut f = open_tape_device(&tape_dev, true, false, false, use_direct_io)?;
        let fd = f.as_raw_fd();
        let file_index: i32 = conn.query_row("SELECT COUNT(*) FROM catalog WHERE tape_uuid = ?1 AND tape_offset < ?2", params![tape_uuid, tape_offset], |row| row.get(0)).unwrap_or(0);
        let _ = send_mtio_cmd(fd, MTREW, 1);
        if file_index > 0 { let _ = send_mtio_cmd(fd, MTFSF, file_index); }
        
        let mut discard_buf = AlignedBuffer::new(ALIGNMENT * 256);
        let mut bytes_to_discard = (4096 + target_c_offset) - alignment_skew as u64;
        while bytes_to_discard > 0 {
            let chunk = std::cmp::min(bytes_to_discard, discard_buf.capacity as u64) as usize;
            f.read_exact(&mut discard_buf.as_mut_slice()[..chunk])?;
            bytes_to_discard -= chunk as u64;
        }
        StorageReader::Local(f)
    } else {
        let mut f = open_tape_device(&tape_dev, true, false, false, use_direct_io)?;
        f.seek(SeekFrom::Start(aligned_offset))?;
        StorageReader::Local(f)
    };

    // Direct output into whatever writer was passed (TCP Socket or Stdout)
    let mut skip_writer = SkipWriter { inner: out_handle, bytes_to_skip: skip_bytes, bytes_written: 0, limit: length };

    // Struct encapsulating O_DIRECT blocks, skews, and EOF logic safely.
    struct AlignedTapeReader<'a> {
        tape: &'a mut StorageReader,
        io_buf: AlignedBuffer,
        buf_start: usize,
        buf_end: usize,
        alignment_skew: usize,
        skew_discarded: usize, 
        target_c_len: usize,
        bytes_produced: usize,
    }

    impl<'a> std::io::Read for AlignedTapeReader<'a> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            // Stop providing bytes once we've fulfilled the exact compressed frame length
            if self.bytes_produced >= self.target_c_len {
                return Ok(0); 
            }

            // 1. Safely discard the alignment skew, tolerating highly fragmented stream reads
            while self.skew_discarded < self.alignment_skew {
                if self.buf_start >= self.buf_end {
                    let to_read = self.io_buf.capacity;
                    let n = self.tape.read(&mut self.io_buf.as_mut_slice()[..to_read])?;
                    if n == 0 { return Ok(0); } // Unexpected early EOF
                    self.buf_start = 0;
                    self.buf_end = n;
                }
                
                let available = self.buf_end - self.buf_start;
                let to_discard = std::cmp::min(available, self.alignment_skew - self.skew_discarded);
                
                self.buf_start += to_discard;
                self.skew_discarded += to_discard;
            }

            // 2. Read actual compressed payload (Replenish if buffer empty)
            if self.buf_start >= self.buf_end {
                let to_read = self.io_buf.capacity;
                let n = self.tape.read(&mut self.io_buf.as_mut_slice()[..to_read])?;
                if n == 0 { return Ok(0); }
                self.buf_start = 0;
                self.buf_end = n;
            }

            // 3. Serve the target payload, clamped by remaining target_c_len
            let available = self.buf_end - self.buf_start;
            let remaining = self.target_c_len - self.bytes_produced;
            let chunk = std::cmp::min(std::cmp::min(buf.len(), available), remaining);

            if chunk > 0 {
                buf[..chunk].copy_from_slice(&self.io_buf.as_slice()[self.buf_start .. self.buf_start + chunk]);
                self.buf_start += chunk;
                self.bytes_produced += chunk;
            }
            
            Ok(chunk)
        }
    }

    let mut aligned_reader = AlignedTapeReader {
        tape: &mut tape,
        io_buf: AlignedBuffer::new(ALIGNMENT * 256), // 1MB properly aligned block for O_DIRECT
        buf_start: 0,
        buf_end: 0,
        alignment_skew,
        skew_discarded: 0, 
        target_c_len: target_c_len as usize,
        bytes_produced: 0,
    };

    if db_type == 1 {
        info!("[StreamGate] Launching Zstd Read-Decoder on Target Frames...");
        let mut decoder = zstd::stream::read::Decoder::new(aligned_reader)?;
        std::io::copy(&mut decoder, &mut skip_writer)?;
        skip_writer.flush()?;
        info!("[StreamGate] StreamGate Extraction Complete!");
    } else {
        info!("[StreamGate] Copying raw uncompressed bytes...");
        std::io::copy(&mut aligned_reader, &mut skip_writer)?;
        skip_writer.flush()?;
        info!("[StreamGate] StreamGate Extraction Complete!");
    }

    Ok(())
}

// CLI Wrapper for stream_file mapping to standard output
fn cat_file(config: &Arc<HuskConfig>, db_path: &str, file_path: &str, offset: u64, length: Option<u64>, use_direct_io: bool, target_uuid: Option<&str>) -> std::io::Result<()> {
    let stdout = std::io::stdout();
    let mut out_handle = stdout.lock();
    stream_file(config, db_path, file_path, offset, length, use_direct_io, target_uuid, &mut out_handle)
}

fn restore_file(config: &Arc<HuskConfig>, db_path: &str, tape_dev: &str, file_path: &str, dest_fd: i32, tape_offset: u64, use_direct_io: bool, is_manual: bool) -> std::io::Result<()> {
    info!("\nRestoring from Volume '{}'...", tape_dev);
    
    let sidecar = SidecarBridge::new(config);
    sidecar.send_event(json!({"event": "RESTORE_START", "file": file_path}));

    // 1. Fetch exact object size from DB to drive the stream logic
    let conn = Connection::open(db_path).map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "DB Open Failed"))?;
    let (db_payload, db_comp, db_type): (u64, u64, u8) = conn.query_row(
        "SELECT payload_size, compressed_size, compression_type FROM catalog WHERE file_path = ?1 AND tape_offset = ?2",
        params![file_path, tape_offset],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    ).map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "Object not found in catalog"))?;
    
    let padded_size = ((db_comp + 4095) / 4096) * 4096;

    let is_char_dev = std::fs::metadata(tape_dev)
        .map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR)
        .unwrap_or(false);

    // Fetch location hint and Wake Hardware
    let tape_uuid: String = conn.query_row(
        "SELECT tape_uuid FROM catalog WHERE file_path = ?1 AND tape_offset = ?2",
        params![file_path, tape_offset], |row| row.get(0)
    ).unwrap_or_default();
    
    let location_hint: String = conn.query_row(
        "SELECT COALESCE(location_hint, '') FROM tapes WHERE tape_uuid = ?1",
        params![tape_uuid], |row| row.get(0)
    ).unwrap_or_default();

    if !tape_dev.starts_with("rclone:") && !tape_dev.starts_with("husk-grid:") {
        if let Err(e) = sidecar.wake_volume(&tape_uuid, tape_dev, &location_hint) {
            error!("⚠️ Hardware timeout for read {}: {}", tape_dev, e);
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Hardware Wake Timeout"));
        }
    }

    // 2. Open correct stream backend
    let mut tape: StorageReader = if tape_dev.starts_with("husk-grid://") {
        let mut stream = std::os::unix::net::UnixStream::connect(config.sidecar_socket_path.as_deref().unwrap_or("/tmp/husk_sidecar.sock"))?;
        let msg = json!({"action": "GRID_TUNNEL_READ", "target": tape_dev, "offset": tape_offset, "count": padded_size});
        let _ = stream.write_all(msg.to_string().as_bytes());
        let _ = stream.write_all(b"\n");
        
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf)?;
        if String::from_utf8_lossy(&buf[..n]).trim() != "READY" {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Grid tunnel failed"));
        }
        StorageReader::Grid(stream)
    } else if tape_dev.starts_with("rclone:") {
        let clean_remote = tape_dev.strip_prefix("rclone:").unwrap();
        let object_path = format!("{}/husk_{}.bin", clean_remote, tape_offset);
        let mut child = Command::new("rclone")
            .arg("cat")
            .arg(&object_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        StorageReader::Rclone(child.stdout.take().unwrap(), child)
    } else if is_char_dev {
        let f = open_tape_device(tape_dev, true, false, false, use_direct_io)?;
        let fd = f.as_raw_fd();
        
        let tape_uuid: String = conn.query_row(
            "SELECT tape_uuid FROM catalog WHERE file_path = ?1 AND tape_offset = ?2",
            params![file_path, tape_offset], |row| row.get(0)
        ).unwrap_or_default();

        let file_index: i32 = conn.query_row(
            "SELECT COUNT(*) FROM catalog WHERE tape_uuid = ?1 AND tape_offset < ?2",
            params![tape_uuid, tape_offset], |row| row.get(0)
        ).unwrap_or(0);

        info!("📼 Physical Tape: Rewinding and advancing {} Filemarks...", file_index);
        let _ = send_mtio_cmd(fd, MTREW, 1);
        if file_index > 0 { let _ = send_mtio_cmd(fd, MTFSF, file_index); }
        StorageReader::Local(f)
    } else {
        let mut f = open_tape_device(tape_dev, true, false, false, use_direct_io)?;
        f.seek(SeekFrom::Start(tape_offset))?;
        StorageReader::Local(f)
    };

    let mut header_buf = AlignedBuffer::new(ALIGNMENT);
    let mut header: ObjectHeader;

    if tape_dev.starts_with("rclone:") || is_char_dev {
        // Append-Only Media streams contain: 4KB dummy -> Payload -> 4KB Real Header
        tape.read_exact(header_buf.as_mut_slice())?; // Discard dummy
        header = unsafe { std::mem::zeroed() }; // Mock header to pass into the decoding loop
        header.payload_size = db_payload;
        header.compressed_size = db_comp;
        header.padded_size = padded_size;
        header.compression_type = db_type;
    } else {
        tape.read_exact(header_buf.as_mut_slice())?;
        header = *bytemuck::from_bytes(header_buf.as_slice());

        let stored_crc = header.header_crc32;
        header.header_crc32 = 0;
        let mut crc = Crc32Hasher::new();
        crc.update(bytemuck::bytes_of(&header));
        if crc.finalize() != stored_crc {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Header CRC mismatch!"));
        }
        info!("Header verified! Expected Payload: {} bytes.", header.payload_size);
    }

    let dup_fd = unsafe { libc::dup(dest_fd) };
    if dup_fd < 0 { return Err(std::io::Error::last_os_error()); }

    // --- FIX: Explicitly enforce original UNIX ownership and permissions ---
    // Only apply for manual restores to prevent stripping POSIX ACLs on SMB shares.
    // The fanotify daemon doesn't need this because hole-punched stubs retain their original permissions/ACLs.
    if is_manual {
        unsafe {
            if header.uid != 0 || header.gid != 0 {
                libc::fchown(dup_fd, header.uid, header.gid);
            }
            if header.posix_mode != 0 {
                libc::fchmod(dup_fd, header.posix_mode & 0o7777); // Apply standard read/write permissions
            }
        }
    }

    let mut dest_file = unsafe { std::fs::File::from_raw_fd(dup_fd) };
    dest_file.seek(SeekFrom::Start(0)).unwrap_or(0);

    let hash_writer = HashWriter { inner: dest_file, hasher: Hasher::new() };
    let mut io_buf = AlignedBuffer::new(ALIGNMENT * 256);
    let mut bytes_read: u64 = 0;

    let final_hash = if header.compression_type == 1 {
        let mut decoder = zstd::stream::write::Decoder::new(hash_writer)?;
        while bytes_read < header.padded_size {
            let chunk = std::cmp::min(header.padded_size - bytes_read, io_buf.capacity as u64) as usize;
            tape.read_exact(&mut io_buf.as_mut_slice()[..chunk])?;
            
            let valid_compressed = if bytes_read + chunk as u64 > header.compressed_size {
                header.compressed_size.saturating_sub(bytes_read) as usize
            } else {
                chunk
            };

            if valid_compressed > 0 { decoder.write_all(&io_buf.as_slice()[..valid_compressed])?; }
            bytes_read += chunk as u64;
        }
        decoder.flush()?;
        decoder.into_inner().finalize_hash()
    } else {
        let mut raw_writer = hash_writer;
        let mut bytes_left_to_write = header.payload_size;
        while bytes_read < header.padded_size {
            let chunk = std::cmp::min(header.padded_size - bytes_read, io_buf.capacity as u64) as usize;
            tape.read_exact(&mut io_buf.as_mut_slice()[..chunk])?;
            
            let write_chunk = std::cmp::min(bytes_left_to_write, chunk as u64) as usize;
            if write_chunk > 0 { raw_writer.write_all(&io_buf.as_slice()[..write_chunk])?; }
            bytes_read += chunk as u64;
            bytes_left_to_write -= write_chunk as u64;
        }
        raw_writer.flush()?;
        raw_writer.finalize_hash()
    };

    // Fetch Real Header for Append-Only media now that payload is consumed
    if tape_dev.starts_with("rclone:") || is_char_dev {
        tape.read_exact(header_buf.as_mut_slice())?;
        header = *bytemuck::from_bytes(header_buf.as_slice());
        
        let stored_crc = header.header_crc32;
        header.header_crc32 = 0;
        let mut crc = Crc32Hasher::new();
        crc.update(bytemuck::bytes_of(&header));
        if crc.finalize() != stored_crc {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Footer Header CRC mismatch!"));
        }
        info!("Footer Header verified! Expected Payload: {} bytes.", header.payload_size);
    }

    if final_hash.as_bytes() != &header.data_checksum {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "BLAKE3 checksum mismatch!"));
    }

    // Unpack Xattrs from TLV
    let mut tlv_offset = 0;
    while tlv_offset + 4 <= header.tlv_data.len() {
        let t_type = u16::from_be_bytes([header.tlv_data[tlv_offset], header.tlv_data[tlv_offset+1]]);
        let t_len = u16::from_be_bytes([header.tlv_data[tlv_offset+2], header.tlv_data[tlv_offset+3]]) as usize;
        if t_type == 0 || tlv_offset + 4 + t_len > header.tlv_data.len() { break; }
        
        if t_type == 0x02 { 
            let payload_start = tlv_offset + 4;
            let name_len = header.tlv_data[payload_start] as usize;
            if name_len > 0 && payload_start + 1 + name_len + 2 <= payload_start + t_len {
                let name = String::from_utf8_lossy(&header.tlv_data[payload_start+1 .. payload_start+1+name_len]).into_owned();
                let val_len_start = payload_start+1+name_len;
                let val_len = u16::from_be_bytes([header.tlv_data[val_len_start], header.tlv_data[val_len_start+1]]) as usize;
                let val_start = val_len_start + 2;
                if val_start + val_len <= payload_start + t_len {
                    let val = &header.tlv_data[val_start .. val_start+val_len];
                    
                    // Look up actual dest path via /proc mapping since we only have the raw_fd
                    let proc_path = format!("/proc/self/fd/{}", dest_fd);
                    if let Ok(real_path) = std::fs::read_link(&proc_path) {
                        let _ = xattr::set(real_path, &name, val);
                    }
                }
            }
        }
        tlv_offset += 4 + t_len;
    }
    
    info!("Restore successful! BLAKE3 Payload Hash verified.");
    Ok(())
}

fn manual_restore(config: &Arc<HuskConfig>, db_path: &str, file_path: &str, dest_path: &str, version: Option<u32>, use_direct_io: bool) -> std::io::Result<()> {
    let conn = Connection::open(db_path).map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "DB open failed"))?;
    
    let mut query = String::from(
        "SELECT t.device_path, c.tape_offset, c.version 
         FROM catalog c 
         JOIN tapes t ON c.tape_uuid = t.tape_uuid 
         WHERE c.file_path = ?1"
    );
    
    let row_res = if let Some(v) = version {
        query.push_str(" AND c.version = ?2 LIMIT 1");
        conn.query_row(&query, params![file_path, v], |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?, row.get::<_, u32>(2)?)))
    } else {
        query.push_str(" ORDER BY c.version DESC LIMIT 1");
        conn.query_row(&query, params![file_path], |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?, row.get::<_, u32>(2)?)))
    };

    match row_res {
        Ok((device_path, offset, found_ver)) => {
            info!(" Found '{}' (Version {}) on Volume '{}' at offset {}", file_path, found_ver, device_path, offset);
            let tmp_dest = format!("{}.husk_tmp", dest_path);
            let file = OpenOptions::new().write(true).create(true).truncate(true).open(&tmp_dest)?;
            let fd = file.as_raw_fd();
            
            match restore_file(config, db_path, &device_path, file_path, fd, offset, use_direct_io, true) {
                Ok(_) => {
                    // Atomically overwrite target file only when fully verified
                    std::fs::rename(&tmp_dest, dest_path)?;
                    info!("✅ Successfully rolled back to '{}'", dest_path);
                    Ok(())
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp_dest);
                    error!("❌ Restore corrupted or failed. Cleaned up temporary file.");
                    Err(e)
                }
            }
        }
        Err(_) => {
            error!("❌ File '{}' (Version: {:?}) not found in catalog.", file_path, version);
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Version not found in catalog."))
        }
    }
}

// ---------------------------------------------------------
// Helper: Recursively apply fanotify marks to subdirectories
// ---------------------------------------------------------
fn mark_directory_recursive(fan_fd: i32, dir: &Path, mask: u64, config: &Arc<HuskConfig>) {
    let path_str = dir.to_str().unwrap_or("");
    if is_path_excluded(path_str, config) {
        return;
    }

    let c_path = CString::new(path_str).unwrap();
    let ret = unsafe {
        libc::fanotify_mark(fan_fd, libc::FAN_MARK_ADD, mask, libc::AT_FDCWD, c_path.as_ptr())
    };

    // If the mark succeeds, walk into its child folders
    if ret >= 0 {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    // Ignore symlinks to prevent infinite recursive loop traps
                    if file_type.is_dir() && !file_type.is_symlink() {
                        mark_directory_recursive(fan_fd, &entry.path(), mask, config);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------
// 5 The "Interceptor" Daemon (Event-Driven fanotify)
// ---------------------------------------------------------
fn run_interceptor(config: Arc<HuskConfig>, use_direct_io: bool) -> std::io::Result<()> {
    let watch_dir = &config.hot_tier;
    let db_path = &config.db_path;
    info!("\n[Daemon] Starting fanotify interceptor on '{}'...", watch_dir);
    let abs_dir = std::fs::canonicalize(watch_dir)?;
    
    let fan_fd = unsafe {
        libc::fanotify_init(libc::FAN_CLASS_PRE_CONTENT, libc::O_RDWR as u32)
    };
    if fan_fd < 0 { 
        let err = std::io::Error::last_os_error();
        error!("❌ fanotify_init failed: {}. Missing Root or Capabilities!", err);
        return Err(err); 
    }

    // CRITICAL FIX: Use FAN_ACCESS_PERM instead of FAN_OPEN_PERM to avoid VFS inode lock deadlocks
        let mark_mask = libc::FAN_ACCESS_PERM | libc::FAN_CLOSE_WRITE | libc::FAN_EVENT_ON_CHILD;
        
        // 1. Recursively mark the root watch directory and all current subdirectories
        info!("[Daemon]  Scanning and attaching listeners to all subdirectories...");
        mark_directory_recursive(fan_fd, &abs_dir, mark_mask, &config);

        info!("\n=======================================================");
        info!("[Daemon] Listening for File Reads & Modifies...");
        info!("=======================================================\n");

        // 2. Background thread to periodically re-mark dynamically created subdirectories
        let bg_fan_fd = fan_fd;
        let bg_watch_dir = abs_dir.clone();
        let config_clone = Arc::clone(&config);
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(15)); // Rescan every 15s for new folders
                mark_directory_recursive(bg_fan_fd, &bg_watch_dir, mark_mask, &config_clone);
            }
        });

        let conn = Connection::open(db_path).unwrap();
        let active_restores = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let mut buf = [0u8; 4096];

    loop {
        let n = unsafe { libc::read(fan_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n <= 0 { continue; }

        let mut offset = 0;
        while offset < n as usize {
            let metadata_ptr = unsafe { buf.as_ptr().add(offset) as *const libc::fanotify_event_metadata };
            let metadata = unsafe { &*metadata_ptr };
            
            // Ignore events triggered by the USTD daemon itself
            if metadata.pid == unsafe { libc::getpid() } {
                if metadata.fd >= 0 {
                    if (metadata.mask as u64 & libc::FAN_ACCESS_PERM) != 0 {
                        let response = libc::fanotify_response { fd: metadata.fd, response: libc::FAN_ALLOW };
                        unsafe { libc::write(fan_fd, &response as *const _ as *const libc::c_void, std::mem::size_of_val(&response)); }
                    }
                    unsafe { libc::close(metadata.fd); }
                }
                offset += metadata.event_len as usize;
                continue;
            }
            
            let event_len = metadata.event_len as usize;
            if metadata.vers != libc::FANOTIFY_METADATA_VERSION as u8 { break; }

            if metadata.fd >= 0 {
                let fd_raw = metadata.fd;
                let proc_path = format!("/proc/self/fd/{}", fd_raw);
                let mask = metadata.mask as u64;
                
                if let Ok(real_path) = std::fs::read_link(&proc_path) {
                    let path_str = real_path.to_string_lossy().to_string();

                    // EVENT 1: Process is trying to READ a file
                    if (mask & libc::FAN_ACCESS_PERM) != 0 {
                        // FIX: Only query xattr (disk access) on READ events. 
                        let is_stubbed = xattr::get(&path_str, "trusted.husk.status")
                            .map(|v| v == Some(b"stubbed".to_vec()))
                            .unwrap_or(false);

                        if is_stubbed {
                            // FAST PATH: If the file is being opened strictly to OVERWRITE it (O_WRONLY / O_TRUNC),
                            // do NOT pull it from tape. Just drop the stub status and allow the overwrite instantly.
                            let flags = unsafe { libc::fcntl(fd_raw, libc::F_GETFL) };
                            let is_trunc = (flags & libc::O_TRUNC) != 0;

                            // BUG FIX: O_WRONLY on its own (like Python's "a" for append) is NOT safe to bypass,
                            // because we still need the original data to append to! Only bypass if explicitly truncating.
                            if is_trunc {
                                info!("[Daemon] Fast-Path Bypass: '{}' opened for truncation. Skipping Volume restore.", path_str);
                                let _ = xattr::remove(&path_str, "trusted.husk.status");
                                
                                let response = libc::fanotify_response { fd: fd_raw, response: libc::FAN_ALLOW };
                                unsafe { 
                                    libc::write(fan_fd, &response as *const _ as *const libc::c_void, std::mem::size_of_val(&response)); 
                                    libc::close(fd_raw);
                                }
                                
                                offset += event_len;
                                continue; // Skip the tape restore thread entirely
                            }

                            // Find out WHICH process is reading the file
                            let pid = metadata.pid;
                            let mut proc_name = String::new();
                            if let Ok(comm) = std::fs::read_to_string(format!("/proc/{}/comm", pid)) {
                                proc_name = comm.trim().to_string();
                            }

                            // Prevent aggressive background indexers from unintentionally restoring files
                            let ignore_list = ["rg", "code", "node", "git", "grep", "tracker-miner-f"];
                            if ignore_list.contains(&proc_name.as_str()) {
                                info!("[Daemon]  Ignoring background read from '{}' to keep file stubbed.", proc_name);
                            } else {
                                info!("\n[Daemon] INTERCEPTED READ ON STUB: {} (Triggered by PID {}: {})", path_str, pid, proc_name);
                                // 1. Gather Replicas synchronously
                                let mut stmt = conn.prepare(
                                    "SELECT t.device_path, c.tape_offset 
                                     FROM catalog c 
                                     JOIN tapes t ON c.tape_uuid = t.tape_uuid 
                                     WHERE c.file_path = ?1 AND c.version = (SELECT MAX(version) FROM catalog WHERE file_path = ?1)"
                                ).unwrap();
                                let mut rows = stmt.query(params![path_str]).unwrap();
                                let mut replicas = Vec::new();
                                while let Some(row) = rows.next().unwrap() {
                                    replicas.push((row.get::<_, String>(0).unwrap(), row.get::<_, u64>(1).unwrap()));
                                }

                                // 2. Dispatch restoration to thread pool to prevent blocking UI loops
                                let path_clone = path_str.clone();
                                let db_path_clone = db_path.to_string();
                                let active_restores_clone = Arc::clone(&active_restores);
                                let interceptor_config = Arc::clone(&config);
                                
                                thread::spawn(move || {
                                    let mut is_primary = false;
                                    {
                                        let mut restores = active_restores_clone.lock().unwrap();
                                        if !restores.contains(&path_clone) {
                                            restores.insert(path_clone.clone());
                                            is_primary = true;
                                        }
                                    }

                                    if is_primary {
                                        // 1. Capture original timestamps BEFORE restoring
                                        let (atime_sec, atime_nsec, mtime_sec, mtime_nsec) = if let Ok(meta) = std::fs::metadata(&path_clone) {
                                            (meta.atime(), meta.atime_nsec(), meta.mtime(), meta.mtime_nsec())
                                        } else {
                                            (0, 0, 0, 0)
                                        };

                                        let mut restored = false;
                                        for (db_tape, db_offset) in replicas {
                                            info!("[Daemon] Trying to wake Volume replica at '{}'...", db_tape);
                                            if let Err(e) = restore_file(&interceptor_config, &db_path_clone, &db_tape, &path_clone, fd_raw, db_offset, use_direct_io, false) {
                                                error!("[Daemon] ⚠️ Replica at '{}' unavailable: {}. Trying next...", db_tape, e);
                                            } else {
                                                restored = true;
                                                break;
                                            }
                                        }

                                        if restored {
                                            let _ = xattr::remove(&path_clone, "trusted.husk.status");
                                            if let Ok(t_conn) = Connection::open(&db_path_clone) {
                                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                                                let _ = t_conn.execute("INSERT OR REPLACE INTO active_tracking (file_path, last_touch) VALUES (?1, ?2)", params![path_clone, now]);
                                            }
                                            
                                            // 2. Hide the restore modification from file watchers and editors
                                            if mtime_sec != 0 {
                                                let c_path = CString::new(path_clone.as_str()).unwrap();
                                                let times = [
                                                    libc::timespec { tv_sec: atime_sec as libc::time_t, tv_nsec: atime_nsec as libc::c_long },
                                                    libc::timespec { tv_sec: mtime_sec as libc::time_t, tv_nsec: mtime_nsec as libc::c_long },
                                                ];
                                                unsafe {
                                                    libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0);
                                                }
                                            }
                                            
                                            info!("[Daemon] ✅ Restore complete for: {}", path_clone);
                                        } else {
                                            error!("[Daemon] CRITICAL: All replicas for '{}' offline!", path_clone);
                                            if let Ok(meta) = std::fs::metadata(&path_clone) {
                                                let _ = stub_file(&path_clone, meta.len());
                                            }
                                        }

                                        {
                                            let mut restores = active_restores_clone.lock().unwrap();
                                            restores.remove(&path_clone);
                                        }
                                    } else {
                                        // Secondary read request. Wait for primary to finish restoring the file.
                                        loop {
                                            {
                                                let restores = active_restores_clone.lock().unwrap();
                                                if !restores.contains(&path_clone) { break; }
                                            }
                                            thread::sleep(Duration::from_millis(50));
                                        }
                                    }

                                    // Release the blocked process ONLY after restore is done
                                    let response = libc::fanotify_response { fd: fd_raw, response: libc::FAN_ALLOW };
                                    unsafe { 
                                        libc::write(fan_fd, &response as *const _ as *const libc::c_void, std::mem::size_of_val(&response)); 
                                        libc::close(fd_raw); 
                                    }
                                });
                                
                                offset += event_len;
                                continue; // Skip default loop closures; the thread owns the FD now
                            } // Closes `else`
                        } // Closes `if is_stubbed`
                        
                        // MUST respond ALLOW so ignored indexers read safe zeroes instead of locking up
                        let response = libc::fanotify_response { fd: fd_raw, response: libc::FAN_ALLOW };
                        unsafe { libc::write(fan_fd, &response as *const _ as *const libc::c_void, std::mem::size_of_val(&response)); }
                    }

                    // EVENT 2: Process finished WRITING to a file
                    if (mask & libc::FAN_CLOSE_WRITE) != 0 {
                        // ZERO-LOCK DEBOUNCING: Identify directories via RAM mask. NO xattr or metadata checks here!
                        // This prevents Samba/SMB/gedit Sharing Violations during atomic temporary saves.
                        // Ghost files (.goutputstream) are blindly queued in O(1) time and self-clean in the Janitor.
                        let is_dir = (mask & libc::FAN_ONDIR as u64) != 0;
                        if !is_dir && !is_path_excluded(&path_str, &config) {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                            if let Err(e) = conn.execute(
                                "INSERT OR REPLACE INTO active_tracking (file_path, last_touch) VALUES (?1, ?2)",
                                params![path_str, now],
                            ) {
                                error!("[Daemon] ❌ Failed to track modified file {}: {}", path_str, e);
                            } else {
                                info!("[Daemon] Tracked modified file: {}", path_str);
                            }
                        }
                    }
                }
                unsafe { libc::close(fd_raw); }
            }
            offset += event_len;
        }
    }
    // Note: This function runs in an infinite loop and is terminated via signal handler
}

// ---------------------------------------------------------
// 6. The "Janitor" (Database-Driven Policy Engine)
// ---------------------------------------------------------

// Worker thread: Sits in the background, receives files via a queue, and uploads them.
fn run_archive_worker(rx: mpsc::Receiver<String>, config: Arc<HuskConfig>, use_direct_io: bool) {
    let conn = Connection::open(&config.db_path).expect("Worker failed to open catalog");
    let mut archived_since_last_mirror = 0;
    
    loop {
        // Wait for a file in the queue. If idle for 5 seconds, perform maintenance (DB Mirroring).
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(path_str) => {
                let meta = match std::fs::metadata(&path_str) {
                    Ok(m) => m,
                    Err(_) => {
                        let _ = conn.execute("DELETE FROM active_tracking WHERE file_path = ?1", params![path_str]);
                        continue;
                    }
                };

                if meta.is_dir() || is_path_excluded(&path_str, &config) {
                    let _ = conn.execute("DELETE FROM active_tracking WHERE file_path = ?1", params![path_str]);
                    continue;
                }

                let is_stubbed = xattr::get(&path_str, "trusted.husk.status")
                    .map(|v| v == Some(b"stubbed".to_vec()))
                    .unwrap_or(false);

                if !is_stubbed {
                    info!("[Worker] Processing Cold File: {}", path_str);
                    match archive_file(&conn, &path_str, &config, use_direct_io) {
                        Ok((replica_list, jump_table)) => {
                            let next_version: i64 = conn.query_row(
                                "SELECT COALESCE(MAX(version), 0) + 1 FROM catalog WHERE file_path = ?1",
                                params![path_str], |row| row.get(0),
                            ).unwrap_or(1);

                            let mut payload_size_saved = 0;
                            for (offset, size, comp_size, comp_type, hash, tape_uuid, dev_path) in replica_list {
                                payload_size_saved = size;
                                let drive_serial = get_drive_serial(&dev_path);
                                
                                let _ = conn.execute(
                                    "INSERT OR REPLACE INTO tapes (tape_uuid, device_path, drive_serial) VALUES (?1, ?2, ?3)",
                                    params![tape_uuid, dev_path, drive_serial],
                                );
                                let _ = conn.execute(
                                    "INSERT INTO catalog (file_path, version, tape_uuid, tape_offset, payload_size, compressed_size, compression_type, uid, gid, posix_mode, original_mtime, blake3_hash) 
                                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                                    params![path_str, next_version, tape_uuid, offset, size, comp_size, comp_type, meta.uid(), meta.gid(), meta.mode(), meta.mtime(), hash],
                                );
                            }

                            //  Store Jump Table for StreamGate
                            for (u_off, c_off, c_size) in jump_table {
                                let _ = conn.execute(
                                    "INSERT INTO object_frames (file_path, version, uncompressed_offset, compressed_offset, compressed_size) VALUES (?1, ?2, ?3, ?4, ?5)",
                                    params![path_str, next_version, u_off, c_off, c_size]
                                );
                            }

                            let _ = conn.execute(
                                "DELETE FROM catalog WHERE file_path = ?1 AND version NOT IN (
                                    SELECT version FROM catalog WHERE file_path = ?1 ORDER BY version DESC LIMIT ?2
                                )", params![path_str, config.max_versions]
                            );
                            
                            // Cleanup old StreamGate frames
                            let _ = conn.execute(
                                "DELETE FROM object_frames WHERE file_path = ?1 AND version NOT IN (
                                    SELECT version FROM catalog WHERE file_path = ?1 ORDER BY version DESC LIMIT ?2
                                )", params![path_str, config.max_versions]
                            );
                            
                            // ---------------------------------------------------------
                            // POST-COMMIT CATALOG TRIGGER (Multi-Node Global Sync)
                            // ---------------------------------------------------------
                            let sidecar = SidecarBridge::new(&config);
                            sidecar.send_event(json!({
                                "event": "CATALOG_UPDATE", 
                                "table": "catalog",
                                "file_path": path_str,
                                "version": next_version
                            }));

                            if let Err(e) = stub_file(&path_str, payload_size_saved) {
                                error!("[Worker] ❌ Failed to stub {}: {}", path_str, e);
                            } else {
                                conn.execute("DELETE FROM active_tracking WHERE file_path = ?1", params![path_str]).unwrap();
                                info!("[Worker] ✅ Stubbed and removed from queue.");
                                archived_since_last_mirror += 1;
                            }
                        },
                        Err(e) => error!("[Worker] ❌ Failed to archive {}: {}", path_str, e),
                    }
                } else {
                    conn.execute("DELETE FROM active_tracking WHERE file_path = ?1", params![path_str]).unwrap();
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Idle Time! If we just finished archiving files, mirror the database.
                if archived_since_last_mirror > 0 {
                    info!("[Worker] Queue idle. Mirroring Catalog to primary storage (Database Anchor)...");
                    let backup_path = format!("{}_backup.db", config.db_path);
                    let _ = std::fs::remove_file(&backup_path);
                    
                    if conn.execute(&format!("VACUUM INTO '{}'", backup_path), []).is_ok() {
                        if let Ok((results, _)) = archive_file(&conn, &backup_path, &config, use_direct_io) {
                            let special_path = "__HUSK_CATALOG_BACKUP__";
                            let next_ver: i64 = conn.query_row(
                                "SELECT COALESCE(MAX(version), 0) + 1 FROM catalog WHERE file_path = ?1",
                                params![special_path], |row| row.get(0),
                            ).unwrap_or(1);
                            
                            for (offset, size, comp_size, comp_type, hash, tape_uuid, _) in results {
                                let _ = conn.execute(
                                    "INSERT INTO catalog (file_path, version, tape_uuid, tape_offset, payload_size, compressed_size, compression_type, uid, gid, posix_mode, original_mtime, blake3_hash) 
                                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                                    params![special_path, next_ver, tape_uuid, offset, size, comp_size, comp_type, 0, 0, 0644, 0, hash],
                                );
                            }
                            info!("[Worker] ✅ Catalog securely mirrored.");
                        } else {
                            error!("[Worker] ❌ Failed to write Catalog Mirror.");
                        }
                        let _ = std::fs::remove_file(&backup_path);
                    }
                    archived_since_last_mirror = 0;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

// Scanner thread: Instantly queries the DB and populates the queue without blocking.
fn run_janitor_scanner(tx: mpsc::SyncSender<String>, config: Arc<HuskConfig>) {
    let conn = Connection::open(&config.db_path).unwrap();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let max_age_secs = config.max_age_days * 24 * 3600; 

    let mut stmt = conn.prepare("SELECT file_path, last_touch FROM active_tracking").unwrap();
    let rows: Vec<(String, u64)> = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap().filter_map(Result::ok).collect();

    for (path_str, last_touch) in rows {
        let path = Path::new(&path_str);
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        
        let is_immediate_ext = config.immediate_archive_extensions.iter().any(|e| e == ext);
        let is_immediate_dir = config.immediate_archive_dirs.iter().any(|d| path_str.contains(d));
        let is_immediate = is_immediate_ext || is_immediate_dir;
        
        let age_secs = now.saturating_sub(last_touch);

        if is_immediate || age_secs >= max_age_secs {
            if let Err(mpsc::TrySendError::Full(_)) = tx.try_send(path_str) {
                info!("[Janitor] Queue full, will retry later.");
                break; 
            }
        }
    }
}
// ---------------------------------------------------------
// 7. Disaster Recovery: Deep Scan Rebuild
// ---------------------------------------------------------
fn rebuild_catalog(tape_dev: &str, db_path: &str, use_direct_io: bool) -> std::io::Result<()> {
    info!("Initiating Deep Scan Recovery on Volume: {}...", tape_dev);
    
    // Initialize a fresh catalog
    let conn = init_catalog(db_path).expect("Failed to create recovery DB");
    
    let mut tape = open_tape_device(tape_dev, true, false, false, use_direct_io)?;
    
    let mut offset: u64 = 0;
    let mut recovered_count = 0;
    let mut header_buf = AlignedBuffer::new(ALIGNMENT);

    loop {
        if tape.seek(SeekFrom::Start(offset)).is_err() { break; }
        
        let bytes_read = tape.read(header_buf.as_mut_slice()).unwrap_or(0);
        if bytes_read < ALIGNMENT { break; } // EOF reached
        
        let header: ObjectHeader = *bytemuck::from_bytes(header_buf.as_slice());
        
        // Look for magic bytes
        if header.magic_bytes == *b"USTD\x00\x01\x02\x03" {
            // Reconstruct filename from TLV
            let filename = if header.tlv_data[0] == 0x00 && header.tlv_data[1] == 0x01 {
                let name_len = u16::from_be_bytes([header.tlv_data[2], header.tlv_data[3]]) as usize;
                String::from_utf8_lossy(&header.tlv_data[4..4 + name_len]).into_owned()
            } else {
                format!("recovered_file_{}", offset)
            };

            let hash_hex = header.data_checksum.iter().map(|b| format!("{:02x}", b)).collect::<String>();

            info!("✅ Recovered: '{}' (Offset: {}, Size: {})", filename, offset, header.payload_size);
            
            // Re-insert into database
            let _ = conn.execute(
                "INSERT OR REPLACE INTO catalog (file_path, version, tape_uuid, tape_offset, payload_size, compressed_size, compression_type, uid, gid, posix_mode, original_mtime, blake3_hash) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![&filename, 1, tape_dev, offset, header.payload_size, header.compressed_size, header.compression_type, header.uid, header.gid, header.posix_mode, header.mtime, hash_hex],
            );
            
            //  Recover StreamGate Jump Table from TLV
            let mut tlv_offset = 0;
            while tlv_offset + 4 <= header.tlv_data.len() {
                let t_type = u16::from_be_bytes([header.tlv_data[tlv_offset], header.tlv_data[tlv_offset+1]]);
                let t_len = u16::from_be_bytes([header.tlv_data[tlv_offset+2], header.tlv_data[tlv_offset+3]]) as usize;
                if t_type == 0 || tlv_offset + 4 + t_len > header.tlv_data.len() { break; }
                
                if t_type == 0x03 {
                    let frame_count = t_len / 4;
                    let mut p = tlv_offset + 4;
                    let mut u_off: u64 = 0;
                    let mut c_off: u64 = 0;
                    
                    for _ in 0..frame_count {
                        if p + 4 > header.tlv_data.len() { break; }
                        let c_size = u32::from_be_bytes([header.tlv_data[p], header.tlv_data[p+1], header.tlv_data[p+2], header.tlv_data[p+3]]) as u64;
                        
                        let _ = conn.execute(
                            "INSERT INTO object_frames (file_path, version, uncompressed_offset, compressed_offset, compressed_size) VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![&filename, 1, u_off, c_off, c_size]
                        );
                        
                        u_off += 16 * 1024 * 1024; // 16MB steps
                        c_off += c_size;
                        p += 4;
                    }
                }
                tlv_offset += 4 + t_len;
            }

            recovered_count += 1;
            
            // Jump forward by the padded size + the header size
            offset += (ALIGNMENT as u64) + header.padded_size;
        } else {
            // Not a header. Move forward 4KB and try again (Scan mode).
            offset += ALIGNMENT as u64;
        }
    }
    
    info!(" Recovery Complete! Successfully rebuilt {} entries into {}.", recovered_count, db_path);
    Ok(())
}

// ---------------------------------------------------------
// 7.5 The Repacker: Tape Garbage Collection 
// ---------------------------------------------------------
fn repack_tape(db_path: &str, source_dev: &str, dest_dev: &str, use_direct_io: bool) -> std::io::Result<()> {
    let src_is_char = std::fs::metadata(source_dev).map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR).unwrap_or(false);
    let dest_is_char = std::fs::metadata(dest_dev).map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR).unwrap_or(false);
    
    if source_dev.starts_with("rclone:") || dest_dev.starts_with("rclone:") || src_is_char || dest_is_char {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Repacking Append-Only/Cloud volumes is not yet supported."));
    }
    info!(" Starting Repacker: Moving active data from '{}' to '{}'...", source_dev, dest_dev);
    
    let conn = Connection::open(db_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    // 1. Check Source Volume
    let mut src_tape = open_tape_device(source_dev, true, false, false, use_direct_io)?;
    let mut vol_buf = AlignedBuffer::new(ALIGNMENT);
    src_tape.read_exact(vol_buf.as_mut_slice())?;
    let src_vol: VolumeHeader = *bytemuck::from_bytes(vol_buf.as_slice());
    let src_uuid_hex = src_vol.volume_uuid.iter().map(|b| format!("{:02x}", b)).collect::<String>();
    
    // 2. Open/Format Destination Volume
    let mut dest_tape = open_tape_device(dest_dev, true, true, true, use_direct_io)?;
    let dest_meta = dest_tape.metadata()?;
    if dest_meta.len() < ALIGNMENT as u64 {
        format_tape(dest_dev, use_direct_io)?;
    }
    
    dest_tape.seek(SeekFrom::Start(0))?;
    dest_tape.read_exact(vol_buf.as_mut_slice())?;
    let dest_vol: VolumeHeader = *bytemuck::from_bytes(vol_buf.as_slice());
    let dest_uuid_hex = dest_vol.volume_uuid.iter().map(|b| format!("{:02x}", b)).collect::<String>();
    
    // 3. Find latest versions of all files on the source tape
    let query = "
        SELECT id, file_path, tape_offset, compressed_size 
        FROM catalog c1 
        WHERE tape_uuid = ?1 
          AND version = (SELECT MAX(version) FROM catalog c2 WHERE c1.file_path = c2.file_path)
        ORDER BY tape_offset ASC
    ";
    
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = stmt.query(params![src_uuid_hex]).unwrap();
    
    let mut moved_count = 0;
    let mut dest_offset: u64 = conn.query_row(
        "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog WHERE tape_uuid = ?1",
        params![dest_uuid_hex], |row| row.get::<_, i64>(0)
    ).unwrap_or(4096) as u64;
    dest_offset = if dest_offset % ALIGNMENT as u64 == 0 { dest_offset } else { dest_offset + ALIGNMENT as u64 - (dest_offset % ALIGNMENT as u64) };

    let mut io_buf = AlignedBuffer::new(ALIGNMENT * 256); // 1MB zero-copy buffer

    while let Some(row) = rows.next().unwrap() {
        let id: i64 = row.get(0).unwrap();
        let path: String = row.get(1).unwrap();
        let src_offset: u64 = row.get(2).unwrap();
        let compressed_size: u64 = row.get(3).unwrap();
        let padded_size: u64 = ((compressed_size + 4095) / 4096) * 4096;
        
        // Read Source Header
        src_tape.seek(SeekFrom::Start(src_offset))?;
        let mut header_buf = AlignedBuffer::new(ALIGNMENT);
        src_tape.read_exact(header_buf.as_mut_slice())?;
        
        let mut header: ObjectHeader = *bytemuck::from_bytes(header_buf.as_slice());
        
        // Update header for new volume
        header.tape_uuid = dest_vol.volume_uuid;
        header.header_crc32 = 0;
        let mut crc = Crc32Hasher::new();
        crc.update(bytemuck::bytes_of(&header));
        header.header_crc32 = crc.finalize();
        
        // Write Header to Dest
        dest_tape.seek(SeekFrom::Start(dest_offset))?;
        header_buf.as_mut_slice().copy_from_slice(bytemuck::bytes_of(&header));
        dest_tape.write_all(header_buf.as_slice())?;
        
        // Fast-copy Compressed Zstd Payload (Zero-Decompression)
        let mut bytes_left = padded_size;
        while bytes_left > 0 {
            let chunk = std::cmp::min(bytes_left, io_buf.capacity as u64) as usize;
            src_tape.read_exact(&mut io_buf.as_mut_slice()[..chunk])?;
            dest_tape.write_all(&io_buf.as_slice()[..chunk])?;
            bytes_left -= chunk as u64;
        }
        
        // Ensure new Tape is listed in pool
        conn.execute("INSERT OR IGNORE INTO tapes (tape_uuid, device_path) VALUES (?1, ?2)", params![dest_uuid_hex, dest_dev]).unwrap();

        // Update Catalog pointing to the new tape
        conn.execute(
            "UPDATE catalog SET tape_uuid = ?1, tape_offset = ?2 WHERE id = ?3",
            params![dest_uuid_hex, dest_offset, id]
        ).unwrap();
        
        moved_count += 1;
        dest_offset += ALIGNMENT as u64 + padded_size;
        info!("   ✅ Repacked: {}", path);
    }
    
    // Purge old wasteland records from DB
    let pruned = conn.execute("DELETE FROM catalog WHERE tape_uuid = ?1", params![src_uuid_hex]).unwrap();
    info!("Repack Complete! Moved {} active objects. Erased {} deleted/old entries.", moved_count, pruned);
    info!(" You may now safely format or delete the physical drive: '{}'.", source_dev);
    
    Ok(())
}

// ---------------------------------------------------------
// 8. Data Integrity: The Scrubber 
// ---------------------------------------------------------
fn scrub_tape(tape_dev: &str, db_path: &str, use_direct_io: bool) -> std::io::Result<()> {
    let is_char_dev = std::fs::metadata(tape_dev).map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR).unwrap_or(false);
    if tape_dev.starts_with("rclone:") || is_char_dev {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Scrubbing Append-Only/Cloud volumes is not yet supported."));
    }
    info!(" Starting Scrubber on Volume: {}...", tape_dev);
    
    let conn = Connection::open(db_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    let mut tape = open_tape_device(tape_dev, true, false, false, use_direct_io)?;
    tape.seek(SeekFrom::Start(0))?;
    
    let mut vol_buf = AlignedBuffer::new(ALIGNMENT);
    tape.read_exact(vol_buf.as_mut_slice())?;
    let vol_header: VolumeHeader = *bytemuck::from_bytes(vol_buf.as_slice());
    
    if &vol_header.magic_bytes != b"USTDVOL\0" {
        error!("❌ Invalid Volume Header on {}. Cannot scrub.", tape_dev);
        return Ok(());
    }
    
    let tape_uuid_hex = vol_header.volume_uuid.iter().map(|b| format!("{:02x}", b)).collect::<String>();

    let mut stmt = conn.prepare(
        "SELECT file_path, tape_offset, blake3_hash 
         FROM catalog WHERE tape_uuid = ?1 ORDER BY tape_offset ASC"
    ).unwrap();
    
    let mut rows = stmt.query(params![tape_uuid_hex]).unwrap();
    
    let mut total_checked = 0;
    let mut total_corrupted = 0;
    let mut io_buf = AlignedBuffer::new(ALIGNMENT * 256);

    info!(" Scanning valid objects from database index...");

    while let Some(row) = rows.next().unwrap() {
        let file_path: String = row.get(0).unwrap();
        let offset: u64 = row.get(1).unwrap();
        let expected_hash_hex: String = row.get(2).unwrap();
        
        total_checked += 1;
        
        if tape.seek(SeekFrom::Start(offset)).is_err() {
            error!("❌ [CORRUPT] Seek error at offset {} for '{}'", offset, file_path);
            total_corrupted += 1;
            continue;
        }
        
        let mut header_buf = AlignedBuffer::new(ALIGNMENT);
        if tape.read_exact(header_buf.as_mut_slice()).is_err() {
            error!("❌ [CORRUPT] Read error at offset {} for '{}'", offset, file_path);
            total_corrupted += 1;
            continue;
        }
        
        let mut header: ObjectHeader = *bytemuck::from_bytes(header_buf.as_slice());
        let stored_crc = header.header_crc32;
        header.header_crc32 = 0;
        let mut crc = Crc32Hasher::new();
        crc.update(bytemuck::bytes_of(&header));
        if crc.finalize() != stored_crc {
            error!("❌ [CORRUPT] Header CRC mismatch for '{}' at offset {}", file_path, offset);
            total_corrupted += 1;
            continue;
        }

        // Use std::io::sink() so we don't consume memory/disk while decoding
        let hash_writer = HashWriter { inner: std::io::sink(), hasher: Hasher::new() };
        let mut bytes_read: u64 = 0;

        let final_hash_res = if header.compression_type == 1 {
            let mut decoder = match zstd::stream::write::Decoder::new(hash_writer) {
                Ok(d) => d,
                Err(_) => {
                    error!("❌ [CORRUPT] Zstd init failed for '{}'", file_path);
                    total_corrupted += 1;
                    continue;
                }
            };
            let mut decode_ok = true;
            while bytes_read < header.padded_size {
                let chunk = std::cmp::min(header.padded_size - bytes_read, io_buf.capacity as u64) as usize;
                if tape.read_exact(&mut io_buf.as_mut_slice()[..chunk]).is_err() {
                    decode_ok = false; break;
                }
                let valid_compressed = if bytes_read + chunk as u64 > header.compressed_size {
                    header.compressed_size.saturating_sub(bytes_read) as usize
                } else { chunk };

                if valid_compressed > 0 { 
                    if decoder.write_all(&io_buf.as_slice()[..valid_compressed]).is_err() {
                        decode_ok = false; break;
                    }
                }
                bytes_read += chunk as u64;
            }
            if !decode_ok || decoder.flush().is_err() { Err(()) } else { Ok(decoder.into_inner().finalize_hash()) }
        } else {
            let mut raw_writer = hash_writer;
            let mut bytes_left_to_write = header.payload_size;
            let mut read_ok = true;
            while bytes_read < header.padded_size {
                let chunk = std::cmp::min(header.padded_size - bytes_read, io_buf.capacity as u64) as usize;
                if tape.read_exact(&mut io_buf.as_mut_slice()[..chunk]).is_err() {
                    read_ok = false; break;
                }
                let write_chunk = std::cmp::min(bytes_left_to_write, chunk as u64) as usize;
                if write_chunk > 0 && raw_writer.write_all(&io_buf.as_slice()[..write_chunk]).is_err() {
                    read_ok = false; break;
                }
                bytes_read += chunk as u64;
                bytes_left_to_write -= write_chunk as u64;
            }
            if !read_ok || raw_writer.flush().is_err() { Err(()) } else { Ok(raw_writer.finalize_hash()) }
        };

        match final_hash_res {
            Ok(hash) => {
                if hash.to_hex().to_string() != expected_hash_hex {
                    error!("❌ [CORRUPT] BLAKE3 mismatch for '{}' (Offset: {})", file_path, offset);
                    total_corrupted += 1;
                } else if total_checked % 50 == 0 {
                    info!("   ... {} objects scrubbed, 0 errors so far ...", total_checked);
                }
            }
            Err(_) => {
                error!("❌ [CORRUPT] Data stream error for '{}' (Offset: {})", file_path, offset);
                total_corrupted += 1;
            }
        }
    }
    
    info!("Scrub Complete! Checked: {}, Corrupted: {}", total_checked, total_corrupted);
    if total_corrupted == 0 && total_checked > 0 { info!("✅ Volume is 100% HEALTHY."); } 
    else if total_checked == 0 { info!("⚠️ Volume index is empty."); }
    
    Ok(())
}

// ---------------------------------------------------------
// Helper: Auto-Discover Moved Tape Drives
// ---------------------------------------------------------
fn rescan_tape_drives(conn: &Connection) {
    info!(" Scanning for physically moved Volumes...");
    let mut stmt = conn.prepare("SELECT tape_uuid, drive_serial, device_path FROM tapes WHERE drive_serial != 'VIRTUAL_IMAGE'").unwrap();
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
    }).unwrap().filter_map(Result::ok);

    for (uuid, serial, old_path) in rows {
        let mut found = false;
        
        // 1. Scan Block Devices (HDDs / SSDs / USBs)
        if let Ok(entries) = std::fs::read_dir("/sys/block") {
            for entry in entries.flatten() {
                let dev_name = entry.file_name().to_string_lossy().to_string();
                if dev_name.starts_with("loop") || dev_name.starts_with("ram") { continue; }
                
                let sys_path = format!("/sys/block/{}/device/serial", dev_name);
                if let Ok(current_serial) = std::fs::read_to_string(&sys_path) {
                    if current_serial.trim() == serial {
                        let new_path = format!("/dev/{}", dev_name);
                        if new_path != old_path {
                            info!("Volume Moved! UUID {} is now safely tracked at {}", uuid, new_path);
                            conn.execute("UPDATE tapes SET device_path = ?1 WHERE tape_uuid = ?2", params![new_path, uuid]).unwrap();
                        }
                        found = true;
                        break;
                    }
                }
            }
        }

        // 2. Scan SCSI Tape Drives if not found in block
        if !found {
            if let Ok(entries) = std::fs::read_dir("/sys/class/scsi_tape") {
                for entry in entries.flatten() {
                    let dev_name = entry.file_name().to_string_lossy().to_string();
                    let sys_path = format!("/sys/class/scsi_tape/{}/device/model", dev_name);
                    if let Ok(current_serial) = std::fs::read_to_string(&sys_path) {
                        if current_serial.trim() == serial {
                            let new_path = format!("/dev/{}", dev_name);
                            if new_path != old_path {
                                info!("Tape Drive Moved! UUID {} is now safely tracked at {}", uuid, new_path);
                                conn.execute("UPDATE tapes SET device_path = ?1 WHERE tape_uuid = ?2", params![new_path, uuid]).unwrap();
                            }
                            found = true;
                            break;
                        }
                    }
                }
            }
        }

        if !found {
            error!("⚠️ Drive {} (Serial/Model: {}) is OFFLINE. Restores from it will fail.", old_path, serial);
        }
    }
}

// ---------------------------------------------------------
// HTTP Streaming Gateway (VLC / Plex Bridge)
// ---------------------------------------------------------
fn handle_http_client(mut stream: TcpStream, config: Arc<HuskConfig>, use_direct_io: bool) {
    // VITAL: Prevent zombie threads if a client network drops
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(15)));
    
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    
    if reader.read_line(&mut request_line).is_err() || request_line.is_empty() { return; }
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    
    // VITAL: Support HTTP HEAD requests used by Plex/VLC for probing file sizes
    if parts.len() < 2 || (parts[0] != "GET" && parts[0] != "HEAD") { return; }
    let is_head_request = parts[0] == "HEAD";

    // 1. Map URL to Local Path securely
    let url_path = parts[1].replace("%20", " "); 
    if url_path.contains("..") { return; } // Requirement 4: Prevent Path Traversal
    
    let clean_path = url_path.strip_prefix("/stream/").unwrap_or_else(|| url_path.strip_prefix("/").unwrap_or(&url_path));
    let file_path_obj = Path::new(&config.hot_tier).join(clean_path);
    
    let abs_path = match file_path_obj.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n");
            return;
        }
    };
    
    let path_str = abs_path.to_string_lossy().to_string();

    // 2. Parse 'Range' Header for media players (RFC 7233)
    let mut range_start: u64 = 0;
    let mut range_end: Option<u64> = None;
    
    loop {
        let mut header_line = String::new();
        if reader.read_line(&mut header_line).is_err() || header_line == "\r\n" { break; }
        if header_line.to_lowercase().starts_with("range: bytes=") {
            let range_val = header_line[13..].trim();
            let split: Vec<&str> = range_val.split('-').collect();
            if !split.is_empty() {
                range_start = split[0].parse().unwrap_or(0);
                if split.len() > 1 && !split[1].is_empty() {
                    range_end = Some(split[1].parse().unwrap());
                }
            }
        }
    }

    // 3. Database Lookup: Fetch Logical Size AND Target Device Path
    let conn = rusqlite::Connection::open(&config.db_path).unwrap();
    let db_res: Result<(u64, String), _> = conn.query_row(
        "SELECT c.payload_size, t.device_path 
         FROM catalog c 
         JOIN tapes t ON c.tape_uuid = t.tape_uuid 
         WHERE c.file_path = ?1 ORDER BY c.version DESC LIMIT 1",
        rusqlite::params![path_str],
        |row| Ok((row.get(0)?, row.get(1)?))
    );

    let (total_size, device_path) = match db_res {
        Ok(res) => res,
        Err(_) => {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n");
            return;
        }
    };

    // 3.5 Hardware Safety Check (Requirement 4: Graceful Backend Handling / 503)
    if !device_path.starts_with("rclone:") {
        if !std::path::Path::new(&device_path).exists() {
            error!("[Gateway] Backend volume offline or unreachable: {}", device_path);
            let _ = stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\n\r\n");
            return;
        }
    }

    let end = range_end.unwrap_or(total_size.saturating_sub(1));
    let length = end - range_start + 1;

    // 4. Content-Type Detection
    let ext = std::path::Path::new(&path_str).extension().unwrap_or_default().to_string_lossy().to_lowercase();
    let mime = match ext.as_ref() {
        "mp4" => "video/mp4", "mkv" => "video/x-matroska", "webm" => "video/webm",
        "avi" => "video/x-msvideo", "mov" => "video/quicktime", "mp3" => "audio/mpeg",
        "flac" => "audio/flac", "iso" => "application/x-iso9660-image", "m2ts" => "video/mp2t",
        _ => "application/octet-stream",
    };

    // 5. Send HTTP Response Headers (Support 206 Partial Content)
    if range_start > 0 || range_end.is_some() {
        let headers = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Type: {}\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\n\r\n",
            mime, length, range_start, end, total_size
        );
        let _ = stream.write_all(headers.as_bytes());
    } else {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
            mime, total_size
        );
        let _ = stream.write_all(headers.as_bytes());
    }

    if is_head_request {
        return; // HEAD request stops here. It just wanted the headers.
    }

    // 6. Zero-Disk Delivery (Requirement 3: Performance & Zero-Copy)
    info!("[Gateway] Streaming {} (Bytes {}-{})", clean_path, range_start, end);
    if let Err(e) = stream_file(&config, &config.db_path, &path_str, range_start, Some(length), use_direct_io, None, &mut stream) {
        // VITAL: Ignore "BrokenPipe". It just means the user closed VLC or scrubbed forward on the timeline.
        if e.kind() != std::io::ErrorKind::BrokenPipe {
            error!("[Gateway] Streaming interrupted for {}: {}", clean_path, e);
        }
    }
}

fn run_http_gateway(config: Arc<HuskConfig>, use_direct_io: bool) {
    let port = config.http_port.unwrap_or(8080);
    // Binds strictly to localhost to prevent open internet exposure (Requirement 4)
    let addr = format!("127.0.0.1:{}", port); 
    
    let listener = TcpListener::bind(&addr).expect("❌ Failed to bind HTTP Gateway port");
    info!("[Gateway] Local HTTP Streaming Gateway active: http://{}/stream/", addr);

    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let cfg = Arc::clone(&config);
            // Requirement 3: Threaded Concurrency per HTTP request
            thread::spawn(move || {
                handle_http_client(stream, cfg, use_direct_io);
            });
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let use_direct_io = std::env::var("DISABLE_O_DIRECT").is_err();

    // Graceful Shutdown Hook
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        log::info!("\n[System]  Shutting down safely...");
        r.store(false, Ordering::SeqCst);
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    // Load Config globally for all commands
    let config: HuskConfig = if let Ok(contents) = std::fs::read_to_string(&cli.config) {
        toml::from_str(&contents).expect("Failed to parse config file")
    } else {
        println!("⚠️ Config file not found. Generating default '{}'", cli.config);
        std::fs::write(&cli.config, DEFAULT_TOML).unwrap();
        toml::from_str(DEFAULT_TOML).unwrap()
    };
    let config_arc = Arc::new(config.clone());

    // Initialize logging based on TOML config
    let env = env_logger::Env::default().filter_or("RUST_LOG", &config_arc.log_level);
    env_logger::Builder::from_env(env).init();

    match &cli.command {
        Commands::Format { tape_dev } => {
            if let Err(e) = format_tape(tape_dev, use_direct_io) {
                error!("Format failed: {}", e);
            }
        }
        Commands::Daemon => {
            if unsafe { libc::geteuid() } != 0 {
                info!("⚠️ Running Husk without root privileges.");
                info!("   If fanotify fails, grant capabilities to the binary using:");
                info!("   sudo setcap cap_sys_admin,cap_dac_read_search+ep ./target/release/husk");
            }

            std::fs::create_dir_all(&config_arc.hot_tier).unwrap();
            let startup_conn = init_catalog(&config_arc.db_path).expect("Failed to init catalog database");
            rescan_tape_drives(&startup_conn);
            drop(startup_conn); 
            
            info!("Husk Archiver Initialized (Grid & Policy Engine Mode).");
            info!("Hot Tier: ./{}", config_arc.hot_tier);
            info!("Primary Volumes: {}", config_arc.primary_volumes.join(", "));
            info!("Replication Targets: {}", config_arc.replication_volumes.join(", "));

            let (tx, rx) = mpsc::sync_channel(100);

            // 1. Spawn the Archive Worker (Handles the heavy lifting)
            let worker_config = Arc::clone(&config_arc);
            thread::spawn(move || {
                run_archive_worker(rx, worker_config, use_direct_io);
            });

            // 2. Spawn the Janitor Policy Scanner (The Scheduler)
            let scanner_config = Arc::clone(&config_arc);
            thread::spawn(move || {
                loop {
                    // --- SCHEDULER: Sleep BEFORE scanning ---
                    if let Some(ref schedule) = scanner_config.janitor_schedule_time {
                        if schedule.to_lowercase() != "none" && schedule.contains(':') {
                            let now = chrono::Local::now();
                            if let Ok(target_time) = chrono::NaiveTime::parse_from_str(schedule, "%H:%M") {
                                let mut target_dt = now.date_naive().and_time(target_time);
                                
                                // If the scheduled time has already passed today, wait for tomorrow's slot
                                if target_dt <= now.naive_local() {
                                    target_dt += chrono::Duration::days(1);
                                }
                                
                                let duration = target_dt - now.naive_local();
                                let secs_to_wait = duration.num_seconds().max(1) as u64;
                                
                                info!("[Janitor] Production Mode: Sleeping until {} ({} seconds remaining)...", schedule, secs_to_wait);
                                thread::sleep(std::time::Duration::from_secs(secs_to_wait));
                            } else {
                                error!("[Janitor] ❌ Invalid schedule_time format '{}'. Use 'HH:MM'. Falling back to interval.", schedule);
                                thread::sleep(std::time::Duration::from_secs(scanner_config.janitor_interval_secs));
                            }
                        } else {
                            // Test Mode: Just sleep for the interval
                            thread::sleep(std::time::Duration::from_secs(scanner_config.janitor_interval_secs));
                        }
                    } else {
                        // Default Fallback
                        thread::sleep(std::time::Duration::from_secs(scanner_config.janitor_interval_secs));
                    }

                    // --- EXECUTION: Run the scan after the sleep period ends ---
                    info!("[Janitor] Starting scheduled policy scan...");
                    run_janitor_scanner(tx.clone(), Arc::clone(&scanner_config));
                }
            });

            // 3. Start HTTP Streaming Gateway (Persistent local media server bridge)
            let http_config = Arc::clone(&config_arc);
            thread::spawn(move || {
                run_http_gateway(http_config, use_direct_io);
            });

            // 4. Start Foreground Interceptor (Handles real-time OS restores)
            run_interceptor(config_arc, use_direct_io).unwrap();
        }
        Commands::Rebuild { tape_dev, output_db } => {
            rebuild_catalog(tape_dev, output_db, use_direct_io).unwrap();
        }
        Commands::Info { tape_dev } => {
            let target = tape_dev.as_deref().unwrap_or(&config_arc.primary_volumes[0]);
            print_tape_gauge(target, &config_arc.db_path);
        }
        Commands::Scrub { tape_dev } => {
            let target = tape_dev.as_deref().unwrap_or(&config_arc.primary_volumes[0]);
            if let Err(e) = scrub_tape(target, &config_arc.db_path, use_direct_io) {
                error!("Scrubber failed: {}", e);
            }
        }
        Commands::Restore { file_path, dest_path, version } => {
            if let Err(e) = manual_restore(&config_arc, &config_arc.db_path, file_path, dest_path, *version, use_direct_io) {
                error!("Manual restore failed: {}", e);
            }
        }
        
        Commands::Cat { file_path, offset, length, tape_uuid } => {
            log::set_max_level(log::LevelFilter::Debug);
            if let Err(e) = cat_file(&config_arc, &config_arc.db_path, file_path, *offset, *length, use_direct_io, tape_uuid.as_deref()) {
                eprintln!("Cat failed: {}", e);
            }
        }
        Commands::Repack { source_tape, dest_tape } => {
            if let Err(e) = repack_tape(&config_arc.db_path, source_tape, dest_tape, use_direct_io) {
                error!("Repacker failed: {}", e);
            }
        }
    }
}
// ---------------------------------------------------------
// 8.5 The "Stubber" Logic (Hole Punching & Xattr)
// ---------------------------------------------------------
fn stub_file(file_path: &str, file_size: u64) -> std::io::Result<()> {
    info!("\nStubbing '{}' (Punching hole to free {} bytes of SSD space)...", file_path, file_size);
    
    // 0. Save the original timestamps to prevent IDEs/Git from noticing the change
    let meta = std::fs::metadata(file_path)?;
    let atime_sec = meta.atime();
    let atime_nsec = meta.atime_nsec();
    let mtime_sec = meta.mtime();
    let mtime_nsec = meta.mtime_nsec();

    let file = OpenOptions::new().write(true).open(file_path)?;
    let fd = file.as_raw_fd();

    // 1. Punch a hole through the entire file using Linux fallocate
    // Kernel rejects fallocate length of 0 with EINVAL, so skip it for empty files
    if file_size > 0 {
        let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
        let ret = unsafe {
            libc::fallocate(fd, mode, 0, file_size as libc::off_t)
        };

        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    
    // Explicitly drop/close the file descriptor before restoring timestamps
    drop(file);

    // 2. Mark it as an archived stub via xattr
    xattr::set(file_path, "trusted.husk.status", b"stubbed")?;

    // 3. Silently restore the old timestamps to fool file watchers
    let c_path = CString::new(file_path).unwrap();
    let times = [
        libc::timespec { tv_sec: atime_sec as libc::time_t, tv_nsec: atime_nsec as libc::c_long },
        libc::timespec { tv_sec: mtime_sec as libc::time_t, tv_nsec: mtime_nsec as libc::c_long },
    ];
    unsafe {
        libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0);
    }

    info!("File successfully stubbed! 'ls -l' shows full size, but actual disk space used is 0.");
    Ok(())
}

// ---------------------------------------------------------
// Helper: Extract Hardware Drive Serial Number
// ---------------------------------------------------------
fn get_drive_serial(tape_dev: &str) -> String {
    if !tape_dev.starts_with("/dev/") { return "VIRTUAL_IMAGE".to_string(); }
    
    let dev_name = std::path::Path::new(tape_dev)
        .file_name().unwrap_or_default().to_string_lossy();
    
    // 1. Check for physical SCSI Tape Drives (e.g., /dev/nst0)
    if dev_name.starts_with("nst") || dev_name.starts_with("st") {
        let sys_path = format!("/sys/class/scsi_tape/{}/device/model", dev_name);
        return std::fs::read_to_string(&sys_path)
            .unwrap_or_else(|_| "SCSI_TAPE_DRIVE".to_string())
            .trim().to_string();
    }
    
    // 2. Check for standard Block Devices (e.g., /dev/sdb)
    let parent_dev = dev_name.trim_end_matches(char::is_numeric);
    let sys_path = format!("/sys/block/{}/device/serial", parent_dev);
    
    match std::fs::read_to_string(&sys_path) {
        Ok(serial) => serial.trim().to_string(),
        Err(_) => "UNKNOWN_HARDWARE".to_string(),
    }
}

// ---------------------------------------------------------
// 9. The Catalog (SQLite Database)
// ---------------------------------------------------------
fn init_catalog(db_path: &str) -> SqlResult<Connection> {
    let conn = Connection::open(db_path)?;
    
    // EXTREMELY IMPORTANT: Enable Write-Ahead Logging.
    // This allows the Sweeper (Thread) and Interceptor (Main) to write simultaneously 
    // without throwing "database is locked" errors.
    conn.execute_batch("PRAGMA journal_mode = WAL;")?;
    
    // Main tape catalog
    conn.execute(
        "CREATE TABLE IF NOT EXISTS catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            version INTEGER NOT NULL,
            tape_uuid TEXT NOT NULL,
            tape_offset INTEGER NOT NULL,
            payload_size INTEGER NOT NULL,
            compressed_size INTEGER DEFAULT 0,
            compression_type INTEGER DEFAULT 0,
            uid INTEGER NOT NULL,
            gid INTEGER NOT NULL,
            posix_mode INTEGER NOT NULL,
            archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            original_mtime INTEGER NOT NULL,
            blake3_hash TEXT NOT NULL,
            custom_metadata TEXT
        )",
        [],
    )?;
    
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_path_version ON catalog (file_path, version);",
        [],
    )?;

    // Tape Pool Table (Maps UUID to device paths & hardware serials)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS tapes (
            tape_uuid TEXT PRIMARY KEY,
            device_path TEXT NOT NULL,
            drive_serial TEXT DEFAULT 'VIRTUAL_IMAGE',
            backend_type TEXT DEFAULT 'local',
            location_hint TEXT DEFAULT NULL
        )",
        [],
    )?;
    
    // Alpha Patch: Add columns to existing DBs without wiping them
    let _ = conn.execute("ALTER TABLE tapes ADD COLUMN drive_serial TEXT DEFAULT 'VIRTUAL_IMAGE'", []);
    let _ = conn.execute("ALTER TABLE tapes ADD COLUMN backend_type TEXT DEFAULT 'local'", []);
    let _ = conn.execute("ALTER TABLE tapes ADD COLUMN location_hint TEXT DEFAULT NULL", []);

    //  Active File Tracking (Event-Driven Sweeper queue)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS active_tracking (
            file_path TEXT PRIMARY KEY,
            last_touch INTEGER NOT NULL
        )",
        [],
    )?;
    
    //  StreamGate Frame Index
    conn.execute(
        "CREATE TABLE IF NOT EXISTS object_frames (
            file_path TEXT NOT NULL,
            version INTEGER NOT NULL,
            uncompressed_offset INTEGER NOT NULL,
            compressed_offset INTEGER NOT NULL,
            compressed_size INTEGER NOT NULL
        )",
        [],
    )?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_frames ON object_frames (file_path, version);", [])?;
    
    Ok(conn)
}
