//hardware.rs
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use log::{info, error};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use rusqlite::{params, Connection};
use std::os::unix::io::AsRawFd;

use crate::format::*;
use crate::config::ALIGNMENT;

// ---------------------------------------------------------
// Tank Gauge: Capacity and Status Check
// ---------------------------------------------------------
pub fn format_bytes(bytes: u64) -> String {
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

pub fn check_tape_gauge(tape_dev: &str, db_path: &str) -> std::io::Result<(u64, u64, u64)> {
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
        meta.len() // Respect the actual fallocate size of the .img file
    };

    let mut used_capacity = ALIGNMENT as u64; 
    let mut active_data = 0;

    // Logic: If the DB exists, calculate "Used" space by finding the highest offset written.
    if Path::new(db_path).exists() {
        if let Ok(conn) = Connection::open(db_path) {
            if let Ok(mut file) = File::open(tape_dev) {
                let mut vol_buf = [0u8; ALIGNMENT];
                if file.read_exact(&mut vol_buf).is_ok() {
                    let vol_header: VolumeHeader = *bytemuck::from_bytes(&vol_buf);
                    if &vol_header.magic_bytes == b"USTDVOL\0" {
                        let tape_uuid_hex = vol_header.volume_uuid.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                        
                        let query_eof = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096), 4096) FROM catalog WHERE tape_uuid = ?1";
                        if let Ok(max_used) = conn.query_row(query_eof, params![tape_uuid_hex], |row| row.get::<_, i64>(0)) { 
                            used_capacity = max_used as u64; 
                        }

                        let query_active = "SELECT COALESCE(SUM(((compressed_size + 4095) / 4096) * 4096 + 4096), 0) FROM catalog c1 INNER JOIN (SELECT file_path, MAX(version) as max_ver FROM catalog GROUP BY file_path) c2 ON c1.file_path = c2.file_path AND c1.version = c2.max_ver WHERE tape_uuid = ?1";
                        if let Ok(act_data) = conn.query_row(query_active, params![tape_uuid_hex], |row| row.get::<_, i64>(0)) { 
                            active_data = act_data as u64; 
                        }
                    }
                }
            }
        }
    } else if !is_block_dev && !is_char_dev {
        // Only if DB is missing and it's a file, assume it's full
        used_capacity = std::cmp::max(meta.len(), ALIGNMENT as u64);
        active_data = used_capacity; 
    }

    used_capacity = std::cmp::min(used_capacity, total_capacity);
    Ok((used_capacity, total_capacity, active_data))
}

pub fn print_tape_gauge(tape_dev: &str, db_path: &str) {
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
                error!("!!WARNING: Tape capacity is critically low!");
            }
            if wasteland_pct >= 40.0 && used > (total / 4) {
                info!(" TIP: Reclaimable Space is high. Consider running a Repacker to reclaim space.");
            }
        }
        Err(e) => error!(" Failed to read Volume Health: {}", e),
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
pub fn open_tape_device(tape_dev: &str, read: bool, write: bool, create: bool, use_direct_io: bool) -> std::io::Result<File> {
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
                error!("!!O_DIRECT is unsupported on '{}'. Falling back to buffered I/O. (Set DISABLE_O_DIRECT=1 to silence this)", tape_dev);
                let mut fallback_opts = OpenOptions::new();
                fallback_opts.read(read).write(write).create(create);
                return fallback_opts.open(tape_dev);
            }
            Err(e) => return Err(e), // Bubble up other errors (e.g., Permission Denied)
        }
    }

    opts.open(tape_dev)
}

pub fn format_tape(tape_dev: &str, use_direct_io: bool) -> std::io::Result<()> {
    info!("Formatting Volume '{}'...", tape_dev);
    let mut tape = open_tape_device(tape_dev, false, true, true, use_direct_io)?;

    let is_char_dev = std::fs::metadata(tape_dev)
        .map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR)
        .unwrap_or(false);

    if is_char_dev {
        info!("📼 Physical Tape Drive detected. Issuing SCSI Rewind (MTREW)...");
        if let Err(e) = send_mtio_cmd(tape.as_raw_fd(), MTREW, 1) {
            error!("!!Tape rewind failed: {}. Ensure device is ready.", e);
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
    pub capacity: usize,
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
// Helper: Extract Hardware Drive Serial Number
// ---------------------------------------------------------
pub fn get_drive_serial(tape_dev: &str) -> String {
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
// Helper: Check Hot Tier SSD Usage (High-Water Mark)
// ---------------------------------------------------------
pub fn get_disk_usage(path: &str) -> std::io::Result<(u64, u64)> {
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let c_path = std::ffi::CString::new(path)?;
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret == 0 {
        let total = stat.f_blocks as u64 * stat.f_frsize as u64;
        let free = stat.f_bavail as u64 * stat.f_frsize as u64;
        let used = total.saturating_sub(free);
        Ok((used, total))
    } else {
        Err(std::io::Error::last_os_error())
    }
}
