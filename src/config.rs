//config.rs
use serde::Deserialize;
use clap::{Parser, Subcommand};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;
use std::io::{Read, Write};
use serde_json::json;

pub const ALIGNMENT: usize = 4096;

pub fn default_no_compress() -> Vec<String> {
    vec![
        "mp4".into(), "mkv".into(), "avi".into(), "mov".into(), "zip".into(),
        "tar".into(), "gz".into(), "rar".into(), "7z".into(), "jpg".into(),
        "png".into(), "iso".into()
    ]
}

#[derive(Deserialize, Clone, Debug)]
pub struct HuskConfig {
    pub min_free_space_gb: Option<u64>,
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
    pub hot_tier_max_usage_percent: Option<u8>, 
}

// ---------------------------------------------------------
//: Enterprise Sidecar IPC Bridge
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

pub const DEFAULT_TOML: &str = r#"# ==========================================
# Husk - Hybrid User-Space Storage Kernel
# ==========================================

# --- Core Paths ---
hot_tier = "hot_tier"
db_path = "husk_catalog.db"
log_level = "info" # Options: debug, info, warn, error
http_port = 8080   # HTTP Streaming Gateway Port
hot_tier_max_usage_percent = 80 # Spillover to tape if Hot Tier exceeds 80% full

# --- Volume Tiering ---
min_free_space_gb = 0 # Prevent drives from filling beyond this limit
primary_volumes = ["my_archive.img"]
failover_volumes = ["failover_tape.img"]
replication_volumes = ["replication_archive.img"]
#example replication_volumes = ["rclone:my_aws:huskhoard-archive/backups"]
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
pub struct Cli {
    #[arg(short, long, default_value = "husk_config.toml", global = true)]
    pub config: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the USTD daemon (Interceptor + Queue Worker)
    Daemon,
    /// Export the catalog metadata to a file format (e.g., Parquet) for Data Engineering
    Export {
        #[arg(long, default_value = "parquet")]
        format: String,
        #[arg(long)]
        output: String,
    },
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
        #[arg(long)]
        source: Option<String>,
    },
    /// Reclaim tape space by copying only the latest active files to a new tape (Garbage Collection)
    Repack {
        #[arg(long)]
        source_tape: String,
        #[arg(long)]
        dest_tape: String,
    },
    /// Reconcile the catalog by deleting DB entries for files that no longer exist on the filesystem
    Prune,
    /// Permanently delete a file from both the filesystem and the tape catalog
    Rm {
        #[arg(long)]
        file_path: String,
    }
}
pub fn is_path_excluded(path: &str, config: &Arc<HuskConfig>) -> bool {
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
