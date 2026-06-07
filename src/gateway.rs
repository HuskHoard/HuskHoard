//gateway.rs
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use log::{info, error};

use crate::config::HuskConfig;
use crate::engine::stream_file;
use serde_json::json;

// ---------------------------------------------------------
// HTTP Streaming Gateway (VLC / Plex Bridge)
// ---------------------------------------------------------
pub fn handle_http_client(mut stream: TcpStream, config: Arc<HuskConfig>, use_direct_io: bool) {
    // VITAL: Prevent zombie threads if a client network drops
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(15)));
    
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    
    if reader.read_line(&mut request_line).is_err() || request_line.is_empty() { return; }
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    
    if parts.len() < 2 { return; }
    let method = parts[0];

    // --- NEW: Handle browser CORS Preflight (OPTIONS) ---
    if method == "OPTIONS" {
        let headers = "HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, HEAD, OPTIONS\r\nAccess-Control-Allow-Headers: *\r\n\r\n";
        let _ = stream.write_all(headers.as_bytes());
        return;
    }

    // VITAL: Support HTTP HEAD requests used by Plex/VLC for probing file sizes
    if method != "GET" && method != "HEAD" { return; }
    let is_head_request = method == "HEAD";

    // 1. Map URL to Local Path securely
    let url_path = parts[1].replace("%20", " "); 
    if url_path.contains("..") { return; } // Requirement 4: Prevent Path Traversal

    // --- NEW: Live Dashboard API Endpoint ---
    if url_path == "/api/dashboard" {
        let conn = rusqlite::Connection::open(&config.db_path).unwrap();
        
        // Fetch Live Volumes — merge config-declared tiers with DB-registered tapes
        let mut vols = Vec::new();
        let mut seen_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Helper closure: build a volume JSON entry from a device path + tier label
        let build_vol = |path: &str, tier: &str, conn: &rusqlite::Connection, db_path: &str| -> serde_json::Value {
            // Look up DB metadata if this volume has been registered
            let (uuid, serial, backend_type, loc) = conn.query_row(
                "SELECT tape_uuid, COALESCE(drive_serial,''), COALESCE(backend_type,'local'), COALESCE(location_hint,'') FROM tapes WHERE device_path = ?1",
                rusqlite::params![path],
                |row| Ok((row.get::<_,String>(0)?, row.get::<_,String>(1)?, row.get::<_,String>(2)?, row.get::<_,String>(3)?))
            ).unwrap_or_else(|_| (
                format!("unregistered-{}", &path[path.len().saturating_sub(8)..]),
                String::new(),
                if path.starts_with("rclone:") { "cloud".into() } else { "local".into() },
                String::new(),
            ));

            let (used, total, active) = crate::hardware::check_tape_gauge(path, db_path).unwrap_or((0, 0, 0));
            let is_online = if path.starts_with("rclone:") { true } else { std::path::Path::new(path).exists() };

            json!({
                "uuid": uuid,
                "name": path.split('/').last().unwrap_or(path),
                "path": path,
                "tier": tier,
                "backend": backend_type.to_uppercase(),
                "status": if !is_online { "OFFLINE" } else if total > 0 { "ONLINE" } else { "DEGRADED" },
                "total": total,
                "used": used,
                "active": active,
                "wasteland": used.saturating_sub(active),
                "serial": serial,
                "location": loc,
            })
        };

        // 1. Primary volumes
        for path in &config.primary_volumes {
            if seen_paths.insert(path.clone()) {
                vols.push(build_vol(path, "PRIMARY", &conn, &config.db_path));
            }
        }
        // 2. Failover volumes
        for path in &config.failover_volumes {
            if seen_paths.insert(path.clone()) {
                vols.push(build_vol(path, "FAILOVER", &conn, &config.db_path));
            }
        }
        // 3. Replication volumes
        for path in &config.replication_volumes {
            if seen_paths.insert(path.clone()) {
                vols.push(build_vol(path, "REPLICATION", &conn, &config.db_path));
            }
        }
        // 4. Any DB-registered tapes not covered by config (e.g. old/moved volumes)
        if let Ok(mut stmt) = conn.prepare("SELECT device_path FROM tapes") {
            let db_paths: Vec<String> = stmt.query_map([], |row| row.get(0))
                .unwrap().filter_map(Result::ok).collect();
            for path in db_paths {
                if seen_paths.insert(path.clone()) {
                    vols.push(build_vol(&path, "LEGACY", &conn, &config.db_path));
                }
            }
        }

        // Fetch Live Catalog
        let mut catalog = Vec::new();
        if let Ok(mut stmt) = conn.prepare("SELECT id, file_path, payload_size, version, original_mtime, tape_uuid FROM catalog ORDER BY id DESC LIMIT 50") {
            let _ = stmt.query_map([], |row| {
                catalog.push(json!({
                    "id": row.get::<_, i64>(0)?,
                    "path": row.get::<_, String>(1)?,
                    "size": row.get::<_, i64>(2)?,
                    "status": "STUBBED", 
                    "versions": row.get::<_, i64>(3)?,
                    "mtime": row.get::<_, i64>(4)?,
                    "tape": row.get::<_, String>(5)?
                }));
                Ok(())
            }).map(|rows| rows.filter_map(Result::ok).collect::<Vec<_>>());
        }

        let payload = json!({
            "volumes": vols,
            "catalog": catalog,
            "stats": {
                "filesArchivedToday": catalog.len(), 
                "totalReplicas": catalog.len(),
                "queueDepth": 0,
                "janitorNextRun": config.janitor_schedule_time.clone().unwrap_or_else(|| format!("Every {}s", config.janitor_interval_secs)),
                "activeStreams": 1
            },
            "events": [], 
            "logs": [],
            "config": {
                "hot_tier_max_usage_percent": config.hot_tier_max_usage_percent.unwrap_or(80),
                "min_free_space_gb": config.min_free_space_gb.unwrap_or(0),
                "max_age_days": config.max_age_days,
                "max_versions": config.max_versions,
                "janitor_interval_secs": config.janitor_interval_secs,
                "exclude_dirs": config.exclude_dirs,
                "immediate_archive_extensions": config.immediate_archive_extensions,
                "no_compress_extensions": config.no_compress_extensions
            }
        });

        let json_str = payload.to_string();
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
            json_str.len(), json_str
        );
        let _ = stream.write_all(headers.as_bytes());
        return;
    }
    // --- END API Endpoint ---

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

    // 5. Send HTTP Response Headers (Support 206 Partial Content + CORS)
    if range_start > 0 || range_end.is_some() {
        let headers = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Type: {}\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nAccess-Control-Allow-Origin: *\r\n\r\n",
            mime, length, range_start, end, total_size
        );
        let _ = stream.write_all(headers.as_bytes());
    } else {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nAccess-Control-Allow-Origin: *\r\n\r\n",
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

pub fn run_http_gateway(config: Arc<HuskConfig>, use_direct_io: bool) {
    let port = config.http_port.unwrap_or(8080);
    // Bind to 0.0.0.0 to allow SSH tunnels, Tailscale, and LAN connections
    let addr = format!("0.0.0.0:{}", port); 
    
    let listener = TcpListener::bind(&addr).expect(" Failed to bind HTTP Gateway port");
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
