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
use std::sync::atomic::{AtomicUsize, Ordering};

pub static ACTIVE_STREAMS: AtomicUsize = AtomicUsize::new(0);

struct StreamGuard;
impl Drop for StreamGuard {
    fn drop(&mut self) { ACTIVE_STREAMS.fetch_sub(1, Ordering::SeqCst); }
}

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

    // 1. Read request headers first: both the access token and the Range
    //    request must be known before we touch the filesystem or the catalog.
    let mut range_start: u64 = 0;
    let mut range_end: Option<u64> = None;
    let mut auth_header: Option<String> = None;

    loop {
        let mut header_line = String::new();
        if reader.read_line(&mut header_line).is_err() || header_line == "\r\n" { break; }
        let lower = header_line.to_lowercase();
        if lower.starts_with("range: bytes=") {
            let range_val = header_line[13..].trim();
            let split: Vec<&str> = range_val.split('-').collect();
            if !split.is_empty() {
                range_start = split[0].parse().unwrap_or(0);
                if split.len() > 1 && !split[1].is_empty() {
                    if let Ok(end_val) = split[1].parse() {
                        range_end = Some(end_val);
                    }
                }
            }
        } else if lower.starts_with("authorization:") {
            auth_header = Some(header_line[14..].trim().to_string());
        }
    }

    // 2. Access control: the gateway hands out raw archived payloads, so an
    //    optional bearer token gates every route (dashboard included).
    if let Some(expected) = config.http_token.as_deref().filter(|t| !t.is_empty()) {
        if !is_authorized(auth_header.as_deref(), parts[1], expected) {
            let _ = stream.write_all(
                b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer\r\nContent-Length: 0\r\n\r\n",
            );
            return;
        }
    }

    // 3. Map URL to Local Path securely (Strip Query Parameters!)
    let raw_url = parts[1].split('?').next().unwrap_or(parts[1]);
    let url_path = String::from_utf8_lossy(&percent_decode(raw_url)).into_owned();
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
            let db_paths: Vec<String> = stmt.query_map((), |row| row.get(0))
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
            let _ = stmt.query_map((), |row| {
                let path_str: String = row.get(1)?;
                let status = if xattr::get(&path_str, "trusted.husk.status").map(|v| v == Some(b"stubbed".to_vec())).unwrap_or(false) {
                    "STUBBED"
                } else {
                    "RESIDENT"
                };

                catalog.push(json!({
                    "id": row.get::<_, i64>(0)?,
                    "path": path_str,
                    "size": row.get::<_, i64>(2)?,
                    "status": status, 
                    "versions": row.get::<_, i64>(3)?,
                    "mtime": row.get::<_, i64>(4)?,
                    "tape": row.get::<_, String>(5)?
                }));
                Ok(())
            }).map(|rows| rows.filter_map(Result::ok).collect::<Vec<_>>());
        }

        let files_today: i64 = conn.query_row("SELECT COUNT(*) FROM catalog WHERE archived_at >= date('now')", (), |row| row.get(0)).unwrap_or(0);
        let total_replicas: i64 = conn.query_row("SELECT COUNT(*) FROM catalog", (), |row| row.get(0)).unwrap_or(0);

        let payload = json!({
            "volumes": vols,
            "catalog": catalog,
            "stats": {
                "filesArchivedToday": files_today, 
                "totalReplicas": total_replicas,
                "queueDepth": 0,
                "janitorNextRun": config.janitor_schedule_time.clone().unwrap_or_else(|| format!("Every {}s", config.janitor_interval_secs)),
                "activeStreams": ACTIVE_STREAMS.load(Ordering::SeqCst)
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
        Ok(p) => {
            let hot_tier_abs = Path::new(&config.hot_tier).canonicalize().unwrap_or_default();
            if !p.starts_with(&hot_tier_abs) {
                let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\n\r\n");
                return;
            }
            p
        },
        Err(_) => {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n");
            return;
        }
    };
    
    let path_str = abs_path.to_string_lossy().to_string();

    // The Range request was already parsed together with the other headers.

    // 3. Database Lookup: Fetch Logical Size AND Target Device Path with Failover!
    let conn = rusqlite::Connection::open(&config.db_path).unwrap();
    let mut stmt = conn.prepare(
        "SELECT c.payload_size, t.device_path, t.tape_uuid 
         FROM catalog c 
         JOIN tapes t ON c.tape_uuid = t.tape_uuid 
         WHERE c.file_path = ?1 AND c.version = (SELECT MAX(version) FROM catalog WHERE file_path = ?1)"
    ).unwrap();
    
    let mut rows = stmt.query(rusqlite::params![path_str]).unwrap();
    
    let mut total_size: u64 = 0;
    let mut target_tape_uuid = String::new();
    let mut found_valid = false;

    while let Some(row) = rows.next().unwrap() {
        total_size = row.get::<_, i64>(0).unwrap() as u64;
        let dev_path: String = row.get(1).unwrap();
        let tape_uuid: String = row.get(2).unwrap();
        
        if dev_path.starts_with("rclone:") || std::path::Path::new(&dev_path).exists() {
            target_tape_uuid = tape_uuid;
            found_valid = true;
            break;
        }
    }

    if !found_valid {
        error!("[Gateway] Backend volume offline or unreachable for: {}", path_str);
        let _ = stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\n\r\n");
        return;
    }

    // RFC 7233: clamp the range to the object and reject unsatisfiable ones.
    // Computing `end - start + 1` on an unbounded/inverted range underflows a
    // u64: debug builds panic, release builds wrap into a huge Content-Length.
    let (range_start, end) = match resolve_range(range_start, range_end, total_size) {
        Some(v) => v,
        None => {
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\n\r\n",
                    total_size
                ).as_bytes(),
            );
            return;
        }
    };
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
    if let Err(e) = stream_file(&config, &config.db_path, &path_str, range_start, Some(length), use_direct_io, Some(&target_tape_uuid), &mut stream) {
        // VITAL: Ignore "BrokenPipe". It just means the user closed VLC or scrubbed forward on the timeline.
        if e.kind() != std::io::ErrorKind::BrokenPipe {
            error!("[Gateway] Streaming interrupted for {}: {}", clean_path, e);
        }
    }
}

pub fn run_http_gateway(config: Arc<HuskConfig>, use_direct_io: bool) {
    let port = config.http_port.unwrap_or(8080);
    // Loopback by default: the gateway serves raw archived payloads, so exposing
    // it on all interfaces must be an explicit decision (see `http_bind`).
    let bind = config.http_bind.as_deref().filter(|b| !b.is_empty()).unwrap_or("127.0.0.1");
    let has_token = config.http_token.as_deref().map(|t| !t.is_empty()).unwrap_or(false);
    if !has_token && !is_loopback(bind) {
        log::warn!(
            "[Gateway] SECURITY: StreamGate is listening on {} without http_token; \
             anyone who can reach this port can read every archived file. \
             Set http_token (or bind to 127.0.0.1) to restrict access.",
            bind
        );
    }

    let addr = format!("{}:{}", bind, port);
    
    let listener = TcpListener::bind(&addr).expect(" Failed to bind HTTP Gateway port");
    info!("[Gateway] HTTP Streaming Gateway active: http://{}/stream/", addr);

    for stream in listener.incoming() {
        if let Ok(mut stream) = stream {
            if ACTIVE_STREAMS.load(Ordering::SeqCst) >= 50 {
                let _ = stream.write_all(b"HTTP/1.1 503 Service Unavailable\r\n\r\n");
                continue;
            }
            ACTIVE_STREAMS.fetch_add(1, Ordering::SeqCst);
            
            let cfg = Arc::clone(&config);
            thread::spawn(move || {
                let _guard = StreamGuard;
                handle_http_client(stream, cfg, use_direct_io);
            });
        }
    }
}

// ---------------------------------------------------------
// Request helpers (pure functions, unit-tested below)
// ---------------------------------------------------------

/// Decode `%XX` escapes without panicking on malformed input.
fn percent_decode(input: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut bytes = input.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            match (bytes.next(), bytes.next()) {
                (Some(h1), Some(h2)) => {
                    if let (Some(n1), Some(n2)) = (hex_val(h1), hex_val(h2)) {
                        out.push((n1 << 4) | n2);
                    } else {
                        // Not a valid escape sequence: keep the raw bytes.
                        out.push(b);
                        out.push(h1);
                        out.push(h2);
                    }
                }
                (Some(h1), None) => {
                    out.push(b);
                    out.push(h1);
                }
                (None, _) => out.push(b),
            }
            continue;
        }
        out.push(b);
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Constant-time comparison so a token check cannot leak how much of the
/// prefix matched.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Accepted forms: `Authorization: Bearer <token>` or `?token=<token>`
/// (media players often cannot set custom headers).
fn is_authorized(header: Option<&str>, request_target: &str, expected: &str) -> bool {
    if let Some(value) = header {
        let token = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
            .unwrap_or(value)
            .trim();
        if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
            return true;
        }
    }
    if let Some((_, query)) = request_target.split_once('?') {
        for pair in query.split('&') {
            if let Some(value) = pair.strip_prefix("token=") {
                if constant_time_eq(&percent_decode(value), expected.as_bytes()) {
                    return true;
                }
            }
        }
    }
    false
}

/// Resolve a requested byte range against the object size.
/// `None` means the range is unsatisfiable (HTTP 416).
fn resolve_range(start: u64, requested_end: Option<u64>, total: u64) -> Option<(u64, u64)> {
    if total == 0 || start >= total {
        return None;
    }
    let end = requested_end.unwrap_or(total - 1).min(total - 1);
    if end < start {
        return None;
    }
    Some((start, end))
}

fn is_loopback(bind: &str) -> bool {
    matches!(bind, "127.0.0.1" | "::1" | "localhost")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_file_without_range() {
        assert_eq!(resolve_range(0, None, 100), Some((0, 99)));
    }

    #[test]
    fn open_ended_range_clamps_to_eof() {
        assert_eq!(resolve_range(90, None, 100), Some((90, 99)));
    }

    #[test]
    fn end_beyond_eof_is_clamped() {
        assert_eq!(resolve_range(10, Some(500), 100), Some((10, 99)));
    }

    #[test]
    fn inverted_range_is_unsatisfiable() {
        // `Range: bytes=10-5` used to underflow `end - start + 1`.
        assert_eq!(resolve_range(10, Some(5), 100), None);
    }

    #[test]
    fn start_past_eof_is_unsatisfiable() {
        // `Range: bytes=500-` used to underflow as well.
        assert_eq!(resolve_range(500, None, 100), None);
    }

    #[test]
    fn empty_object_is_unsatisfiable() {
        assert_eq!(resolve_range(0, None, 0), None);
    }

    #[test]
    fn percent_decode_handles_escapes_and_garbage() {
        assert_eq!(percent_decode("/a%20b"), b"/a b".to_vec());
        assert_eq!(percent_decode("/a%zz"), b"/a%zz".to_vec());
        assert_eq!(percent_decode("/a%2"), b"/a%2".to_vec());
    }

    #[test]
    fn auth_accepts_bearer_and_query_token() {
        assert!(is_authorized(Some("Bearer s3cret"), "/stream/f", "s3cret"));
        assert!(is_authorized(Some("bearer s3cret"), "/stream/f", "s3cret"));
        assert!(is_authorized(None, "/stream/f?token=s3cret", "s3cret"));
        assert!(is_authorized(
            None,
            "/stream/f?a=1&token=s3%63ret",
            "s3cret"
        ));
    }

    #[test]
    fn auth_rejects_missing_or_wrong_tokens() {
        assert!(!is_authorized(None, "/stream/f", "s3cret"));
        assert!(!is_authorized(Some("Bearer wrong"), "/stream/f", "s3cret"));
        assert!(!is_authorized(Some("Basic s3cret"), "/stream/f", "s3cret"));
        assert!(!is_authorized(None, "/stream/f?token=wrong", "s3cret"));
    }

    #[test]
    fn only_loopback_binds_are_treated_as_safe() {
        assert!(is_loopback("127.0.0.1"));
        assert!(is_loopback("::1"));
        assert!(!is_loopback("0.0.0.0"));
        assert!(!is_loopback("192.168.1.10"));
    }
}
