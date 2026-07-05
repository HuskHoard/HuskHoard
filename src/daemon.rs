//daemon.rs
use std::ffi::CString;
use std::fs::OpenOptions;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::io::Write;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::os::unix::net::UnixStream;
use std::sync::mpsc;
use log::{info, error};
use rusqlite::{params, Connection};
use serde_json::json;

use crate::config::{HuskConfig, SidecarBridge, is_path_excluded};
use crate::engine::archive_file;
use crate::hardware::get_drive_serial;

// ---------------------------------------------------------
// Helper: Recursively apply fanotify marks to subdirectories
// ---------------------------------------------------------
pub fn mark_directory_recursive(fan_fd: i32, dir: &Path, mask: u64, config: &Arc<HuskConfig>) {
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
pub fn run_interceptor(config: Arc<HuskConfig>, use_direct_io: bool) -> std::io::Result<()> {
    let watch_dir = &config.hot_tier;
    let db_path = &config.db_path;
    info!("\n[Daemon] Starting fanotify interceptor on '{}'...", watch_dir);
    let abs_dir = std::fs::canonicalize(watch_dir)?;
    
    let fan_fd = unsafe {
        libc::fanotify_init(libc::FAN_CLASS_PRE_CONTENT, libc::O_RDWR as u32)
    };
    if fan_fd < 0 { 
        let err = std::io::Error::last_os_error();
        error!(" fanotify_init failed: {}. Missing Root or Capabilities!", err);
        return Err(err); 
    }

    //  Use FAN_ACCESS_PERM instead of FAN_OPEN_PERM to avoid VFS inode lock deadlocks
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
                    // Ensure path is absolute and consistent with DB records
                    let path_str = std::fs::canonicalize(&real_path)
                        .unwrap_or(real_path)
                        .to_string_lossy()
                        .to_string();

                    // EVENT 1: Process is trying to READ a file
                    if (mask & libc::FAN_ACCESS_PERM) != 0 {
                        // Only query xattr (disk access) on READ events. 
                        let is_stubbed = xattr::get(&path_str, "trusted.husk.status")
                            .map(|v| v == Some(b"stubbed".to_vec()))
                            .unwrap_or(false);

                        if is_stubbed {
                            // FAST PATH: If the file is being opened strictly to OVERWRITE it (O_WRONLY / O_TRUNC),
                            // do NOT pull it from tape. Just drop the stub status and allow the overwrite instantly.
                            let flags = unsafe { libc::fcntl(fd_raw, libc::F_GETFL) };
                            let is_trunc = (flags & libc::O_TRUNC) != 0;

                            //  O_WRONLY on its own (like Python's "a" for append) is NOT safe to bypass,
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

                            if config.ignore_processes.iter().any(|p| p == &proc_name) {
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
                                        // (Timestamp capture removed: Sparse hole-filling preserves the inode)

                                        let mut restored = false;
                                        
                                        // 1. Duplicate the FD to write into the Stub safely
                                        let dup_fd = unsafe { libc::dup(fd_raw) };
                                        let mut dest_file = unsafe { std::fs::File::from_raw_fd(dup_fd) };

                                        for (db_tape, _db_offset) in &replicas {
                                            info!("[Cloud NAS] Fetching full file from replica '{}'...", db_tape);
                                            
                                            // Seek to the beginning of the file for a full restore
                                            if let Err(_) = std::io::Seek::seek(&mut dest_file, std::io::SeekFrom::Start(0)) {
                                                continue;
                                            }

                                            //  We must restore the FULL object before clearing the stub flag.
                                            // Partial fills combined with removing the stub flag cause subsequent reads 
                                            // to silently read zeroes from the hole-punched areas.
                                            match crate::engine::stream_file(
                                                &interceptor_config, 
                                                &db_path_clone, 
                                                &path_clone, 
                                                0, // start from beginning
                                                None, // read until EOF
                                                use_direct_io, 
                                                None, // tape_uuid
                                                &mut dest_file
                                            ) {
                                                Ok(_) => {
                                                    restored = true;
                                                    break;
                                                }
                                                Err(e) => {
                                                    error!("[Daemon] Full restore failed on '{}': {}", db_tape, e);
                                                }
                                            }
                                        }

                                        if restored {
                                            let _ = xattr::remove(&path_clone, "trusted.husk.status");
                                            // ...
                                            info!("[Daemon] Restore complete for: {}", path_clone);
                                        } else {
                                            // More descriptive error to distinguish between "Not Found" and "Offline"
                                            if replicas.is_empty() {
                                                error!("[Daemon] CRITICAL: Path '{}' not found in database. Check if paths are absolute!", path_clone);
                                            } else {
                                                error!("[Daemon] CRITICAL: All replicas for '{}' are physically offline!", path_clone);
                                            }

                                            if let Ok(meta) = std::fs::metadata(&path_clone) {
                                                let _ = stub_file(&path_clone, meta.len());
                                            }
                                        }

                                        {
                                            let mut restores = active_restores_clone.lock().unwrap();
                                            restores.remove(&path_clone);
                                        }
                                    } else {
                                        info!("[Daemon] Process '{}' waiting for primary restore of '{}'...", proc_name, path_clone);
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
                                error!("[Daemon]  Failed to track modified file {}: {}", path_str, e);
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
pub fn run_archive_worker(rx: mpsc::Receiver<String>, config: Arc<HuskConfig>, use_direct_io: bool) {
    let mut conn = Connection::open(&config.db_path).expect("Worker failed to open catalog");
    let _ = conn.busy_timeout(std::time::Duration::from_secs(30)); // Set busy timeout
    let mut archived_since_last_mirror = 0;
    
    loop {
        // Wait for a file in the queue. If idle for 5 seconds, perform maintenance (DB Mirroring).
        match rx.recv_timeout(Duration::from_secs(3600)) {
            Ok(mut path_str) => {
                // Canonicalize path before processing so DB records are always absolute
                if let Ok(abs_path) = std::fs::canonicalize(&path_str) {
                    path_str = abs_path.to_string_lossy().to_string();
                }

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
                    
                    // --- M&E SIDECAR PRE-ARCHIVE HOOK ---
                    // Wait for Sidecar to generate Proxies/MHLs before we archive and stub.
                    let mut custom_meta = String::from("{}");
                    if let Some(socket_path) = &config.sidecar_socket_path {
                        if let Ok(mut stream) = UnixStream::connect(socket_path) {
                            // M&E Transcodes can take time. Give it up to 1 hour to reply.
                            let _ = stream.set_read_timeout(Some(Duration::from_secs(3600))); 
                            let msg = json!({ "action": "PRE_ARCHIVE", "file_path": path_str });
                            let _ = stream.write_all(msg.to_string().as_bytes());
                            let _ = stream.write_all(b"\n");
                            
                            let mut reader = std::io::BufReader::new(stream);
                            let mut resp = String::new();
                            if let Ok(_) = reader.read_line(&mut resp) {
                                let resp = resp.trim();
                                if resp.starts_with('{') { custom_meta = resp.to_string(); }
                            }
                        }
                    }
                    // ------------------------------------

                    match archive_file(&conn, &path_str, &config, use_direct_io) {
                        Ok((replica_list, jump_table)) => {
                            let next_version: i64 = conn.query_row(
                                "SELECT COALESCE(MAX(version), 0) + 1 FROM catalog WHERE file_path = ?1",
                                params![path_str], |row| row.get(0),
                            ).unwrap_or(1);

                            let mut payload_size_saved = 0;
                            
                            // Wrap all catalog writes in a transaction to ensure durability.
                            // Do not stub the file if the database commit fails.
                            let mut db_commit_success = false;
                            
                            if let Ok(tx) = conn.transaction() {
                                let mut all_inserts_ok = true;
                                
                                for (offset, size, comp_size, comp_type, hash, tape_uuid, dev_path, ext_blocks) in &replica_list {
                                    payload_size_saved = *size; 
                                    let drive_serial = get_drive_serial(dev_path);
                                    
                                    if tx.execute(
                                        "INSERT OR REPLACE INTO tapes (tape_uuid, device_path, drive_serial) VALUES (?1, ?2, ?3)",
                                        params![tape_uuid, dev_path, drive_serial],
                                    ).is_err() { all_inserts_ok = false; }
                                    
                                    if tx.execute(
                                        "INSERT INTO catalog (file_path, version, tape_uuid, tape_offset, payload_size, compressed_size, compression_type, uid, gid, posix_mode, original_mtime, blake3_hash, custom_metadata, ext_blocks) 
                                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                                        params![path_str, next_version, tape_uuid, offset, size, comp_size, comp_type, meta.uid(), meta.gid(), meta.mode(), meta.mtime(), hash, custom_meta, *ext_blocks as u32],
                                    ).is_err() { all_inserts_ok = false; }
                                }

                                for (u_off, c_off, c_size) in jump_table {
                                    if tx.execute(
                                        "INSERT INTO object_frames (file_path, version, uncompressed_offset, compressed_offset, compressed_size) VALUES (?1, ?2, ?3, ?4, ?5)",
                                        params![path_str, next_version, u_off, c_off, c_size]
                                    ).is_err() { all_inserts_ok = false; }
                                }

                                let _ = tx.execute(
                                    "DELETE FROM catalog WHERE file_path = ?1 AND version NOT IN (
                                        SELECT version FROM catalog WHERE file_path = ?1 ORDER BY version DESC LIMIT ?2
                                    )", params![path_str, config.max_versions]
                                );
                                
                                let _ = tx.execute(
                                    "DELETE FROM object_frames WHERE file_path = ?1 AND version NOT IN (
                                        SELECT version FROM catalog WHERE file_path = ?1 ORDER BY version DESC LIMIT ?2
                                    )", params![path_str, config.max_versions]
                                );
                                
                                if all_inserts_ok && tx.commit().is_ok() {
                                    db_commit_success = true;
                                }
                            }

                            if !db_commit_success {
                                error!("[Worker] Catalog write failed for {}. Aborting stub process to prevent data loss.", path_str);
                                continue;
                            }
                            
                            // ---------------------------------------------------------
                            // POST-COMMIT CATALOG TRIGGER (Multi-Node Global Sync)
                            // ---------------------------------------------------------
                            let sidecar = SidecarBridge::new(&config);

                            let replicas_json: Vec<serde_json::Value> = replica_list.iter().map(|(offset, _, _, _, _, tape_uuid, dev_path, _)| {
                                json!({
                                    "tape_uuid": tape_uuid,
                                    "tape_offset": offset,
                                    "device_path": dev_path
                                })
                            }).collect();

                            let payload_size = replica_list[0].1;
                            let blake3_hash = &replica_list[0].4;

                            sidecar.send_event(json!({
                                "event": "CATALOG_UPDATE", 
                                "action": "UPSERT",
                                "file_path": path_str,
                                "version": next_version,
                                "payload_size": payload_size,
                                "blake3_hash": blake3_hash,
                                "replicas": replicas_json,
                                "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
                            }));

                            if let Err(e) = stub_file(&path_str, payload_size_saved) {
                                error!("[Worker]  Failed to stub {}: {}", path_str, e);
                            } else {
                                conn.execute("DELETE FROM active_tracking WHERE file_path = ?1", params![path_str]).unwrap();
                                info!("[Worker] Stubbed and removed from queue.");
                                archived_since_last_mirror += 1;
                            }
                        },
                        Err(e) => error!("[Worker]  Failed to archive {}: {}", path_str, e),
                    }
                } else {
                    conn.execute("DELETE FROM active_tracking WHERE file_path = ?1", params![path_str]).unwrap();
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Idle Time! If we just finished archiving files, mirror the database.
                if archived_since_last_mirror > 0 {
                    info!("[Worker] Queue idle for 1 hour. Mirroring Catalog to primary storage...");
                    let backup_path = format!("{}_backup.db", config.db_path);
                    let _ = std::fs::remove_file(&backup_path);
                    
                    if conn.execute(&format!("VACUUM INTO '{}'", backup_path), []).is_ok() {
                        if let Ok((results, _)) = archive_file(&conn, &backup_path, &config, use_direct_io) {
                            let special_path = "__HUSK_CATALOG_BACKUP__";
                            let next_ver: i64 = conn.query_row(
                                "SELECT COALESCE(MAX(version), 0) + 1 FROM catalog WHERE file_path = ?1",
                                params![special_path], |row| row.get(0),
                            ).unwrap_or(1);
                            
                            for (offset, size, comp_size, comp_type, hash, tape_uuid, _, ext_blocks) in results {
                            let _ = conn.execute(
                                "INSERT INTO catalog (file_path, version, tape_uuid, tape_offset, payload_size, compressed_size, compression_type, uid, gid, posix_mode, original_mtime, blake3_hash, custom_metadata, ext_blocks) 
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                                params![special_path, next_ver, tape_uuid, offset, size, comp_size, comp_type, 0, 0, 0644, 0, hash, "{}", ext_blocks as u32],
                            );
                        }

                        // FIFO: Keep only the 3 most recent versions of the catalog backup
                        let _ = conn.execute(
                            "DELETE FROM catalog WHERE file_path = ?1 AND version NOT IN (
                                SELECT version FROM catalog WHERE file_path = ?1 ORDER BY version DESC LIMIT 3
                            )", params![special_path]
                        );
                        
                        // Clean up any StreamGate frame metadata associated with the purged versions
                        let _ = conn.execute(
                            "DELETE FROM object_frames WHERE file_path = ?1 AND version NOT IN (
                                SELECT version FROM catalog WHERE file_path = ?1 ORDER BY version DESC LIMIT 3
                            )", params![special_path]
                        );

                        info!("[Worker] Catalog securely mirrored (retaining 3 versions).");
                        } else {
                            error!("[Worker]  Failed to write Catalog Mirror.");
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
pub fn run_janitor_scanner(tx: mpsc::SyncSender<String>, config: Arc<HuskConfig>) {
    let conn = Connection::open(&config.db_path).unwrap();
    let _ = conn.busy_timeout(Duration::from_secs(30));
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let max_age_secs = config.max_age_days * 24 * 3600; 

    // NEW: Check Hot Tier Usage (High-Water Mark)
    let mut emergency_bytes_to_free = 0u64;
    let max_pct = config.hot_tier_max_usage_percent.unwrap_or(80) as f64 / 100.0;
    
    if let Ok((used, total)) = crate::hardware::get_disk_usage(&config.hot_tier) {
        let target_max_used = (total as f64 * max_pct) as u64;
        if used > target_max_used {
            emergency_bytes_to_free = used - target_max_used;
            info!("[Janitor] EMERGENCY SPILLOVER: Hot tier is over {}% full. Freeing {} to reach safe levels.", 
                  config.hot_tier_max_usage_percent.unwrap_or(80), 
                  crate::hardware::format_bytes(emergency_bytes_to_free));
        }
    }

    // NEW: Order by oldest first to ensure emergency spillover drops the stalest files
    let mut stmt = conn.prepare("SELECT file_path, last_touch FROM active_tracking ORDER BY last_touch ASC").unwrap();
    let rows: Vec<(String, u64)> = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap().filter_map(Result::ok).collect();

    for (path_str, last_touch) in rows {
        let path = Path::new(&path_str);
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        
        let is_immediate_ext = config.immediate_archive_extensions.iter().any(|e| e == ext);
        let is_immediate_dir = config.immediate_archive_dirs.iter().any(|d| path_str.contains(d));
        let is_immediate = is_immediate_ext || is_immediate_dir;
        
        let age_secs = now.saturating_sub(last_touch);

        let mut should_archive = is_immediate || age_secs >= max_age_secs;

        // EMERGENCY SPILLOVER LOGIC
        if !should_archive && emergency_bytes_to_free > 0 {
            let is_stubbed = xattr::get(&path_str, "trusted.husk.status")
                .map(|v| v == Some(b"stubbed".to_vec()))
                .unwrap_or(false);
                
            if !is_stubbed {
                if let Ok(meta) = std::fs::metadata(&path_str) {
                    if meta.is_file() {
                        should_archive = true;
                        // Deduct the size of the file we are about to stub
                        emergency_bytes_to_free = emergency_bytes_to_free.saturating_sub(meta.len());
                        info!("[Janitor] Queuing '{}' for emergency spillover...", path_str);
                    }
                }
            }
        }

        if should_archive {
            if let Err(mpsc::TrySendError::Full(_)) = tx.try_send(path_str) {
                info!("[Janitor] Queue full, will retry later.");
                break; 
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
