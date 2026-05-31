//database.rs
use rusqlite::{params, Connection, Result as SqlResult};
use log::{info, error};

pub fn rescan_tape_drives(conn: &Connection) {
    info!(" Scanning for physically moved Volumes...");
    let mut stmt = conn.prepare("SELECT tape_uuid, drive_serial, device_path FROM tapes WHERE drive_serial != 'VIRTUAL_IMAGE'").unwrap();
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
    }).unwrap().filter_map(Result::ok);

    for (uuid, serial, old_path) in rows {
        let mut found = false;
        
        // Scan Block Devices (HDDs / SSDs / USBs)
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

        // Scan SCSI Tape Drives if not found in block
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
            error!("!!Drive {} (Serial/Model: {}) is OFFLINE. Restores from it will fail.", old_path, serial);
        }
    }
}
// ---------------------------------------------------------
// 9. The Catalog (SQLite Database)
// ---------------------------------------------------------
pub fn init_catalog(db_path: &str) -> SqlResult<Connection> {
    let conn = Connection::open(db_path)?;
    let _ = conn.busy_timeout(std::time::Duration::from_secs(30));
    
    // Enable Write-Ahead Logging.
    // This allows the Sweeper (Thread) and Interceptor (Main) to write simultaneously 
    
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
