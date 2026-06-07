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
// ---------------------------------------------------------
// 10. Data Engineering: Parquet Export
// ---------------------------------------------------------
use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use std::fs::File;

pub fn export_catalog_parquet(db_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open(db_path)?;
    
    let mut stmt = conn.prepare(
        "SELECT id, file_path, version, tape_uuid, tape_offset, payload_size, 
                compressed_size, compression_type, archived_at, blake3_hash, custom_metadata 
         FROM catalog"
    )?;

    // Initialize Arrow Column Builders
    let mut id_b = Int64Builder::new();
    let mut path_b = StringBuilder::new();
    let mut version_b = Int64Builder::new();
    let mut uuid_b = StringBuilder::new();
    let mut offset_b = Int64Builder::new();
    let mut payload_b = Int64Builder::new();
    let mut comp_size_b = Int64Builder::new();
    let mut comp_type_b = Int64Builder::new();
    let mut archived_b = StringBuilder::new();
    let mut hash_b = StringBuilder::new();
    let mut meta_b = StringBuilder::new();

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, String>(8).unwrap_or_default(),
            row.get::<_, String>(9)?,
            row.get::<_, String>(10).unwrap_or_else(|_| "{}".to_string()),
        ))
    })?;

    for row in rows {
        if let Ok((id, path, version, uuid, offset, payload, comp_size, comp_type, archived, hash, meta)) = row {
            id_b.append_value(id);
            path_b.append_value(path);
            version_b.append_value(version);
            uuid_b.append_value(uuid);
            offset_b.append_value(offset);
            payload_b.append_value(payload);
            comp_size_b.append_value(comp_size);
            comp_type_b.append_value(comp_type);
            archived_b.append_value(archived);
            hash_b.append_value(hash);
            meta_b.append_value(meta);
        }
    }

    // Define the formal Parquet Schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("version", DataType::Int64, false),
        Field::new("tape_uuid", DataType::Utf8, false),
        Field::new("tape_offset", DataType::Int64, false),
        Field::new("payload_size", DataType::Int64, false),
        Field::new("compressed_size", DataType::Int64, false),
        Field::new("compression_type", DataType::Int64, false),
        Field::new("archived_at", DataType::Utf8, false),
        Field::new("blake3_hash", DataType::Utf8, false),
        Field::new("custom_metadata", DataType::Utf8, false), // Crucial for AI/MAM Tags!
    ]);

    // Build the Record Batch
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_b.finish()),
            Arc::new(path_b.finish()),
            Arc::new(version_b.finish()),
            Arc::new(uuid_b.finish()),
            Arc::new(offset_b.finish()),
            Arc::new(payload_b.finish()),
            Arc::new(comp_size_b.finish()),
            Arc::new(comp_type_b.finish()),
            Arc::new(archived_b.finish()),
            Arc::new(hash_b.finish()),
            Arc::new(meta_b.finish()),
        ],
    )?;

    // Write to Disk with Snappy Compression (Standard for Big Data)
    let file = File::create(output_path)?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
        
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
