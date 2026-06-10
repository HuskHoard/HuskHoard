//engine.rs
use log::{info, error};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::os::unix::net::UnixStream;

use rusqlite::{params, Connection};
use blake3::Hasher;
use crc32fast::Hasher as Crc32Hasher;
use serde_json::json;

use crate::config::{HuskConfig, SidecarBridge, ALIGNMENT};
use crate::format::*;
use crate::storage::*;
use crate::hardware::*;
use crate::database::*;

// ---------------------------------------------------------
// Volume Allocation: Sequential Fill ( Core Default)
// Fills volumes one by one by sorting by MOST used space first.
// ---------------------------------------------------------
pub fn get_balanced_volumes(volumes: &[String], db_path: &str, min_free_bytes: u64) -> Vec<String> {
    let mut vols_with_space: Vec<(String, u64)> = volumes.iter().filter_map(|dev| {
        // Use existing gauge to safely check Tapes, Block Devs, and Rclone!
        if let Ok((used, total, _)) = check_tape_gauge(dev, db_path) {
            let free = total.saturating_sub(used);
            if free >= min_free_bytes {
                // CHANGED: Store 'used' space instead of 'free' space
                Some((dev.clone(), used))
            } else {
                None // Drive falls below minimum threshold
            }
        } else {
            None // Skip inaccessible volumes
        }
    }).collect();

    // CHANGED: Sort descending by used space (Sticky drive effect)
    vols_with_space.sort_by(|a, b| b.1.cmp(&a.1));

    vols_with_space.into_iter().map(|(dev, _)| dev).collect()
}
// Returns a tuple: (Vector of replicas, Vector of StreamGate Frames (UncompressedOffset, CompressedOffset, CompressedSize))
pub fn archive_file(conn: &Connection, source_path: &str, config: &Arc<HuskConfig>, use_direct_io: bool) -> std::io::Result<(Vec<(u64, u64, u64, u8, String, String, String)>, Vec<(u64, u64, u64)>)> {
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
                error!("!!Hardware timeout for {}: {}", dev_path, e);
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
                } else { 
                    tape_meta.len() // Respect the actual fallocate size of the .img file
                };

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

    let min_free_bytes = config.min_free_space_gb.unwrap_or(0) * 1024 * 1024 * 1024;

    // Balance Tier 1: Primary Volume
    let balanced_primary = get_balanced_volumes(&config.primary_volumes, &config.db_path, min_free_bytes);
    let mut primary_secured = false;
    
    for dev in &balanced_primary {
        if try_attach_tape(dev) { primary_secured = true; break; }
    }
    
    if !primary_secured {
        error!("!!Primary volumes unavailable or below {} GB free! Attempting Failover Tier...", config.min_free_space_gb.unwrap_or(0));
        let balanced_failover = get_balanced_volumes(&config.failover_volumes, &config.db_path, min_free_bytes);
        for dev in &balanced_failover {
            if try_attach_tape(dev) { break; }
        }
    }

    // Balance Tier 2: Replication Volumes
    let balanced_replicas = get_balanced_volumes(&config.replication_volumes, &config.db_path, min_free_bytes);
    let mut replicas_secured = 0;
    for dev in &balanced_replicas {
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
    
    // LOGIC: Check config to bypass Zstd compression
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

        // Type 0x03: Pack StreamGate Jump Table (Array of u32 compressed sizes)
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
            error!(" Cloud finalization failed for {}: {}", tape.dev_path, e);
            return Err(e);
        }
    }

    let destination_names: Vec<String> = results.iter().map(|r| r.6.clone()).collect();
    info!("Replicated {} times to [{}] ({} bytes -> {} bytes)", 
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

pub fn stream_file<W: std::io::Write>(config: &Arc<HuskConfig>, db_path: &str, file_path: &str, offset: u64, length: Option<u64>, use_direct_io: bool, target_uuid: Option<&str>, out_handle: &mut W) -> std::io::Result<()> {
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
            error!("!!Hardware timeout for read {}: {}", tape_dev, e);
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Hardware Wake Timeout"));
        }
    }

    // --- STREAMGATE MATH (Jump Table Lookup) ---
    let req_start = offset;
    let req_end = length.map(|l| std::cmp::min(offset + l, payload_size)).unwrap_or(payload_size);
    
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

    // ---  O_DIRECT BLOCK ALIGNMENT ---
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
        
        // Rclone objects are individual files, not one giant tape. 
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
pub fn cat_file(config: &Arc<HuskConfig>, db_path: &str, file_path: &str, offset: u64, length: Option<u64>, use_direct_io: bool, target_uuid: Option<&str>) -> std::io::Result<()> {
    let stdout = std::io::stdout();
    let mut out_handle = stdout.lock();
    stream_file(config, db_path, file_path, offset, length, use_direct_io, target_uuid, &mut out_handle)
}

pub fn restore_file(config: &Arc<HuskConfig>, db_path: &str, tape_dev: &str, file_path: &str, dest_fd: i32, tape_offset: u64, use_direct_io: bool, is_manual: bool) -> std::io::Result<()> {
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
            error!("!!Hardware timeout for read {}: {}", tape_dev, e);
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

    // --- Explicitly enforce original UNIX ownership and permissions ---
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

pub fn manual_restore(config: &Arc<HuskConfig>, db_path: &str, file_path: &str, dest_path: &str, version: Option<u32>, use_direct_io: bool) -> std::io::Result<()> {
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
                    info!("Successfully rolled back to '{}'", dest_path);
                    Ok(())
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp_dest);
                    error!(" Restore corrupted or failed. Cleaned up temporary file.");
                    Err(e)
                }
            }
        }
        Err(_) => {
            error!(" File '{}' (Version: {:?}) not found in catalog.", file_path, version);
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Version not found in catalog."))
        }
    }
}
// ---------------------------------------------------------
// 7. Disaster Recovery: Deep Scan Rebuild
// ---------------------------------------------------------
pub fn rebuild_catalog(tape_dev: &str, db_path: &str, use_direct_io: bool) -> std::io::Result<()> {
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

            info!("Recovered: '{}' (Offset: {}, Size: {})", filename, offset, header.payload_size);
            
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
pub fn repack_tape(db_path: &str, source_dev: &str, dest_dev: &str, use_direct_io: bool) -> std::io::Result<()> {
    let src_is_char = std::fs::metadata(source_dev).map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR).unwrap_or(false);
    let dest_is_char = std::fs::metadata(dest_dev).map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR).unwrap_or(false);
    
    if source_dev.starts_with("rclone:") || dest_dev.starts_with("rclone:") || src_is_char || dest_is_char {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Repacking Append-Only/Cloud volumes is not yet supported."));
    }
    info!(" Starting Repacker: Moving active data from '{}' to '{}'...", source_dev, dest_dev);
    
    let conn = Connection::open(db_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    let _ = conn.busy_timeout(std::time::Duration::from_secs(30)); // Wait for daemon locks instead of panicking
    
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
        info!("   Repacked: {}", path);
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
pub fn scrub_tape(tape_dev: &str, db_path: &str, use_direct_io: bool) -> std::io::Result<()> {
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
        error!(" Invalid Volume Header on {}. Cannot scrub.", tape_dev);
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
            error!(" [CORRUPT] Seek error at offset {} for '{}'", offset, file_path);
            total_corrupted += 1;
            continue;
        }
        
        let mut header_buf = AlignedBuffer::new(ALIGNMENT);
        if tape.read_exact(header_buf.as_mut_slice()).is_err() {
            error!(" [CORRUPT] Read error at offset {} for '{}'", offset, file_path);
            total_corrupted += 1;
            continue;
        }
        
        let mut header: ObjectHeader = *bytemuck::from_bytes(header_buf.as_slice());
        let stored_crc = header.header_crc32;
        header.header_crc32 = 0;
        let mut crc = Crc32Hasher::new();
        crc.update(bytemuck::bytes_of(&header));
        if crc.finalize() != stored_crc {
            error!(" [CORRUPT] Header CRC mismatch for '{}' at offset {}", file_path, offset);
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
                    error!(" [CORRUPT] Zstd init failed for '{}'", file_path);
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
                    error!(" [CORRUPT] BLAKE3 mismatch for '{}' (Offset: {})", file_path, offset);
                    total_corrupted += 1;
                } else if total_checked % 50 == 0 {
                    info!("   ... {} objects scrubbed, 0 errors so far ...", total_checked);
                }
            }
            Err(_) => {
                error!(" [CORRUPT] Data stream error for '{}' (Offset: {})", file_path, offset);
                total_corrupted += 1;
            }
        }
    }
    
    info!("Scrub Complete! Checked: {}, Corrupted: {}", total_checked, total_corrupted);
    if total_corrupted == 0 && total_checked > 0 { info!("Volume is 100% HEALTHY."); } 
    else if total_checked == 0 { info!("!!Volume index is empty."); }
    
    Ok(())
}

// ---------------------------------------------------------
// 8.6 Maintenance: Prune & Hard Remove
// ---------------------------------------------------------
pub fn prune_catalog(db_path: &str) -> std::io::Result<()> {
    info!("Starting Catalog Reconciliation...");
    let conn = Connection::open(db_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    // Get all unique files currently in the catalog
    let mut stmt = conn.prepare("SELECT DISTINCT file_path FROM catalog").unwrap();
    let paths: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap().filter_map(Result::ok).collect();
    
    let mut purged_count = 0;

    for path in paths {
        // If the file does not exist on the SSD AT ALL (not even a stub)
        if !std::path::Path::new(&path).exists() {
            info!("  [Prune] File missing from filesystem. Removing from catalog: {}", path);
            
            // Delete from Catalog
            let _ = conn.execute("DELETE FROM catalog WHERE file_path = ?1", params![path]);
            
            // Delete from StreamGate index
            let _ = conn.execute("DELETE FROM object_frames WHERE file_path = ?1", params![path]);
            
            purged_count += 1;
        }
    }
    
    info!("Reconciliation complete. Purged {} orphaned records from the catalog.", purged_count);
    info!("These files will be physically erased from tape during the next 'repack'.");
    
    Ok(())
}

pub fn hard_remove(db_path: &str, file_path: &str) -> std::io::Result<()> {
    let conn = Connection::open(db_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    // 1. Resolve absolute path to match DB
    let abs_path = std::fs::canonicalize(file_path)
        .unwrap_or_else(|_| std::path::PathBuf::from(file_path))
        .to_string_lossy()
        .to_string();

    // 2. Erase from Filesystem if it exists
    if std::path::Path::new(&abs_path).exists() {
        std::fs::remove_file(&abs_path)?;
        info!("Deleted from Hot Tier: {}", abs_path);
    }

    // 3. Erase from Database
    let deleted_cat = conn.execute("DELETE FROM catalog WHERE file_path = ?1", params![abs_path]).unwrap_or(0);
    let _ = conn.execute("DELETE FROM object_frames WHERE file_path = ?1", params![abs_path]);

    if deleted_cat > 0 {
        info!("Deleted from Catalog: {}. It will be erased from tape on next repack.", abs_path);
    } else {
        error!("File not found in catalog: {}", abs_path);
    }

    Ok(())
}
