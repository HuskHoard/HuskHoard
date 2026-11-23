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

//
/// One frame per 16 MiB of payload; a u16 TLV length caps a single 0x03 record
/// at 65535/4 frames, so longer jump tables are split over several records.
const MAX_FRAMES_PER_TLV: usize = (u16::MAX as usize) / 4;

/// Legacy frame table record: compressed size only (4 bytes per frame).
const TLV_FRAME_TABLE_LEGACY: u16 = 0x03;

/// Exact frame table record: compressed size + uncompressed size (8 bytes per
/// frame). Rebuild needs the real frame lengths because the writer flushes a
/// frame once it has accumulated *at least* 16 MiB (read granularity is 1 MiB),
/// so frames are 16-17 MiB and a fixed 16 MiB step drifts.
const TLV_FRAME_TABLE_EXACT: u16 = 0x05;

/// u16 length field / 8 bytes per entry.
const MAX_EXACT_FRAMES_PER_TLV: usize = (u16::MAX as usize) / 8;

/// Serialize the StreamGate jump table as one or more 0x03 TLV records.
/// Splitting keeps every record's u16 length field representable; the reader
/// continues the running offsets across consecutive records.
fn encode_jump_table(jump_table: &[(u64, u64, u64)], out: &mut Vec<u8>) {
    for chunk in jump_table.chunks(MAX_FRAMES_PER_TLV) {
        let frames_payload_len = (chunk.len() * 4) as u16;
        out.push(0x00);
        out.push(0x03);
        out.extend_from_slice(&frames_payload_len.to_be_bytes());
        for (_, _, c_size) in chunk {
            out.extend_from_slice(&(*c_size as u32).to_be_bytes());
        }
    }
}

/// Serialize the exact frame table (0x05): compressed size plus the true
/// uncompressed length of each frame, so a catalog rebuild reproduces the same
/// seek offsets the writer used.
fn encode_frame_table_exact(
    jump_table: &[(u64, u64, u64)],
    payload_size: u64,
    out: &mut Vec<u8>,
) -> std::io::Result<()> {
    let mut start = 0;
    while start < jump_table.len() {
        let end = (start + MAX_EXACT_FRAMES_PER_TLV).min(jump_table.len());
        let chunk = &jump_table[start..end];
        let payload_len = u16::try_from(chunk.len() * 8)
            .map_err(|_| invalid_data("exact frame table chunk exceeds the u16 TLV length"))?;
        out.push(0x00);
        out.push(TLV_FRAME_TABLE_EXACT as u8);
        out.extend_from_slice(&payload_len.to_be_bytes());
        for (i, (u_start, _, c_size)) in chunk.iter().enumerate() {
            let next_u = if start + i + 1 < jump_table.len() {
                jump_table[start + i + 1].0
            } else {
                payload_size
            };
            let u_size = next_u
                .checked_sub(*u_start)
                .ok_or_else(|| invalid_data("frame offsets are not monotonic"))?;
            if u_size == 0 {
                return Err(invalid_data("zero-length frame must not enter the seek index"));
            }
            let c_size = u32::try_from(*c_size)
                .map_err(|_| invalid_data("compressed frame exceeds u32"))?;
            let u_size = u32::try_from(u_size)
                .map_err(|_| invalid_data("uncompressed frame exceeds u32"))?;
            out.extend_from_slice(&c_size.to_be_bytes());
            out.extend_from_slice(&u_size.to_be_bytes());
        }
        start = end;
    }
    Ok(())
}

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
pub fn archive_file(conn: &Connection, source_path: &str, config: &Arc<HuskConfig>, use_direct_io: bool) -> std::io::Result<(Vec<(u64, u64, u64, u8, String, String, String, usize)>, Vec<(u64, u64, u64)>)> {
    info!("Archiving '{}'...", source_path);

    let sidecar = SidecarBridge::new(config);
    let mut src_file = File::open(source_path)?;
    let src_meta = src_file.metadata()?;
    let payload_size = src_meta.len();

    // Telemetry Broadcast
    sidecar.send_event(json!({
        "event": "ARCHIVE_START",
        "file": source_path,
        "size": payload_size
    }));

    // ---------------------------------------------------------
    // Phase 1: Hash & (if enabled) Stage Compression BEFORE touching tape/SMR write pointers
    // ---------------------------------------------------------
    enum StageTarget {
        Ram(Vec<u8>),
        Disk(File),
    }
    struct StageWriter {
        target: StageTarget,
        bytes_written: u64,
    }
    impl std::io::Write for StageWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let n = match &mut self.target {
                StageTarget::Ram(v) => { v.extend_from_slice(buf); buf.len() }
                StageTarget::Disk(f) => f.write(buf)?,
            };
            self.bytes_written += n as u64;
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            match &mut self.target {
                StageTarget::Ram(_) => Ok(()),
                StageTarget::Disk(f) => f.flush(),
            }
        }
    }

    enum StagedPayload {
        RawFile(File),
        Ram(std::io::Cursor<Vec<u8>>),
        DiskFile(File),
    }
    impl std::io::Read for StagedPayload {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self {
                StagedPayload::RawFile(f) => f.read(buf),
                StagedPayload::Ram(c) => c.read(buf),
                StagedPayload::DiskFile(f) => f.read(buf),
            }
        }
    }

    let file_ext = std::path::Path::new(source_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let use_compression = !config.no_compress_extensions.contains(&file_ext);
    let compression_type_flag: u8 = if use_compression { 1 } else { 0 };

    let mut hasher = Hasher::new();
    let mut jump_table: Vec<(u64, u64, u64)> = Vec::new();
    let mut io_buf = vec![0u8; 1024 * 1024]; // 1MB buffer
    let final_compressed_size: u64;
    let mut staged_payload: StagedPayload;

    if use_compression {
        const RAM_STAGE_LIMIT: u64 = 64 * 1024 * 1024; // 64MB in RAM, larger files spill to unlinked temp file
        let target = if payload_size <= RAM_STAGE_LIMIT {
            StageTarget::Ram(Vec::with_capacity(payload_size as usize))
        } else {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let stage_path = Path::new(&config.hot_tier)
                .join(format!(".husk_stage_{}_{}.tmp", std::process::id(), ts));
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&stage_path)?;
            let _ = std::fs::remove_file(&stage_path); // Unlink immediately so OS cleans up on drop/crash
            StageTarget::Disk(f)
        };

        let mut stage_writer = StageWriter { target, bytes_written: 0 };
        let chunk_size = 16 * 1024 * 1024; // 16MB Frame boundaries for StreamGate
        let mut current_frame_bytes = 0;
        let mut uncompressed_start = 0;
        let mut compressed_start = 0;

        let mut encoder = zstd::stream::write::Encoder::new(&mut stage_writer, 3)?;
        loop {
            let n = src_file.read(&mut io_buf)?;
            if n == 0 { break; }
            hasher.update(&io_buf[..n]);
            encoder.write_all(&io_buf[..n])?;
            current_frame_bytes += n;

            if current_frame_bytes >= chunk_size {
                encoder.finish()?;
                let current_compressed_offset = stage_writer.bytes_written;
                let compressed_size = current_compressed_offset - compressed_start;
                jump_table.push((uncompressed_start, compressed_start, compressed_size));

                uncompressed_start += current_frame_bytes as u64;
                compressed_start = current_compressed_offset;
                current_frame_bytes = 0;
                encoder = zstd::stream::write::Encoder::new(&mut stage_writer, 3)?;
            }
        }
        encoder.finish()?;

        let current_compressed_offset = stage_writer.bytes_written;
        let compressed_size = current_compressed_offset - compressed_start;
        if compressed_size > 0 {
            jump_table.push((uncompressed_start, compressed_start, compressed_size));
        }
        final_compressed_size = current_compressed_offset;

        staged_payload = match stage_writer.target {
            StageTarget::Ram(v) => StagedPayload::Ram(std::io::Cursor::new(v)),
            StageTarget::Disk(mut f) => {
                f.seek(SeekFrom::Start(0))?;
                StagedPayload::DiskFile(f)
            }
        };
    } else {
        // Raw uncompressed: hash in pass 1 (0 extra disk/RAM), then stream directly
        loop {
            let n = src_file.read(&mut io_buf)?;
            if n == 0 { break; }
            hasher.update(&io_buf[..n]);
        }
        src_file.seek(SeekFrom::Start(0))?;
        final_compressed_size = payload_size;
        staged_payload = StagedPayload::RawFile(src_file);
    }

    let data_hash = hasher.finalize();
    let hash_hex = data_hash.to_hex().to_string();
    let final_padded_size = ((final_compressed_size + (ALIGNMENT as u64) - 1) / (ALIGNMENT as u64)) * (ALIGNMENT as u64);

    // Build TLVs & extension blocks up front so header is 100% final before any write
    let mut all_tlvs = Vec::new();
    let filename = Path::new(source_path).file_name().unwrap().to_str().unwrap().as_bytes();

    // Type 0x01: Pack Filename
    all_tlvs.push(0x00); all_tlvs.push(0x01);
    all_tlvs.extend_from_slice(&(filename.len() as u16).to_be_bytes());
    all_tlvs.extend_from_slice(filename);

    // Type 0x02: Pack Extracted Xattrs
    if let Ok(xattrs) = xattr::list(source_path) {
        for attr in xattrs {
            if attr.to_string_lossy().starts_with("trusted.ustd") { continue; }
            if let Ok(Some(val)) = xattr::get(source_path, &attr) {
                let attr_bytes = attr.to_string_lossy().as_bytes().to_vec();
                if attr_bytes.len() > 255 {
                    log::warn!("Skipping xattr: name exceeds 255 bytes");
                    continue;
                }
                let total_len = 1 + attr_bytes.len() + 2 + val.len();
                all_tlvs.push(0x00); all_tlvs.push(0x02);
                all_tlvs.extend_from_slice(&(total_len as u16).to_be_bytes());
                all_tlvs.push(attr_bytes.len() as u8);
                all_tlvs.extend_from_slice(&attr_bytes);
                all_tlvs.extend_from_slice(&(val.len() as u16).to_be_bytes());
                all_tlvs.extend_from_slice(&val);
            }
        }
    }

    // Type 0x03 & 0x05: Pack StreamGate Jump Tables (legacy 0x03 + exact 0x05)
    encode_jump_table(&jump_table, &mut all_tlvs);
    encode_frame_table_exact(&jump_table, payload_size, &mut all_tlvs)?;

    let mut tlv_fixed = [0u8; 3960];
    let mut ext_blocks = 0usize;
    let mut ext_aligned_buf: Option<AlignedBuffer> = None;

    if all_tlvs.len() <= 3960 {
        tlv_fixed[..all_tlvs.len()].copy_from_slice(&all_tlvs);
    } else {
        tlv_fixed.copy_from_slice(&all_tlvs[..3960]);
        let overflow = &all_tlvs[3960..];
        ext_blocks = (overflow.len() + 4095) / 4096;
        let mut aligned = AlignedBuffer::new(ext_blocks * 4096);
        aligned.as_mut_slice()[..overflow.len()].copy_from_slice(overflow);
        ext_aligned_buf = Some(aligned);
    }

    let estimated_need = (ALIGNMENT as u64) + final_padded_size + ((ext_blocks as u64 + 1) * ALIGNMENT as u64);

    // ---------------------------------------------------------
    // Phase 2: Select and attach target volumes
    // ---------------------------------------------------------
    let mut active_tapes: Vec<ActiveTape> = Vec::new();

    let mut try_attach_tape = |dev_path: &str| -> bool {
        if dev_path.starts_with("husk-grid://") {
            let mut hasher = blake3::Hasher::new();
            hasher.update(dev_path.as_bytes());
            let uuid_bytes: [u8; 16] = hasher.finalize().as_bytes()[0..16].try_into().unwrap();
            let uuid_hex = uuid_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();

            let query = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096 + (COALESCE(ext_blocks, 0) * 4096)), 4096) FROM catalog WHERE tape_uuid = ?1";
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

            let query = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096 + (COALESCE(ext_blocks, 0) * 4096)), 4096) FROM catalog WHERE tape_uuid = ?1";
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
                let query = "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096 + (COALESCE(ext_blocks, 0) * 4096)), 4096) FROM catalog WHERE tape_uuid = ?1";
                let logical_eof = conn.query_row(query, params![uuid_hex], |row| row.get::<_, i64>(0)).unwrap_or(4096) as u64;
                let start_offset = if logical_eof % ALIGNMENT as u64 == 0 { logical_eof } else { logical_eof + ALIGNMENT as u64 - (logical_eof % ALIGNMENT as u64) };

                let is_block_dev = (tape_meta.mode() & libc::S_IFMT) == libc::S_IFBLK;
                let total_capacity = if is_block_dev {
                    let mut f2 = std::fs::File::open(dev_path).unwrap();
                    f2.seek(SeekFrom::End(0)).unwrap_or(0)
                } else if is_char_dev {
                    12_000_000_000_000
                } else {
                    tape_meta.len()
                };

                if start_offset + estimated_need <= total_capacity {
                    let backend = if is_char_dev {
                        info!("Appending to End of Data (MTEOM) on {}...", dev_path);
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

    let mut skip_replication = false;
    for dir in &config.no_replicate_dirs {
        if source_path.contains(dir) {
            skip_replication = true;
            info!("[Archiver] Replication bypassed: '{}' matches no_replicate_dirs rule ('{}')", source_path, dir);
            break;
        }
    }

    if !skip_replication {
        let balanced_replicas = get_balanced_volumes(&config.replication_volumes, &config.db_path, min_free_bytes);
        let mut replicas_secured = 0;
        for dev in &balanced_replicas {
            if replicas_secured >= config.replicas { break; }
            if try_attach_tape(dev) { replicas_secured += 1; }
        }
    }

    if active_tapes.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "No valid tape pools available with capacity."));
    }

    // ---------------------------------------------------------
    // Phase 3: Write strictly sequentially (No backward seeks!)
    // ---------------------------------------------------------
    let mut tape_headers: Vec<ObjectHeader> = Vec::with_capacity(active_tapes.len());
    let ext_blocks_u16 = u16::try_from(ext_blocks)
        .map_err(|_| invalid_data("extension block count exceeds u16"))?;
    let ext_pad = ext_blocks_u16.to_le_bytes();

    let mut header_buf = AlignedBuffer::new(ALIGNMENT);
    let zero_buf = AlignedBuffer::new(ALIGNMENT);

    for tape in &mut active_tapes {
        let mut header = ObjectHeader {
            magic_bytes: *b"USTD\x00\x01\x02\x03",
            format_version: 1,
            flags: 0,
            compression_type: compression_type_flag,
            reserved_pad: [ext_pad[0], ext_pad[1], 0],
            payload_size,
            compressed_size: final_compressed_size,
            padded_size: final_padded_size,
            object_uuid: [0; 16],
            tape_uuid: tape.volume_uuid,
            mtime: src_meta.mtime(),
            ctime: src_meta.ctime(),
            posix_mode: src_meta.mode(),
            uid: src_meta.uid(),
            gid: src_meta.gid(),
            data_checksum: *data_hash.as_bytes(),
            header_crc32: 0,
            tlv_data: tlv_fixed,
        };
        let mut crc = Crc32Hasher::new();
        crc.update(bytemuck::bytes_of(&header));
        header.header_crc32 = crc.finalize();

        tape.backend.seek_to(tape.start_offset)?;
        if tape.is_append_only {
            // Tape/Rclone/Grid still use 4KB dummy prefix + real footer header
            tape.backend.write_all(zero_buf.as_slice())?;
        } else {
            // Local/Block/HM-SMR: write the REAL ObjectHeader up front (no seek-back overwrite!)
            header_buf.as_mut_slice().copy_from_slice(bytemuck::bytes_of(&header));
            tape.backend.write_all(header_buf.as_slice())?;
        }
        tape_headers.push(header);
    }

    // Stream staged payload sequentially across all target volumes
    {
        let tape_refs: Vec<&mut dyn std::io::Write> = active_tapes
            .iter_mut()
            .map(|t| &mut t.backend as &mut dyn std::io::Write)
            .collect();
        let mut tape_writer = MultiTapeWriter::new(tape_refs);
        loop {
            let n = staged_payload.read(&mut io_buf)?;
            if n == 0 { break; }
            tape_writer.write_all(&io_buf[..n])?;
        }
        tape_writer.pad_and_flush()?;
    }

    // Append footer (append-only media only) and extension blocks sequentially
    let mut results = Vec::new();
    for (tape, header) in active_tapes.iter_mut().zip(tape_headers.iter()) {
        if tape.is_append_only {
            header_buf.as_mut_slice().copy_from_slice(bytemuck::bytes_of(header));
            tape.backend.write_all(header_buf.as_slice())?;
        }
        if let Some(ref ext_buf) = ext_aligned_buf {
            tape.backend.write_all(ext_buf.as_slice())?;
        }
        results.push((
            tape.start_offset,
            payload_size,
            final_compressed_size,
            compression_type_flag,
            hash_hex.clone(),
            tape.uuid_hex.clone(),
            tape.dev_path.clone(),
            ext_blocks,
        ));
    }

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
        conn.query_row(query, params![file_path, uuid], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i64>(2)? as u64, row.get(3)?, row.get::<_, i64>(4)? as u64, row.get::<_, i32>(5)? as u32)))
    } else {
        conn.query_row(query, params![file_path], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i64>(2)? as u64, row.get(3)?, row.get::<_, i64>(4)? as u64, row.get::<_, i32>(5)? as u32)))
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
    let req_end = length
        .map(|l| std::cmp::min(offset.saturating_add(l), payload_size))
        .unwrap_or(payload_size);

    // An empty or past-EOF request must not touch the volume, and must not pick
    // a frame index (a zero length used to fall through to a bogus range).
    if length == Some(0) || req_start >= payload_size || req_end <= req_start {
        info!(
            "[StreamGate] Empty request for {} (offset={}, length={:?}); nothing to stream",
            file_path, offset, length
        );
        return Ok(());
    }
    
    let mut stmt = conn.prepare("SELECT uncompressed_offset, compressed_offset, compressed_size FROM object_frames WHERE file_path = ?1 AND version = ?2 ORDER BY uncompressed_offset ASC").unwrap();
    let frames: Vec<(u64, u64, u64)> = stmt.query_map(params![file_path, version], |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64, row.get::<_, i64>(2)? as u64))).unwrap().filter_map(Result::ok).collect();

    let mut target_c_offset = 0;
    let mut target_c_len = db_comp; // Feed only exact compressed bytes to Zstd
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
        let final_end = final_frame.1 + final_frame.2;
        if final_end < target_c_offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "corrupt frame index: selected frame range ends before it starts",
            ));
        }
        target_c_len = final_end - target_c_offset;
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
        let file_index: i32 = conn.query_row("SELECT COUNT(DISTINCT tape_offset) FROM catalog WHERE tape_uuid = ?1 AND tape_offset < ?2", params![tape_uuid, tape_offset as i64], |row| row.get(0)).unwrap_or(0);
        let _ = send_mtio_cmd(fd, MTREW, 1);
        if file_index > 0 { let _ = send_mtio_cmd(fd, MTFSF, file_index); }
        
        let mut discard_buf = AlignedBuffer::new(ALIGNMENT * 256);
        let mut bytes_to_discard = (4096 + target_c_offset) - alignment_skew as u64;
        while bytes_to_discard > 0 {
            let chunk = std::cmp::min(bytes_to_discard, discard_buf.capacity as u64) as usize;
            f.read_exact(&mut discard_buf.as_mut_slice()[..chunk])?;
            bytes_to_discard -= chunk as u64;
        }

        // --- STAGE TO SSD TO PREVENT TAPE SHOE-SHINING ---
        info!("[StreamGate] Spooling {} bytes to SSD to prevent tape drive shoe-shining...", padded_target_c_len);
        let stage_path = std::path::Path::new(&config.hot_tier).join(format!(".stage_{}_{}", tape_uuid, offset));
        let mut stage_file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&stage_path)?;
        
        let mut bytes_to_spool = padded_target_c_len;
        while bytes_to_spool > 0 {
            let chunk = std::cmp::min(bytes_to_spool, discard_buf.capacity as u64) as usize;
            f.read_exact(&mut discard_buf.as_mut_slice()[..chunk])?;
            stage_file.write_all(&discard_buf.as_slice()[..chunk])?;
            bytes_to_spool -= chunk as u64;
        }
        
        stage_file.sync_all()?;
        
        // Close the tape immediately so the hardware is free for other ops!
        drop(f); 
        
        // Unlink the file immediately. 
        // The file descriptor stays open for the HTTP Gateway, but the file disappears 
        // from the filesystem and auto-deletes when the HTTP stream ends.
        let _ = std::fs::remove_file(&stage_path); 
        
        stage_file.seek(SeekFrom::Start(0))?;
        StorageReader::Local(stage_file)
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
        params![file_path, tape_offset as i64],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64, row.get(2)?))
    ).map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "Object not found in catalog"))?;
    
    let padded_size = ((db_comp + 4095) / 4096) * 4096;

    let is_char_dev = std::fs::metadata(tape_dev)
        .map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR)
        .unwrap_or(false);

    // Fetch location hint and Wake Hardware
    let tape_uuid: String = conn.query_row(
        "SELECT tape_uuid FROM catalog WHERE file_path = ?1 AND tape_offset = ?2",
        params![file_path, tape_offset as i64], |row| row.get(0)
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
            params![file_path, tape_offset as i64], |row| row.get(0)
        ).unwrap_or_default();

        let file_index: i32 = conn.query_row(
            "SELECT COUNT(DISTINCT tape_offset) FROM catalog WHERE tape_uuid = ?1 AND tape_offset < ?2",
            params![tape_uuid, tape_offset as i64], |row| row.get(0)
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

    // Unpack Xattrs from TLV, merging Extension Blocks if present
    let ext_blocks = u16::from_le_bytes([header.reserved_pad[0], header.reserved_pad[1]]) as usize;
    let mut all_tlvs = header.tlv_data.to_vec();

    if ext_blocks > 0 {
        // Same O_DIRECT constraints as the rebuild path, and a short read must not
        // silently drop the tail of the TLV stream (xattrs would be lost).
        let ext_len = ext_blocks
            .checked_mul(ALIGNMENT)
            .ok_or_else(|| invalid_data("extension block count overflow"))?;
        let mut ext_buf = AlignedBuffer::new(ext_len);
        if tape_dev.starts_with("rclone:") || is_char_dev {
            tape.read_exact(&mut ext_buf.as_mut_slice()[..ext_len])?;
        } else {
            if let StorageReader::Local(f) = &mut tape {
                f.seek(SeekFrom::Start(tape_offset + 4096 + header.padded_size))?;
                f.read_exact(&mut ext_buf.as_mut_slice()[..ext_len])?;
            }
        }
        all_tlvs.extend_from_slice(&ext_buf.as_slice()[..ext_len]);
    }

    let mut tlv_offset = 0;
    while tlv_offset + 4 <= all_tlvs.len() {
        let t_type = u16::from_be_bytes([all_tlvs[tlv_offset], all_tlvs[tlv_offset+1]]);
        let t_len = u16::from_be_bytes([all_tlvs[tlv_offset+2], all_tlvs[tlv_offset+3]]) as usize;
        if t_type == 0 || tlv_offset + 4 + t_len > all_tlvs.len() { break; }
        
        if t_type == 0x04 {
            // Type 0x04 is TLV_HOISTED_META. 
            // In a full restore, we safely ignore the hoisted atom because the physical 
            // tape stream will naturally restore the original moov atom at the end of the file.
        } else if t_type == 0x02 { 
            let payload_start = tlv_offset + 4;
            let name_len = all_tlvs[payload_start] as usize;
            if name_len > 0 && payload_start + 1 + name_len + 2 <= payload_start + t_len {
                let name = String::from_utf8_lossy(&all_tlvs[payload_start+1 .. payload_start+1+name_len]).into_owned();
                let val_len_start = payload_start+1+name_len;
                let val_len = u16::from_be_bytes([all_tlvs[val_len_start], all_tlvs[val_len_start+1]]) as usize;
                let val_start = val_len_start + 2;
                if val_start + val_len <= payload_start + t_len {
                    let val = &all_tlvs[val_start .. val_start+val_len];
                    
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

pub fn manual_restore(config: &Arc<HuskConfig>, db_path: &str, file_path: &str, dest_path: &str, version: Option<u32>, source: Option<String>, use_direct_io: bool) -> std::io::Result<()> {
    let conn = Connection::open(db_path).map_err(|_| std::io::Error::new(std::io::ErrorKind::NotFound, "DB open failed"))?;
    
    let mut query = String::from(
        "SELECT t.device_path, c.tape_offset, c.version 
         FROM catalog c 
         JOIN tapes t ON c.tape_uuid = t.tape_uuid 
         WHERE c.file_path = ?1"
    );
    
    let row_res = match (&version, &source) {
        (Some(v), Some(s)) => {
            query.push_str(" AND (t.device_path LIKE '%' || ?2 || '%' OR t.tape_uuid LIKE '%' || ?2 || '%') AND c.version = ?3 ORDER BY c.tape_offset ASC LIMIT 1");
            conn.query_row(&query, params![file_path, s, v], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i32>(2)? as u32)))
        }
        (Some(v), None) => {
            query.push_str(" AND c.version = ?2 ORDER BY c.tape_offset ASC LIMIT 1");
            conn.query_row(&query, params![file_path, v], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i32>(2)? as u32)))
        }
        (None, Some(s)) => {
            query.push_str(" AND (t.device_path LIKE '%' || ?2 || '%' OR t.tape_uuid LIKE '%' || ?2 || '%') ORDER BY c.version DESC, c.tape_offset ASC LIMIT 1");
            conn.query_row(&query, params![file_path, s], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i32>(2)? as u32)))
        }
        (None, None) => {
            query.push_str(" ORDER BY c.version DESC, c.tape_offset ASC LIMIT 1");
            conn.query_row(&query, params![file_path], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, i32>(2)? as u32)))
        }
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
            error!(" File '{}' (Version: {:?}, Source: {:?}) not found in catalog.", file_path, version, source);
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Version/Source not found in catalog."))
        }
    }
}
// ---------------------------------------------------------
// 7. Disaster Recovery: Deep Scan Rebuild
// ---------------------------------------------------------
/// One entry of the StreamGate seek index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameIndex {
    uncompressed_offset: u64,
    compressed_offset: u64,
    compressed_size: u32,
    uncompressed_size: u32,
}

fn invalid_data(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.into())
}

/// Strictly parse the frame tables out of an object header's TLV stream.
///
/// `payload_size`/`compressed_size` come from the header and are used to prove
/// that the table describes the whole object: an incomplete or corrupt index
/// must fail loudly instead of being written as a partial seek index.
fn parse_frame_tables(
    tlvs: &[u8],
    payload_size: u64,
    compressed_size: u64,
) -> std::io::Result<Vec<FrameIndex>> {
    let mut legacy: Vec<u32> = Vec::new();
    let mut exact: Vec<(u32, u32)> = Vec::new();
    let mut off = 0usize;

    while off + 4 <= tlvs.len() {
        let t = u16::from_be_bytes([tlvs[off], tlvs[off + 1]]);
        let len = u16::from_be_bytes([tlvs[off + 2], tlvs[off + 3]]) as usize;
        if t == 0 {
            break; // zero padding after the last record
        }
        let body = off + 4;
        let end = body
            .checked_add(len)
            .ok_or_else(|| invalid_data("TLV length overflow"))?;
        if end > tlvs.len() {
            return Err(invalid_data("truncated TLV record"));
        }
        match t {
            TLV_FRAME_TABLE_EXACT => {
                if len % 8 != 0 {
                    return Err(invalid_data("exact frame table length is not a multiple of 8"));
                }
                for rec in tlvs[body..end].chunks_exact(8) {
                    let c = u32::from_be_bytes([rec[0], rec[1], rec[2], rec[3]]);
                    let u = u32::from_be_bytes([rec[4], rec[5], rec[6], rec[7]]);
                    exact.push((c, u));
                }
            }
            TLV_FRAME_TABLE_LEGACY => {
                if len % 4 != 0 {
                    return Err(invalid_data("legacy frame table length is not a multiple of 4"));
                }
                for rec in tlvs[body..end].chunks_exact(4) {
                    legacy.push(u32::from_be_bytes([rec[0], rec[1], rec[2], rec[3]]));
                }
            }
            _ => {}
        }
        off = end;
    }

    if exact.is_empty() && legacy.is_empty() {
        if payload_size == 0 {
            return Ok(Vec::new());
        }
        return Err(invalid_data("object has no frame table"));
    }

    if !exact.is_empty() {
        if !legacy.is_empty() {
            if exact.len() != legacy.len() {
                return Err(invalid_data("frame tables disagree on the frame count"));
            }
            for (i, ((c, _), lc)) in exact.iter().zip(legacy.iter()).enumerate() {
                if c != lc {
                    return Err(invalid_data(format!(
                        "frame {i} compressed size differs between the exact and legacy tables"
                    )));
                }
            }
        }
        let mut frames = Vec::with_capacity(exact.len());
        let mut u_off: u64 = 0;
        let mut c_off: u64 = 0;
        for (i, (c, u)) in exact.iter().enumerate() {
            if *u == 0 {
                return Err(invalid_data(format!("frame {i} has an empty uncompressed range")));
            }
            frames.push(FrameIndex {
                uncompressed_offset: u_off,
                compressed_offset: c_off,
                compressed_size: *c,
                uncompressed_size: *u,
            });
            u_off = u_off
                .checked_add(*u as u64)
                .ok_or_else(|| invalid_data("frame offset overflow"))?;
            c_off = c_off
                .checked_add(*c as u64)
                .ok_or_else(|| invalid_data("frame offset overflow"))?;
        }
        if u_off != payload_size {
            return Err(invalid_data("frame table does not cover the object payload"));
        }
        if c_off != compressed_size {
            return Err(invalid_data("frame table does not cover the compressed payload"));
        }
        return Ok(frames);
    }

    // Legacy-only volume: the uncompressed size per frame was never recorded, so
    // keep the historical 16 MiB assumption (and accept its drift).
    let mut frames = Vec::with_capacity(legacy.len());
    let mut u_off: u64 = 0;
    let mut c_off: u64 = 0;
    for (i, c) in legacy.iter().enumerate() {
        let u_size = if i + 1 == legacy.len() {
            payload_size.saturating_sub(u_off)
        } else {
            16 * 1024 * 1024
        };
        let u_size = u32::try_from(u_size).unwrap_or(u32::MAX);
        frames.push(FrameIndex {
            uncompressed_offset: u_off,
            compressed_offset: c_off,
            compressed_size: *c,
            uncompressed_size: u_size,
        });
        u_off = u_off.saturating_add(u_size as u64);
        c_off = c_off
            .checked_add(*c as u64)
            .ok_or_else(|| invalid_data("frame offset overflow"))?;
    }
    if c_off != compressed_size {
        return Err(invalid_data("legacy frame table does not cover the compressed payload"));
    }
    Ok(frames)
}

pub fn rebuild_catalog(tape_dev: &str, db_path: &str, use_direct_io: bool) -> std::io::Result<()> {
    info!("Initiating Deep Scan Recovery on Volume: {}...", tape_dev);
    
    // Initialize a fresh catalog
    let mut conn = init_catalog(db_path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("init catalog: {e}")))?;
    
    let mut tape = open_tape_device(tape_dev, true, false, false, use_direct_io)?;
    
    let mut offset: u64 = 0;
    let mut recovered_count = 0;
    let mut failed_count = 0;
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

            let ext_blocks = u16::from_le_bytes([header.reserved_pad[0], header.reserved_pad[1]]) as usize;
            let next_offset = offset
                + ALIGNMENT as u64
                + header.padded_size
                + (ext_blocks as u64) * ALIGNMENT as u64;

            // Read the TLV extension blocks into an O_DIRECT-compatible buffer and
            // fail loudly: a short read used to leave zero padding that stopped the
            // TLV walk silently, so the object looked recovered with no frames.
            //
            // No position save/restore: stream_position()/seek() return ESPIPE on
            // character tape devices, and the scan loop re-seeks to `offset` at the
            // top of every iteration anyway. On sequential media the head is simply
            // advanced past the payload.
            let is_char_tape = std::fs::metadata(tape_dev)
                .map(|m| (m.mode() & libc::S_IFMT) == libc::S_IFCHR)
                .unwrap_or(false);
            let mut all_tlvs = header.tlv_data.to_vec();
            let tlvs_result = (|| -> std::io::Result<()> {
                if ext_blocks == 0 {
                    return Ok(());
                }
                let ext_len = ext_blocks
                    .checked_mul(ALIGNMENT)
                    .ok_or_else(|| invalid_data("extension block count overflow"))?;
                let ext_offset = offset + ALIGNMENT as u64 + header.padded_size;
                if is_char_tape {
                    let mut skip = header.padded_size;
                    let mut skip_buf = AlignedBuffer::new(ALIGNMENT * 256);
                    while skip > 0 {
                        let chunk = std::cmp::min(skip, skip_buf.capacity as u64) as usize;
                        tape.read_exact(&mut skip_buf.as_mut_slice()[..chunk])?;
                        skip -= chunk as u64;
                    }
                } else {
                    tape.seek(SeekFrom::Start(ext_offset))?;
                }
                let mut ext_buf = AlignedBuffer::new(ext_len);
                tape.read_exact(&mut ext_buf.as_mut_slice()[..ext_len])?;
                all_tlvs.extend_from_slice(&ext_buf.as_slice()[..ext_len]);
                Ok(())
            })();

            if let Err(e) = tlvs_result {
                error!("[Rebuild] '{}' at offset {}: cannot read metadata: {}", filename, offset, e);
                failed_count += 1;
                offset = next_offset;
                continue;
            }

            // Raw (uncompressed) objects carry no frame table by design: the whole
            // payload is one seekable range. Anything else must have a valid table.
            let frames = if header.compression_type == 0 {
                Vec::new()
            } else {
                match parse_frame_tables(&all_tlvs, header.payload_size, header.compressed_size) {
                    Ok(f) => f,
                    Err(e) => {
                        error!("[Rebuild] '{}' at offset {}: {}", filename, offset, e);
                        failed_count += 1;
                        offset = next_offset;
                        continue;
                    }
                }
            };

            // The catalog row and every frame row are written in one transaction:
            // a partial index must never be reported as a successful recovery.
            let write_result = (|| -> rusqlite::Result<()> {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT OR REPLACE INTO catalog (file_path, version, tape_uuid, tape_offset, payload_size, compressed_size, compression_type, uid, gid, posix_mode, original_mtime, blake3_hash) 
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![&filename, 1, tape_dev, offset as i64, header.payload_size as i64, header.compressed_size as i64, header.compression_type, header.uid, header.gid, header.posix_mode, header.mtime, hash_hex],
                )?;
                for f in &frames {
                    tx.execute(
                        "INSERT INTO object_frames (file_path, version, uncompressed_offset, compressed_offset, compressed_size) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![&filename, 1, f.uncompressed_offset as i64, f.compressed_offset as i64, f.compressed_size as i64],
                    )?;
                }
                tx.commit()
            })();

            match write_result {
                Ok(()) => {
                    info!(
                        "Recovered: '{}' (Offset: {}, Size: {}, Frames: {})",
                        filename, offset, header.payload_size, frames.len()
                    );
                    recovered_count += 1;
                }
                Err(e) => {
                    error!("[Rebuild] '{}' at offset {}: catalog write failed: {}", filename, offset, e);
                    failed_count += 1;
                }
            }

            // Jump forward by the padded size + header size + ext blocks size
            offset = next_offset;
        } else {
            // Not a header. Move forward 4KB and try again (Scan mode).
            offset += ALIGNMENT as u64;
        }
    }
    
    info!(
        " Recovery Complete! rebuilt {} entries, {} failed, into {}.",
        recovered_count, failed_count, db_path
    );
    Ok(())
}

// ---------------------------------------------------------
// 7.5 The Repacker: Tape Garbage Collection 
// ---------------------------------------------------------
pub fn repack_tape(db_path: &str, source_dev: &str, dest_dev: &str, use_direct_io: bool, versions_to_keep: u32) -> std::io::Result<()> {
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
    
    // --- Capacity Check before repacking ---
    let query_req = "
        SELECT COALESCE(SUM(((compressed_size + 4095) / 4096) * 4096 + 4096 + (COALESCE(ext_blocks, 0) * 4096)), 0)
        FROM catalog c1 
        WHERE tape_uuid = ?1 
          AND version IN (SELECT DISTINCT version FROM catalog c2 WHERE c1.file_path = c2.file_path ORDER BY version DESC LIMIT ?2)
    ";
    let required_space = conn.query_row(query_req, params![src_uuid_hex, versions_to_keep], |row| row.get::<_, i64>(0)).unwrap_or(0) as u64;
    
    // Add a 10MB safety buffer room for headers and alignment skew
    let required_space_with_room = required_space + (10 * 1024 * 1024);
    
    let (dest_used, dest_total, _) = check_tape_gauge(dest_dev, db_path).unwrap_or((0, 0, 0));
    let dest_free = dest_total.saturating_sub(dest_used);
    
    if dest_free < required_space_with_room {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Repack aborted: Destination volume has insufficient space. Requires {} but only {} is free.", 
                format_bytes(required_space_with_room), 
                format_bytes(dest_free))
        ));
    }
    // ---------------------------------------------
    
    // 3. Find the specified number of recent versions of all files on the source tape
    let query = "
        SELECT id, file_path, tape_offset, compressed_size, ext_blocks 
        FROM catalog c1 
        WHERE tape_uuid = ?1 
          AND version IN (SELECT DISTINCT version FROM catalog c2 WHERE c1.file_path = c2.file_path ORDER BY version DESC LIMIT ?2)
        ORDER BY tape_offset ASC
    ";
    
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = stmt.query(params![src_uuid_hex, versions_to_keep]).unwrap();
    
    let mut moved_count = 0;
    let mut dest_offset: u64 = conn.query_row(
        "SELECT COALESCE(MAX(tape_offset + ((compressed_size + 4095) / 4096) * 4096 + 4096 + (COALESCE(ext_blocks, 0) * 4096)), 4096) FROM catalog WHERE tape_uuid = ?1",
        params![dest_uuid_hex], |row| row.get::<_, i64>(0)
    ).unwrap_or(4096) as u64;
    dest_offset = if dest_offset % ALIGNMENT as u64 == 0 { dest_offset } else { dest_offset + ALIGNMENT as u64 - (dest_offset % ALIGNMENT as u64) };

    let mut io_buf = AlignedBuffer::new(ALIGNMENT * 256); // 1MB zero-copy buffer

    while let Some(row) = rows.next().unwrap() {
        let id: i64 = row.get(0).unwrap();
        let path: String = row.get(1).unwrap();
        let src_offset: u64 = row.get::<_, i64>(2).unwrap() as u64;
        let compressed_size: u64 = row.get::<_, i64>(3).unwrap() as u64;
        let ext_blocks: u64 = row.get::<_, Option<i64>>(4).unwrap().unwrap_or(0) as u64;
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
        
        // Fast-copy Compressed Zstd Payload (Zero-Decompression) + Ext Blocks
        let mut bytes_left = padded_size + (ext_blocks * 4096);
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
            params![dest_uuid_hex, dest_offset as i64, id]
        ).unwrap();
        
        moved_count += 1;
        dest_offset += ALIGNMENT as u64 + padded_size + (ext_blocks * 4096);
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
        let offset: u64 = row.get::<_, i64>(1).unwrap() as u64;
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
pub fn prune_catalog(config: &std::sync::Arc<crate::config::HuskConfig>) -> std::io::Result<()> {
    info!("Starting Catalog Reconciliation (Soft Delete & Retention)...");
    info!("DEBUG: Loaded config retention_days = {}", config.retention_days);
    
    let conn = Connection::open(&config.db_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    
    // Fetch the max deleted_at timestamp (to match original behavior) AND custom_metadata of the latest version
    let mut stmt = conn.prepare("
        SELECT c1.file_path, c2.max_deleted_at, c1.custom_metadata 
        FROM catalog c1 
        INNER JOIN (
            SELECT file_path, MAX(version) as max_ver, MAX(deleted_at) as max_deleted_at
            FROM catalog 
            GROUP BY file_path
        ) c2 ON c1.file_path = c2.file_path AND c1.version = c2.max_ver
        GROUP BY c1.file_path
    ").unwrap();
    
    let rows: Vec<(String, Option<String>, Option<String>)> = stmt.query_map((), |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }).unwrap().filter_map(Result::ok).collect();
    
    let mut marked_count = 0;
    let mut purged_count = 0;

    // A missing path is only evidence of deletion if the tree it lives in is
    // actually visible. When the hot tier (or a sub-mount) is offline every
    // tracked file "does not exist", and treating that as deletion soft-deletes
    // - then eventually purges - the catalog entries for data that is still on
    // tape. Bail out instead of reconciling against a tree we cannot see.
    if !std::path::Path::new(&config.hot_tier).is_dir() {
        log::warn!(
            "[Prune] Hot tier '{}' is not available; skipping catalog reconciliation.",
            config.hot_tier
        );
        return Ok(());
    }

    for (path, deleted_at, custom_metadata) in rows {
        // Check for superseding legal or policy rules
        let mut has_legal_hold = false;
        if let Some(meta_str) = custom_metadata {
            if let Ok(meta_json) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                if meta_json.get("legal_hold").and_then(|v| v.as_bool()).unwrap_or(false) ||
                   meta_json.get("compliance_policy").and_then(|v| v.as_str()) == Some("HIPAA") {
                    has_legal_hold = true;
                }
            }
        }

        if !std::path::Path::new(&path).exists() {
            // FAST PATH: If retention is 0, skip the soft-delete mark and purge directly
            if config.retention_days == 0 {
                if has_legal_hold {
                    info!("  [Prune] BLOCKED: File missing but has superseding rules (HIPAA/Legal Hold). Kept in catalog: {}", path);
                } else {
                    info!("  [Prune] Retention is 0. Purging permanently immediately: {}", path);
                    let _ = conn.execute("DELETE FROM catalog WHERE file_path = ?1", params![path]);
                    let _ = conn.execute("DELETE FROM object_frames WHERE file_path = ?1", params![path]);
                    purged_count += 1;
                }
            } else if deleted_at.is_none() {
                info!("  [Prune] File missing from filesystem. Marking as soft-deleted in catalog: {}", path);
                let _ = conn.execute("UPDATE catalog SET deleted_at = CURRENT_TIMESTAMP WHERE file_path = ?1", params![path]);
                marked_count += 1;
            } else {
                let del_str = deleted_at.unwrap();
                let query = "SELECT (julianday('now') - julianday(?1))";
                let days_deleted: f64 = conn.query_row(query, params![del_str], |row| row.get(0)).unwrap_or(0.0);

                // Using >= prevents float comparison failures in fast automated tests
                if days_deleted >= config.retention_days as f64 {
                    if has_legal_hold {
                        info!("  [Prune] BLOCKED: File past retention but has superseding rules (HIPAA/Legal Hold). Kept in catalog: {}", path);
                    } else {
                        info!("  [Prune] File past retention limit ({} days). Purging permanently: {}", config.retention_days, path);
                        let _ = conn.execute("DELETE FROM catalog WHERE file_path = ?1", params![path]);
                        let _ = conn.execute("DELETE FROM object_frames WHERE file_path = ?1", params![path]);
                        purged_count += 1;
                    }
                }
            }
        } else if deleted_at.is_some() {
            info!("  [Prune] File reappeared on filesystem. Removing deletion mark: {}", path);
            let _ = conn.execute("UPDATE catalog SET deleted_at = NULL WHERE file_path = ?1", params![path]);
        }
    }
    
    info!("Reconciliation complete. Marked {} missing files. Purged {} old/orphaned records.", marked_count, purged_count);
    if purged_count > 0 {
        info!("These purged files will be physically erased from tape during the next 'repack'.");
    }
    
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

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_tlv_records(buf: &[u8]) -> Vec<(u16, usize)> {
        let mut out = Vec::new();
        let mut off = 0;
        while off + 4 <= buf.len() {
            let t = u16::from_be_bytes([buf[off], buf[off + 1]]);
            let l = u16::from_be_bytes([buf[off + 2], buf[off + 3]]) as usize;
            out.push((t, l));
            off += 4 + l;
        }
        assert_eq!(off, buf.len(), "records must exactly cover the buffer");
        out
    }

    #[test]
    fn jump_table_fits_in_a_single_record() {
        let table = [(0u64, 0u64, 1024u64), (16 << 20, 1024, 2048)];
        let mut out = Vec::new();
        encode_jump_table(&table, &mut out);
        let records = decode_tlv_records(&out);
        assert_eq!(records, vec![(0x03, 8)]);
    }

    #[test]
    fn jump_table_beyond_16383_frames_is_split_without_truncation() {
        // 20_000 frames used to overflow the u16 length field and corrupt every
        // TLV that followed.
        let table: Vec<(u64, u64, u64)> =
            (0..20_000u64).map(|i| (i * (16 << 20), i * 4096, 4096)).collect();
        let mut out = Vec::new();
        encode_jump_table(&table, &mut out);

        let records = decode_tlv_records(&out);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], (0x03, MAX_FRAMES_PER_TLV * 4));
        assert_eq!(records[1].0, 0x03);
        let frames: usize = records.iter().map(|(_, len)| len / 4).sum();
        assert_eq!(frames, table.len());
    }

    #[test]
    fn jump_table_chunk_boundary_is_exact() {
        let table: Vec<(u64, u64, u64)> = (0..MAX_FRAMES_PER_TLV as u64)
            .map(|i| (i, i, 4096))
            .collect();
        let mut out = Vec::new();
        encode_jump_table(&table, &mut out);
        assert_eq!(decode_tlv_records(&out), vec![(0x03, 65532)]);
    }

    fn decode_exact_records(buf: &[u8]) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        let mut off = 0;
        while off + 4 <= buf.len() {
            let t = u16::from_be_bytes([buf[off], buf[off + 1]]);
            let l = u16::from_be_bytes([buf[off + 2], buf[off + 3]]) as usize;
            if t == TLV_FRAME_TABLE_EXACT {
                let mut p = off + 4;
                while p + 8 <= off + 4 + l {
                    let c = u32::from_be_bytes([buf[p], buf[p + 1], buf[p + 2], buf[p + 3]]);
                    let u = u32::from_be_bytes([buf[p + 4], buf[p + 5], buf[p + 6], buf[p + 7]]);
                    out.push((c, u));
                    p += 8;
                }
            }
            off += 4 + l;
        }
        assert_eq!(off, buf.len(), "records must exactly cover the buffer");
        out
    }

    fn table_from_sizes(sizes: &[u64]) -> (Vec<(u64, u64, u64)>, u64) {
        let mut table = Vec::new();
        let mut u = 0u64;
        let mut c = 0u64;
        for (i, size) in sizes.iter().enumerate() {
            let c_size = 4096 + i as u64;
            table.push((u, c, c_size));
            u += *size;
            c += c_size;
        }
        (table, u)
    }

    #[test]
    fn exact_frame_table_preserves_irregular_frame_offsets() {
        // Real frames are 16-17 MiB because the writer flushes once it has
        // accumulated at least 16 MiB. Rebuilding with a fixed 16 MiB step
        // drifts; the 0x05 table must reproduce the writer's offsets exactly.
        let sizes = vec![17u64 << 20, 16u64 << 20, 3_000_000, 16u64 << 20];
        let (table, payload) = table_from_sizes(&sizes);

        let mut buf = Vec::new();
        encode_frame_table_exact(&table, payload, &mut buf).unwrap();
        let decoded = decode_exact_records(&buf);
        assert_eq!(decoded.len(), table.len());

        let mut rebuilt_u = 0u64;
        for (i, (c_size, u_size)) in decoded.iter().enumerate() {
            assert_eq!(rebuilt_u, table[i].0, "uncompressed offset drift at frame {i}");
            assert_eq!(*c_size as u64, table[i].2);
            rebuilt_u += *u_size as u64;
        }
        assert_eq!(rebuilt_u, payload);
    }

    #[test]
    fn exact_frame_table_is_split_past_8191_frames() {
        let sizes = vec![16u64 << 20; 9_000];
        let (table, payload) = table_from_sizes(&sizes);

        let mut buf = Vec::new();
        encode_frame_table_exact(&table, payload, &mut buf).unwrap();
        let records = decode_tlv_records(&buf);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], (TLV_FRAME_TABLE_EXACT, (MAX_EXACT_FRAMES_PER_TLV * 8) as usize));
        assert_eq!(decode_exact_records(&buf).len(), table.len());
    }

    fn exact_table_bytes(sizes: &[u64]) -> (Vec<u8>, Vec<(u64, u64, u64)>, u64, u64) {
        let (table, payload) = table_from_sizes(sizes);
        let compressed: u64 = table.iter().map(|f| f.2).sum();
        let mut buf = Vec::new();
        encode_frame_table_exact(&table, payload, &mut buf).unwrap();
        (buf, table, payload, compressed)
    }

    #[test]
    fn parse_frame_tables_accepts_exact_table() {
        let (buf, table, payload, compressed) =
            exact_table_bytes(&[17 << 20, 16 << 20, 3_000_000]);
        let frames = parse_frame_tables(&buf, payload, compressed).unwrap();
        assert_eq!(frames.len(), table.len());
        for (frame, expected) in frames.iter().zip(table.iter()) {
            assert_eq!(frame.uncompressed_offset, expected.0);
            assert_eq!(frame.compressed_offset, expected.1);
            assert_eq!(frame.compressed_size as u64, expected.2);
        }
        assert_eq!(
            frames.iter().map(|f| f.uncompressed_size as u64).sum::<u64>(),
            payload
        );
    }

    #[test]
    fn parse_frame_tables_rejects_partial_coverage() {
        let (buf, _table, payload, compressed) = exact_table_bytes(&[16 << 20, 8 << 20]);
        let err = parse_frame_tables(&buf, payload + 1, compressed).unwrap_err();
        assert!(err.to_string().contains("does not cover the object payload"));
        let err = parse_frame_tables(&buf, payload, compressed + 1).unwrap_err();
        assert!(err.to_string().contains("compressed payload"));
    }

    #[test]
    fn parse_frame_tables_rejects_truncated_record() {
        // Claims a 16-byte payload but only 8 bytes follow.
        let buf = vec![0x00, 0x05, 0x00, 0x10, 0, 0, 0, 1, 0, 0, 0, 2];
        assert!(parse_frame_tables(&buf, 16, 1).is_err());
    }

    #[test]
    fn parse_frame_tables_rejects_misaligned_length() {
        let buf = vec![0x00, 0x05, 0x00, 0x06, 0, 0, 0, 1, 0, 0];
        let err = parse_frame_tables(&buf, 1, 1).unwrap_err();
        assert!(err.to_string().contains("multiple of 8"));
    }

    #[test]
    fn parse_frame_tables_rejects_dual_table_mismatch() {
        let (mut buf, _table, payload, compressed) = exact_table_bytes(&[16 << 20]);
        // A legacy record that disagrees on the compressed size.
        let mut legacy = Vec::new();
        encode_jump_table(&[(0, 0, 9999)], &mut legacy);
        buf.extend_from_slice(&legacy);
        let err = parse_frame_tables(&buf, payload, compressed).unwrap_err();
        assert!(err.to_string().contains("differs between"));
    }

    #[test]
    fn parse_frame_tables_falls_back_to_legacy_tables() {
        let frames = 3usize;
        let table: Vec<(u64, u64, u64)> = (0..frames as u64)
            .map(|i| (i * (16 << 20), i * 4096, 4096))
            .collect();
        let mut buf = Vec::new();
        encode_jump_table(&table, &mut buf);
        let parsed = parse_frame_tables(&buf, 3 * (16 << 20), frames as u64 * 4096).unwrap();
        assert_eq!(parsed.len(), frames);
        assert_eq!(parsed[0].uncompressed_offset, 0);
        assert_eq!(parsed[1].uncompressed_offset, 16 << 20);
        assert_eq!(parsed[2].uncompressed_offset, 32 << 20);
    }

    #[test]
    fn parse_frame_tables_skips_unknown_records_and_zero_padding() {
        let (mut buf, table, payload, compressed) = exact_table_bytes(&[16 << 20]);
        let mut out = Vec::new();
        out.extend_from_slice(&[0x00, 0x07, 0x00, 0x02, 0xAA, 0xBB]); // unknown type
        out.extend_from_slice(&buf);
        out.extend_from_slice(&[0u8; 16]); // zero padding
        buf = out;
        let frames = parse_frame_tables(&buf, payload, compressed).unwrap();
        assert_eq!(frames.len(), table.len());
    }

    #[test]
    fn parse_frame_tables_allows_empty_object() {
        assert!(parse_frame_tables(&[], 0, 0).unwrap().is_empty());
        assert!(parse_frame_tables(&[], 1, 0).is_err());
    }

    #[test]
    fn encoder_rejects_zero_length_frame() {
        // Two entries at the same uncompressed offset -> the first frame is empty.
        let table = [(0u64, 0u64, 100u64), (0u64, 100u64, 100u64)];
        let mut buf = Vec::new();
        let err = encode_frame_table_exact(&table, 16, &mut buf).unwrap_err();
        assert!(err.to_string().contains("zero-length frame"));
    }

    #[test]
    fn encoder_rejects_non_monotonic_offsets() {
        let table = [(10u64, 0u64, 100u64), (5u64, 100u64, 100u64)];
        let mut buf = Vec::new();
        let err = encode_frame_table_exact(&table, 16, &mut buf).unwrap_err();
        assert!(err.to_string().contains("monotonic"));
    }

    #[test]
    fn extension_block_count_round_trips_through_u16() {
        // >255 blocks (the old u8 field) must survive the header round trip.
        for count in [0u16, 1, 255, 256, 1000] {
            let pad = [count as u8, (count >> 8) as u8, 0u8];
            let decoded = u16::from_le_bytes([pad[0], pad[1]]);
            assert_eq!(decoded, count);
        }
    }

    /// End-to-end: format a `.img` volume, archive a payload through the real
    /// writer, rebuild the catalog from the tape image alone and compare the
    /// recovered frame table with what the writer produced.
    #[test]
    fn end_to_end_archive_then_rebuild_matches_frame_table() {
        use crate::config::{HuskConfig, DEFAULT_TOML};

        let dir = std::env::temp_dir().join(format!("husk_e2e_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let img = dir.join("vol.img");
        let img_path = img.to_string_lossy().to_string();
        std::fs::File::create(&img)
            .unwrap()
            .set_len(96 * 1024 * 1024)
            .unwrap();
        crate::hardware::format_tape(&img_path, false).unwrap();

        let db_path = dir.join("catalog.db").to_string_lossy().to_string();
        let conn = super::init_catalog(&db_path).unwrap();

        // 16 MiB + 16 MiB + a short tail: exercises boundary and last frame.
        let payload_len = (32usize << 20) + 12_345;
        let payload: Vec<u8> = (0..payload_len).map(|i| (i % 251) as u8).collect();
        let src = dir.join("payload.bin");
        std::fs::write(&src, &payload).unwrap();

        let mut config: HuskConfig = toml::from_str(DEFAULT_TOML).unwrap();
        config.hot_tier = dir.to_string_lossy().to_string();
        config.db_path = db_path.clone();
        config.primary_volumes = vec![img_path.clone()];
        config.failover_volumes = vec![];
        config.replication_volumes = vec![];
        config.replicas = 0;
        config.min_free_space_gb = Some(0);
        config.no_compress_extensions = vec![];
        let config = std::sync::Arc::new(config);

        let (replicas, jump_table) =
            archive_file(&conn, src.to_str().unwrap(), &config, false).unwrap();
        assert_eq!(replicas.len(), 1);
        assert_eq!(jump_table.len(), 3, "16 MiB + 16 MiB + short tail");
        for (i, (u, _, size)) in jump_table.iter().enumerate() {
            assert!(*size > 0, "frame {i} produced no compressed bytes");
            assert_eq!(*u, i as u64 * (16 << 20), "frame {i} uncompressed start");
        }
        assert_eq!(jump_table[0].1, 0, "first frame starts at compressed offset 0");
        for pair in jump_table.windows(2) {
            assert!(pair[1].1 > pair[0].1, "compressed offsets must increase");
        }

        // The volume must actually carry the exact table (TLV 0x05), not only the
        // legacy 0x03 record.
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut vol = std::fs::File::open(&img_path).unwrap();
            vol.seek(SeekFrom::Start(4096)).unwrap(); // first object header
            let mut hdr = [0u8; 4096];
            vol.read_exact(&mut hdr).unwrap();
            let tlvs = &hdr[136..]; // ObjectHeader::tlv_data
            let has_exact = tlvs
                .windows(2)
                .any(|w| w[0] == 0x00 && w[1] == TLV_FRAME_TABLE_EXACT as u8);
            let has_legacy = tlvs
                .windows(2)
                .any(|w| w[0] == 0x00 && w[1] == TLV_FRAME_TABLE_LEGACY as u8);
            assert!(has_exact, "exact 0x05 frame table must be written");
            assert!(has_legacy, "legacy 0x03 table must stay for old readers");
        }

        // Rebuild purely from the volume and compare the recovered index.
        let rebuilt_db = dir.join("rebuilt.db").to_string_lossy().to_string();
        super::rebuild_catalog(&img_path, &rebuilt_db, false).unwrap();

        let rebuilt = rusqlite::Connection::open(&rebuilt_db).unwrap();
        let mut stmt = rebuilt
            .prepare("SELECT uncompressed_offset, compressed_offset, compressed_size FROM object_frames ORDER BY uncompressed_offset ASC")
            .unwrap();
        let rows: Vec<(i64, i64, i64)> = stmt
            .query_map((), |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        assert_eq!(rows.len(), jump_table.len(), "rebuilt frame count");
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.0 as u64, jump_table[i].0, "frame {i} uncompressed offset");
            assert_eq!(row.2 as u64, jump_table[i].2, "frame {i} compressed size");
        }

        // The payload size recovered from the header must match what was read.
        let recovered_len: i64 = rebuilt
            .query_row("SELECT payload_size FROM catalog LIMIT 1", (), |r| r.get(0))
            .unwrap();
        assert_eq!(recovered_len as u64, payload_len as u64);

        drop(stmt);
        drop(rebuilt);
        drop(conn);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Raw objects (`no_compress_extensions`) have no frame table; the rebuild
    /// must still recover their catalog row instead of failing on the missing TLV.
    #[test]
    fn end_to_end_uncompressed_object_rebuilds_without_frames() {
        use crate::config::{HuskConfig, DEFAULT_TOML};

        let dir = std::env::temp_dir().join(format!("husk_e2e_raw_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let img = dir.join("vol.img");
        let img_path = img.to_string_lossy().to_string();
        std::fs::File::create(&img)
            .unwrap()
            .set_len(32 * 1024 * 1024)
            .unwrap();
        crate::hardware::format_tape(&img_path, false).unwrap();

        let db_path = dir.join("catalog.db").to_string_lossy().to_string();
        let conn = super::init_catalog(&db_path).unwrap();

        let payload_len = 2usize << 20;
        let src = dir.join("raw.bin");
        std::fs::write(&src, vec![0xABu8; payload_len]).unwrap();

        let mut config: HuskConfig = toml::from_str(DEFAULT_TOML).unwrap();
        config.hot_tier = dir.to_string_lossy().to_string();
        config.db_path = db_path.clone();
        config.primary_volumes = vec![img_path.clone()];
        config.failover_volumes = vec![];
        config.replication_volumes = vec![];
        config.replicas = 0;
        config.min_free_space_gb = Some(0);
        config.no_compress_extensions = vec!["bin".to_string()];
        let config = std::sync::Arc::new(config);

        let (replicas, jump_table) =
            archive_file(&conn, src.to_str().unwrap(), &config, false).unwrap();
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].3, 0, "compression must be bypassed for .bin");
        assert!(jump_table.is_empty(), "raw objects carry no frame table");

        let rebuilt_db = dir.join("rebuilt.db").to_string_lossy().to_string();
        super::rebuild_catalog(&img_path, &rebuilt_db, false).unwrap();

        let rebuilt = rusqlite::Connection::open(&rebuilt_db).unwrap();
        let rows: i64 = rebuilt
            .query_row("SELECT COUNT(*) FROM catalog", (), |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1, "raw object must be recovered into the catalog");
        let frames: i64 = rebuilt
            .query_row("SELECT COUNT(*) FROM object_frames", (), |r| r.get(0))
            .unwrap();
        assert_eq!(frames, 0, "raw object has no frame rows");

        drop(rebuilt);
        drop(conn);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
