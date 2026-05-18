use bytemuck::{Pod, Zeroable};

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
