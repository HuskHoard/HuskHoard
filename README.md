
# HuskHoard

![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)
![Built with Rust](https://img.shields.io/badge/Built_with-Rust-orange.svg)
![platform-Linux](https://img.shields.io/badge/platform-Linux%20-red.svg)

**HuskHoard** is an automated, transparent data-tiering engine for Linux. It turns your expensive NVMe drives into a bottomless file system by silently archiving cold data to cheap hard drives(SMR or CMR), LTO Tapes, or cloud buckets, while keeping the files fully visible and accessible to your OS. More info at [www.huskhoard.com](http://www.huskhoard.com)

It acts like an Enterprise Tape Library, but built for the modern hybrid cloud user.

## Why HuskHoard?

Enterprise storage vendors charge thousands of dollars for automated storage tiering and lock your data inside proprietary black boxes. HuskHoard does it for free, right in user-space, using standard open-source formats.

*   **Bring Your Own Hardware:** HuskHoard doesn't care if your "Tape Library" is a $10,000 SAN, a **Physical LTO Tape Drive**, a dusty USB drive, or an Amazon S3 bucket.
*   **Zero-Overhead Transparent Stubbing:** HuskHoard does **not** use FUSE. It uses the Linux `fanotify` kernel API to block and resume processes in real-time.
*   **StreamGate HTTP Gateway:** Watch 4K video directly from Tape or S3 via a local HTTP bridge. This allows Plex, Jellyfin, or VLC to seek through massive files instantly with zero SSD impact.
*   **The "Easy Exit" Promise (No Vendor Lock-in):** Payload data is stored in standard **Zstd** streams verified by **BLAKE3**. Catalog metadata can be exported to **Apache Parquet** for use in external databases.

#### Features
*   **StreamGate (Indexed Direct Access):** Zero-Disk extraction. Use a Jump-Table to instantly seek to any byte in a 10TB file stored on Tape or S3 without downloading the whole thing.
*   **Data Engineering Ready:** Export your entire file catalog to **Apache Parquet**. Perform massive-scale audits, AI tagging, or storage analytics using DuckDB, Python, or Spark.
*   **Native SCSI Tape Driver:** Professional-grade support for LTO-5 through LTO-9 drives via `/dev/nstX`. Handles hardware positioning and 256KB block-alignment to prevent "shoe-shining."
*   **N-Way Replication:** Automatically mirror cold data across local drives, physical tapes, and cloud buckets (via rclone) simultaneously.
*   **High-Water Mark Spillover:** Automatically safeguard your SSD. If the Hot Tier exceeds a threshold (e.g., 80%), HuskHoard triggers an emergency archive cycle.

### Architecture Overview
*   **The Catalog:** A SQLite "Brain" tracking every file, its version history, and its exact byte-offset on physical media.
*   **The Interceptor:** A lightweight fanotify loop that detects when an application requests a stubbed file and triggers an instant recall.
*   **The Janitor:** A background policy engine that identifies cold data based on age, extension, or directory rules.
*   **The Archive Worker:** The heavy-lifter. It compresses data into seekable frames, multiplexes writes across the storage pool, and manages SCSI hardware commands.
*   **More about Architecture:** The blog has a few indepth articles about the architecture. You can read more here www.huskhoard.com/blog.html 

### OS Compatibility & Requirements
HuskHoard relies on the Linux **fanotify** kernel API. It is compatible with modern Linux distributions using **Kernel 5.1 or higher**.

*   **Primary Support:** Ubuntu 22.04 LTS, 24.04 LTS (Recommended)
*   **Enterprise/Server:** Debian 11/12, Rocky/AlmaLinux 8/9, RHEL 8/9
*   **File System:** XFS, ZFS, Ext4, Btrfs
*   **Incompatible:** WSL2 (Windows), CentOS 7 (Kernel too old), Synology/QNAP (unless using custom kernels).

### 🚀 Quick Start (Ubuntu 24.04)

**⚠️ Important:** Run all commands as your standard user. HuskHoard is designed to run in user-space.

#### 1. Prerequisites
```bash
sudo apt update
sudo apt install -y build-essential rclone libcap2-bin attr pkg-config libsqlite3-dev git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

#### 2. Download and Build
```bash
git clone https://github.com/huskhoard/huskhoard.git
cd huskhoard
cargo build --release
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/huskhoard
```

#### 3. Setup Test Environment
```bash
mkdir -p hot_tier
fallocate -l 100M my_archive.img
./target/release/huskhoard format --tape-dev my_archive.img
```

Update your `husk_config.toml` to point to these paths and set `max_age_days = 0` for instant archiving during testing.

#### 4. Launch the Daemon
```bash
./target/release/huskhoard daemon
```

---

### 🕹️ Command Center

**Stream a file directly from Tape (Zero-Disk):**
```bash
./target/release/huskhoard cat --file-path /media/movies/scifi.mp4 | mpv -
```

**Export Catalog for Data Engineering (Parquet):**
```bash
./target/release/huskhoard export --format parquet --output my_catalog.parquet
# Query it instantly with DuckDB:
# duckdb -c "SELECT file_path, payload_size FROM 'my_catalog.parquet' WHERE payload_size > 1e9"
```

**Check Capacity & "Wasteland" statistics:**
```bash
./target/release/huskhoard info --tape-dev /dev/nst0
```

**Scrub a Volume for Bit-Rot:**
```bash
./target/release/huskhoard scrub --tape-dev my_archive.img
```

**Repack (Garbage Collect) an old Volume:**
```bash
./target/release/huskhoard repack --source-tape old_drive.img --dest-tape new_drive.img
```

---

### 🚀 Roadmap
*   [Complete] **StreamGate HTTP:** A local web-gateway allowing video players to seek through tapes via HTTP Range requests.
*   [Complete] **Parquet Export:** Native support for exporting metadata to big-data formats.
*   [Planned] **Web Dashboard:** Real-time visual "Tank Gauge" monitoring.

### License
Husk is licensed under the AGPL v3. Infrastructure software should remain free, open, and permanently protected from proprietary exploitation.

For commercial inquiries, contact `info@huskhoard.com`.
