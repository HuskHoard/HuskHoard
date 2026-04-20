# HuskHoard

![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)
![Built with Rust](https://img.shields.io/badge/Built_with-Rust-orange.svg)
![platform-Linux](https://img.shields.io/badge/platform-Linux%20-red.svg)

**HuskHoard** is an automated, transparent data-tiering engine for Linux. It turns your expensive NVMe drives into a bottomless file system by silently archiving cold data to cheap hard drives, **Physical LTO Tapes**, or cloud buckets—while keeping the files fully visible and accessible to your OS.

It acts like an Enterprise Tape Library, but built for the modern homelab and data hoarder.

## Why HuskHoard?

Enterprise storage vendors charge thousands of dollars for automated storage tiering and lock your data inside proprietary black boxes. HuskHoard does it for free, right in user-space, using standard open-source formats.

*   **Bring Your Own Hardware:** HuskHoard doesn't care if your "Tape Library" is a $10,000 SAN, a **Physical LTO-9 Tape Drive**, a dusty USB drive, or an Amazon S3 bucket.
*   **Zero-Overhead Transparent Stubbing:** HuskHoard does **not** use FUSE. It uses the Linux `fanotify` kernel API to block and resume processes in real-time.
*   **StreamGate (Direct Streaming):** Watch 4K video or search massive log files directly from Tape or S3 without ever writing the data back to your SSD.
*   **The "Easy Exit" Promise (No Vendor Lock-in):** Payload data is stored in standard **Zstd** streams verified by **BLAKE3**. You can extract your data using only standard Linux tools (`dd` and `zstd`).

## Features

*   **Native SCSI Tape Driver:** Native hardware-level support for LTO-5 through LTO-9 drives via `/dev/nstX`. Handles rewinding, positioning, and filemarks automatically.
*   **StreamGate:** Zero-Disk extraction. Pipe archived files directly into `mpv`, `grep`, or `ffmpeg` without taking up any local disk space.
*   **N-Way Replication:** Automatically mirror cold data across local drives, physical tapes, and cloud buckets simultaneously.
*   **Point-in-Time Recovery (PITR):** Roll back any file to a previous version using the built-in versioning engine.
*   **Bit-Rot Scrubber:** Cryptographically verify the integrity of your offline storage with BLAKE3 hashes.

### Architecture Overview
*   **The Catalog:** A SQLite "Brain" tracking every file, its version history, and its exact byte-offset on physical media.
*   **The Interceptor:** A lightweight fanotify loop that detects when an application requests a stubbed file and triggers an instant recall.
*   **The Janitor:** A background policy engine that identifies cold data based on age, extension, or directory rules.
*   **The Archive Worker:** The heavy-lifter. It compresses data into seekable frames, multiplexes writes across the storage pool, and manages SCSI hardware commands.

---
## Hardware-Aware Architecture

### Supports SMR & LTO Tape Hardware
Modern storage requires specialized handling. HuskHoard uses a **Strict Log-Structured Format**:
*   **For SMR Hard Drives:** Eliminates the "write wall" by writing in one continuous, sequential stream.
*   **For Physical Tapes:** Uses optimal **256KB SCSI Framing** to prevent "shoe-shining" (frequent stopping/starting), maximizing both performance and the lifespan of your tape heads.

### Sustainability & Drive Longevity
*   **Reduced Duty Cycle:** Batching allows archive drives to stay spun down and idle 99% of the time.
*   **Energy Efficient:** Large collections don't need dozens of drives spinning 24/7. HuskHoard lets them sleep until you hit "Play."

### Hybrid Cloud Replication
Using **rclone** as its transport layer, HuskHoard streams archives to over 40 providers (S3, B2, etc.) in a single pass. Data is packed into optimal 16MB Zstd frames to minimize API "PUT" requests and storage costs.

---

## 🚀 Quick Start (Ubuntu 24.04)

### 1. Build and Grant Capabilities
Husk needs specific kernel capabilities to intercept file reads without running as full root.

```bash
cargo build --release
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/huskhoard
```

### 2. Format a Volume (Disk or Tape)
Run this command to initialize a storage target. This also generates your `husk_config.toml`.
```bash
# Format a local disk image
./target/release/huskhoard format --tape-dev my_hoard.img

# OR: Format a physical LTO tape drive
./target/release/huskhoard format --tape-dev /dev/nst0
```

### 3. Launch the Daemon
```bash
./target/release/huskhoard daemon
```

### 🕹️ Command Center

**Stream a file directly from Tape (Zero-Disk):**
```bash
./target/release/huskhoard cat --file /media/movies/scifi.mp4 | mpv -
```

**Check Capacity & "Wasteland" statistics:**
```bash
./target/release/huskhoard info --tape-dev /dev/nst0
```

**Scrub a Volume for Bit-Rot:**
```bash
./target/release/huskhoard scrub --tape-dev my_hoard.img
```

**Repack (Garbage Collect) an old Volume:**
```bash
./target/release/huskhoard repack --source-tape old_drive.img --dest-tape new_drive.img
```

---

### 🚀 Roadmap
*   [In-Progress] **StreamGate HTTP:** A local web-gateway allowing video players to seek through tapes via HTTP Range requests.
*   [Planned] **Web Dashboard:** Real-time visual "Tank Gauge" monitoring.
*   [Planned] **Prometheus Integration:** `/metrics` endpoint for Grafana.

### License
Husk is licensed under the AGPL v3. Infrastructure software should remain free, open, and permanently protected from proprietary exploitation.

For commercial inquiries, contact `info@huskhoard.com`.
