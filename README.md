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

#### Features
*   **StreamGate (Indexed Direct Access):** Zero-Disk extraction. Use a Jump-Table to instantly seek to any byte in a 10TB file stored on Tape or S3 without downloading the whole thing. Pipe directly into `mpv`, `grep`, or `ffmpeg`.
*   **Native SCSI Tape Driver:** Professional-grade support for LTO-5 through LTO-9 drives via `/dev/nstX`. Handles hardware positioning, filemarks, and 256KB block-alignment to prevent "shoe-shining."
*   **N-Way Replication:** Automatically mirror cold data across local drives, physical tapes, and cloud buckets (via rclone) simultaneously.
*   **Point-in-Time Recovery (PITR):** Roll back any file to a previous version using the built-in versioning engine.
*   **Bit-Rot Scrubber:** Cryptographically verify the integrity of offline storage using BLAKE3 hashes.

#### Hardware-Aware Architecture
Modern storage requires specialized handling. HuskHoard treats your media differently based on its physics:
*   **For Physical Tapes:** Writes in 256KB optimal SCSI frames and uses **Filemarks** to navigate. StreamGate uses the catalog to skip hardware blocks, reaching your data in seconds rather than minutes.
*   **For SMR Hard Drives:** Eliminates the "write wall" by using a Strict Log-Structured Format—data is only ever written sequentially.
*   **For Cloud (rclone):** Packs data into 16MB Zstd-compressed frames. This optimizes "PUT" request costs and allows for high-speed partial reads via HTTP Range requests.
*   
### Architecture Overview
*   **The Catalog:** A SQLite "Brain" tracking every file, its version history, and its exact byte-offset on physical media.
*   **The Interceptor:** A lightweight fanotify loop that detects when an application requests a stubbed file and triggers an instant recall.
*   **The Janitor:** A background policy engine that identifies cold data based on age, extension, or directory rules.
*   **The Archive Worker:** The heavy-lifter. It compresses data into seekable frames, multiplexes writes across the storage pool, and manages SCSI hardware commands.

### Sustainability & Drive Longevity
*   **Reduced Duty Cycle:** Batching allows archive drives to stay spun down and idle 99% of the time.
*   **Energy Efficient:** Large collections don't need dozens of drives spinning 24/7. HuskHoard lets them sleep until you hit "Play."

### Hybrid Cloud Replication
Using **rclone** as its transport layer, HuskHoard streams archives to over 40 providers (S3, B2, etc.) in a single pass. Data is packed into optimal 16MB Zstd frames to minimize API "PUT" requests and storage costs.

### 🐧 OS Compatibility & Requirements
HuskHoard relies on the Linux **fanotify** kernel API. It is compatible with almost any modern Linux distribution using **Kernel 5.1 or higher**.

*   **Primary Support:** Ubuntu 22.04 LTS, 24.04 LTS (Recommended)
*   **Enterprise/Server:** Debian 11/12, Rocky Linux 8/9, AlmaLinux 8/9, RHEL 8/9
*   **Desktop/Rolling:** Arch Linux, Fedora 38+, openSUSE Tumbleweed
*   **Incompatible:** WSL2 (Windows Subsystem for Linux), CentOS 7 (Kernel too old), Synology/QNAP (unless using custom kernels).

### 🚀 Quick Start (Ubuntu 24.04)

**⚠️ Important:** Run all commands as your standard user. Do not log in as `root`. HuskHoard is designed to run in user-space.

#### 1. Prerequisites
Install the required system tools and the Rust compiler on Ubuntu 24.04 LTS:

```bash
sudo apt update
sudo apt install -y build-essential rclone libcap2-bin attr pkg-config libsqlite3-dev git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

#### 2. Download and Build
Clone the repository and move into the project directory. **You must remain in this directory for the rest of the tutorial.**

```bash
git clone -b tape https://github.com/huskhoard/huskhoard.git
cd huskhoard

# Build the project
cargo build --release
```

#### 3. Grant Kernel Capabilities
HuskHoard needs specific capabilities to intercept file reads via `fanotify` without needing to run as a dangerous root process. Apply these to the newly built binary:

```bash
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/huskhoard
```

#### 4. Configure Your "Test Environment"
Set up a safe testing area right inside the project folder. We will create a `hot_tier` directory (on your SSD) and a 100MB file to act as your physical "Tape Volume".

```bash
# Ensure you are still in the 'huskhoard' project directory
mkdir -p hot_tier
fallocate -l 100M my_hoard.img
```

Next, format the tape volume. Running this command for the first time will automatically generate a `husk_config.toml` file in your current directory.

```bash
./target/release/huskhoard format --tape-dev my_hoard.img

# OR: Format a physical LTO tape drive
./target/release/huskhoard format --tape-dev /dev/nst0
```

Open the newly generated `husk_config.toml` in your text editor. Update these lines to enable **Instant Archiving** so you can see it work immediately. *(Note: Using absolute paths is highly recommended so the daemon always knows where your data is).*

```toml
primary_volumes = ["/home/YOUR_USERNAME/huskhoard/my_hoard.img"]
watch_dir = "/home/YOUR_USERNAME/huskhoard/hot_tier"  # Ensure this points to your hot tier
max_age_days = 0 # TEST MODE: Archive files immediately
janitor_interval_secs = 10
```

#### 5. Launch the Daemon
Start the HuskHoard background engine:

```bash
./target/release/huskhoard daemon
```

#### 6. Test it
Leave the daemon running and open a **second terminal window**. 

Drop a large file into `hot_tier`. Wait 10 seconds. 
* Run `ls -ls hot_tier`. You will see the file's allocated size drop to 4Kb, while its logical size remains intact. 
* Run `du -h hot_tier`. It has become a Husk. 
* Open the file, and watch the Daemon instantly recall it from `my_hoard.img`.

---


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
