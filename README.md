
# HuskHoard

![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)
![Built with Rust](https://img.shields.io/badge/Built_with-Rust-orange.svg)
![platform-Linux](https://img.shields.io/badge/platform-Linux%20-red.svg)

> 🇯🇵 **日本のユーザーの皆様へ (To our Japanese users):**  
> We are big in Japan! You can read the Japanese README here: **[日本語のREADMEはこちら](README-ja.md)**.  
> *Note: We are actively looking for Japanese maintainers/translators to help improve our docs! If you're interested, please open a PR or say hello in the issues.*

**HuskHoard** is an open source data tiering archive for Linux. It turns your expensive NVMe drives into a bottomless file system by silently archiving cold data to cheap hard drives ([SMR](https://huskhoard.com/blog-post-smr.html) or [CMR](https://huskhoard.com/blog-post-cmr.html), [LTO Tapes](https://huskhoard.com/blog-post-lto.html), or [cloud buckets](https://huskhoard.com/blog-post-cloud.html)), while keeping the files fully visible and accessible to your OS. It lets you keep a 1 PB filesystem visible to applications while only keeping the data you're actively using on expensive storage.More info at [www.huskhoard.com](http://www.huskhoard.com)

It acts like an Enterprise Tape Library, but built for the modern hybrid cloud user.
Check our [LTO Compatibility list](https://huskhoard.com/compatibility.html) and create your LTO Archive today! Here is a quick [video](https://youtu.be/SCS7sUUyp5w?si=s4UxEEDrrBAv-SDQ) of installation and archiving.

## Why HuskHoard?

Enterprise storage vendors charge thousands of dollars for automated storage tiering and lock your data inside proprietary black boxes. HuskHoard does it for free, right in user-space, using standard open-source formats. Managing your data in an open source data tiering archive.

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
*   **SMR-Optimized Architecture:** By treating hard drives like sequential LTO tape, HuskHoard completely avoids the "SMR performance cliff." Safely use Shingled Magnetic Recording drives without complex block-level hardware management. Host Managed or Drive Managed SMR, we play both kinds. 

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

Choose between the **Automated Quick Start** (recommended, installs in seconds) or the **Manual Build from Source** below.

---

## ⚡ Option A: 1-Command Quick Start (Recommended)
*Installs pre-flight dependencies, configures kernel capabilities, builds a test environment, and formats virtual tapes automatically in under 15 seconds.*

Run this single command as your standard user:

```bash
curl -fsSL https://raw.githubusercontent.com/HuskHoard/HuskHoard/main/install_huskhoard.sh | bash
```

Once the script finishes, jump straight into your new environment:
```bash
cd huskhoard
./target/release/huskhoard daemon
```
After you are running, scroll down to the testing section and drop in some files and watch things at work.

---

## 🛠️ Option B: Manual Build from Source (Ubuntu 24.04)
*If you prefer to compile from source with Rust, use this step-by-step guide.*

> ⚠️ **Important:** Run all commands as your standard user. HuskHoard is designed to run in user-space.

### 1. Prerequisites
Install the system dependencies and the Rust toolchain:
```bash
sudo apt update
sudo apt install -y curl build-essential rclone libcap2-bin attr pkg-config libsqlite3-dev git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### 2. Download and Build
Clone the repository and compile the release binary:
```bash
git clone https://github.com/HuskHoard/HuskHoard.git huskhoard
cd huskhoard

# Build the project (takes ~5–10 mins)
cargo build --release
```

### 3. Grant Kernel Capabilities
HuskHoard needs specific capabilities to intercept file reads via `fanotify` without needing to run as a dangerous root process:
```bash
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/huskhoard
```

### 4. Configure Your Test Environment
The test environment simulates physical tape storage using rapid-access loopback images right on your disk.

```bash
# Ensure you are still in the 'huskhoard' project directory
mkdir -p hot_tier
fallocate -l 100M my_archive.img
fallocate -l 100M replication_archive.img

# Format the volumes (generates default husk_config.toml)
./target/release/huskhoard format --tape-dev my_archive.img
./target/release/huskhoard format --tape-dev replication_archive.img
```
#### Optional: Emulate Raw DM-SMR and HM-SMR Block Devices (Linux)

HuskHoard writes directly to raw block devices without a filesystem. You can test both SMR paths locally without physical hardware.

> ⚠️ **Device Permissions (Required):** Linux creates raw block devices (`/dev/loopX`, `/dev/sdX`, `/dev/nullb0`) owned by `root:disk` (`0660`). Because `huskhoard daemon` runs as your standard user (and `cap_dac_read_search` only grants read bypass, not write access), **it will fail with `Permission denied` unless you grant your user ownership of the test device** with `chown` below.
>
> *(Using `chown` on the temporary test device is much safer than adding your user to the `disk` group, which exposes all physical drives on your system. Ownership resets automatically when the virtual device is removed.)*

---

##### DM-SMR (Drive-Managed SMR)
DM-SMR drives expose a standard block device interface (`zoned = none`). You can emulate one using a loopback device:

```bash
# 1. Create a 128 MB image and attach it to a loop device
fallocate -l 128M dm_smr.img
LOOP_DEV=$(sudo losetup -f --show dm_smr.img)
echo "DM-SMR loopback device: $LOOP_DEV"

# 2. Grant your user read/write access (required before running huskhoard)
sudo chown "$USER" "$LOOP_DEV"

# 3. Format as your standard user (do NOT use sudo, or config/catalog files will be root-owned)
./target/release/huskhoard format --tape-dev "$LOOP_DEV"
```

Update `husk_config.toml` with the device printed above:
```toml
primary_volumes = ["/dev/loopX"]   # Replace with the $LOOP_DEV path printed above
```

Cleanup when done:
```bash
sudo losetup -d "$LOOP_DEV"
```

---

##### HM-SMR (Host-Managed SMR)
Linux kernels 5.12+ can emulate a real SCSI ZBC host-managed drive in RAM using `scsi_debug` (requires `util-linux` for `blkzone`):

```bash
# 1. Create a 128 MB HM-SMR drive in RAM (8 MB zones, 1 conventional zone, 4K sectors)
sudo modprobe scsi_debug dev_size_mb=128 zbc=host-managed zone_size_mb=8 zone_nr_conv=1 sector_size=4096

# 2. Locate the virtual /dev/sdX device (ALWAYS verify it says "scsi_debug" and "host-managed"!)
grep -H "" /sys/block/sd*/device/model /sys/block/sd*/queue/zoned
```

Once you have verified which `sdX` belongs to `scsi_debug`, copy/paste the block below and **replace `sdX` in the first line**:

```bash
# 3. Set your device path (replace sdX with the scsi_debug device found above)
HM_DEV="/dev/sdX"

# 4. Grant your user read/write access (required before running huskhoard)
sudo chown "$USER" "$HM_DEV"

# 5. Inspect zones and format as your standard user
blkzone report "$HM_DEV"
./target/release/huskhoard format --tape-dev "$HM_DEV"
```

Update `husk_config.toml`:
```toml
primary_volumes = ["/dev/sdX"]   # Replace sdX with your scsi_debug device
```

Cleanup when done (all in-RAM test data is discarded):
```bash
sudo modprobe -r scsi_debug
```

> **Alternative (`null_blk`):** If your kernel's `scsi_debug` was built without ZBC support (or you are on a cloud VM without `linux-modules-extra-$(uname -r)`), you can emulate an HM-SMR device with `null_blk` instead (allocates up to 1 GiB of RAM as written):
> ```bash
> sudo modprobe null_blk nr_devices=1 zoned=1 memory_backed=1 gb=1 zone_size=8 zone_nr_conv=1 bs=4096
> sudo chown "$USER" /dev/nullb0
> blkzone report /dev/nullb0
> ./target/release/huskhoard format --tape-dev /dev/nullb0
> ```
> Update `husk_config.toml` to `primary_volumes = ["/dev/nullb0"]`. Cleanup when done:
> ```bash
> sudo modprobe -r null_blk
> ```

> 💡 **Already ran `format` with `sudo`?** Check your project directory with `ls -la`. If `husk_config.toml` or your catalog database is owned by `root`, fix ownership before starting the daemon:
> ```bash
> sudo chown "$USER:$USER" husk_config.toml
> ```
**Open the generated `husk_config.toml` in your editor and update the paths to absolute locations for rapid test mode (keep or set `db_path`):**
```toml
primary_volumes = ["/home/YOUR_USERNAME/huskhoard/my_archive.img"]
replication_volumes = ["/home/YOUR_USERNAME/huskhoard/replication_archive.img"]
hot_tier = "/home/YOUR_USERNAME/huskhoard/hot_tier"
db_path = "/home/YOUR_USERNAME/huskhoard/husk_catalog.db"
max_age_days = 0                # TEST MODE: Archive files immediately
janitor_interval_secs = 60
http_port = 8080                # Port for the Streaming Gateway
http_bind = "127.0.0.1"          # Loopback only; use "0.0.0.0" to serve the LAN/Tailscale
http_token = ""                  # Optional bearer token required by the gateway
intercept_open_reads = false     # Also intercept open(2): needed if apps mmap stubbed files
hot_tier_max_usage_percent = 80 # Spillover threshold
min_free_space_gb = 0
```

### 5. Launch the Daemon
Start the background engine:
```bash
./target/release/huskhoard daemon
```

## 🧪 Testing the Engine (Both Options)

Confirm the husk_config.toml is set as above for testing, Leave the daemon running and open a **second terminal window**:

1. **Generate a 12MB dummy file in your hot tier:**
   ```bash
   dd if=/dev/urandom of=hot_tier/dummy_data.bin bs=1M count=12
   ```

2. **Wait ~30 seconds** for the Janitor engine to archive the file to tape.

* Run `ls -ls hot_tier`. You will see the file's allocated size drop to 4K bytes, which is the sparse file data, while its logical size remains intact. 
* Run `du -h hot_tier`. It has become a Husk. 
* Open the file, and watch the Daemon instantly recall it from `my_archive.img`.



### 🕹️ Command Center

**Stream a file directly from Tape (Zero-Disk):**
```bash
./target/release/huskhoard cat --file-path /media/movies/scifi.mp4 | mpv -
```

**Stream over the LAN / Tailscale (with access control):**
Set `http_bind = "0.0.0.0"` and `http_token = "a-long-random-string"` in `husk_config.toml`, then:
```bash
# Bearer header (scripts, *arr apps)
curl -H "Authorization: Bearer $TOKEN" http://archive-host:8080/stream/movies/scifi.mp4 -o scifi.mp4
# Query token (players that cannot set headers, e.g. VLC/Plex "network stream")
# http://archive-host:8080/stream/movies/scifi.mp4?token=$TOKEN
```
The gateway serves raw archived payloads, so it binds to `127.0.0.1` by default; exposing it
without a token logs a security warning at startup.

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
