# HuskHoard

![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)
![Built with Rust](https://img.shields.io/badge/Built_with-Rust-orange.svg)
![platform-Linux](https://img.shields.io/badge/platform-Linux%20-red.svg)

**HuskHoard** is an automated, transparent data-tiering engine for Linux. It turns your expensive NVMe drives into a bottomless file system by silently archiving cold data to cheap hard drives, raw disk images, or cloud buckets—while keeping the files fully visible and accessible to your OS.

It acts like an Enterprise Tape Library, but built for the modern homelab and data hoarder.

## Why HuskHoard?

Enterprise storage vendors charge thousands of dollars for automated storage tiering and lock your data inside proprietary black boxes. HuskHoard does it for free, right in user-space, using standard open-source formats.

*   **Bring Your Own Hardware:** HuskHoard doesn't care if your "Tape Library" is a $10,000 SAN, a dusty USB drive, a raw `.img` file, or an Amazon S3 bucket. If you can mount it or pipe to it, HuskHoard can use it.
*   **Zero-Overhead Transparent Stubbing:** HuskHoard does **not** use FUSE. It uses the Linux `fanotify` kernel API. When a file gets cold, HuskHoard punches a hole in it. The file still appears in `ls` and takes up 4 Kbytes of SSD space. 
*   **Instant Recalls:** If you try to open an archived file, HuskHoard instantly intercepts the read, pulls the data back from "tape," and hands it to the application so fast the app doesn't even know it was missing.
*   **The "Easy Exit" Promise (No Vendor Lock-in):** We don't hold your data hostage. The index is a standard **SQLite** database. The payloads are standard **Zstd** streams verified by **BLAKE3**. If HuskHoard ceased to exist tomorrow, you could extract all your data using a 50-line Python script.

##  Features

*   **N-Way Replication:** Automatically mirror cold data across local drives and cloud buckets simultaneously via `rclone`.
*   **Point-in-Time Recovery (PITR):** Husk keeps historical versions of modified files. Roll back any file to yesterday's version instantly.
*   **Bit-Rot Scrubber:** Cryptographically verify the integrity of your offline storage with a single command.
*   **Garbage Collection (Repacker):** Reclaim space from deleted files or old versions by dynamically repacking tapes.

### Architecture Overview
Husk is divided into four main components:
*   **The Catalog:** A SQLite record of every file, when it was created and the storage voulume where it resides, online or offline. 
*   **The Interceptor:** A lightweight event loop listening to fanotify. It detects when an application requests a stubbed file, blocks the application for a few milliseconds, restores the data, and lets the application continue.
*   **The Janitor:** A background SQLite-driven policy engine. It scans for files that haven't been touched in max_age_days and feeds them to the Archive Worker.
*   **The Archive Worker:** Streams the file through BLAKE3 and Zstd, multiplexes the write across your Primary, Failover, and Cloud (rclone) volumes, and punches a hole in the original file to free up your SSD.



---
## Hardware-Aware Architecture

##  Supports all drive technologies including SMR drives
Modern high-capacity SMR drives, suffer from a "write wall" during random writes. HuskHoard embraces this by using a **Strict Log-Structured Format**. By writing in one continuous, sequential stream, HuskHoard eliminates shingle-overlap overhead, allowing budget-friendly USB drives to perform like enterprise-grade hardware. Works with standard CMR drives, NVMe and USB attached SSDs too.


## Sustainability & Drive Longevity
*   **Reduced Duty Cycle:** Batching archival tasks allows your archive drives to stay spun down and idle 99% of the time.
*   **Eco-Acoustic Storage:** Minimizing active seeks reduces mechanical heat, noise, and vibration fatigue.
*   **Energy Efficient:** Large media collections don't need dozens of drives spinning 24/7. HuskHoard lets them sleep until you hit "Play."


## Hybrid Cloud Replication
HuskHoard treats the cloud as a massive, sequential tape drive. Using **rclone** as its transport layer, HuskHoard can stream your archives to over 40 providers (S3, Backblaze B2, Google Drive, Dropbox, etc.) in a single pass.

*   **Multiplexed Writes:** HuskHoard writes to your local drive and your cloud bucket simultaneously.
*   **Cost Optimized:** By packing data into optimal 16MB Zstd-compressed frames, it minimizes API "PUT" requests and cloud metadata overhead.


##  The Hoard: Ransomware & Bit-Rot Defense
*   **The "Hole" Defense:** When an attacker hits a Husked file, they are merely encrypting a "hole." The actual data remains safely stored in the append-only Hoard.
*   **Point-in-Time Rollback:** HuskHoard maintains historic file versions. If a file is deleted or corrupted, the `restore` command allows you to roll back to any previous version.
*   **Bit-Rot Protection:** Every block is **BLAKE3-hashed**. The built-in Scrubber periodically verifies the entire archive, detecting "bit-flips" before they become permanent.

Hi JM. This project looks incredibly cool. Storage tiering is usually a massive headache, and using `fanotify` for a modern, transparent user-space solution is a brilliant approach.

The reason your beta testers are installing things in the root directory (or unexpected places) is due to **Assumed Context**. As the developer, you naturally start in your project folder. But when a user opens a fresh terminal, they start in their Home directory (`~`). If they run `sudo` commands or navigate away, they can easily end up in `/` or `/root`.

Here are the specific missing links in your current guide:
1. **No Clone/Directory Step:** The guide never actually tells them to download the code and `cd` into the folder. 
2. **Relative Paths:** Commands like `./target/release/huskhoard` rely heavily on the user being in the exact right folder. If they aren't, it breaks.
3. **Ambiguous Configuration:** It's not explicitly clear *where* `husk_config.toml` gets generated or if the paths inside it should be absolute or relative.

### 🐧 OS Compatibility & Requirements
HuskHoard relies on the Linux **fanotify** kernel API. It is compatible with almost any modern Linux distribution using **Kernel 5.1 or higher**.

*   **Primary Support:** Ubuntu 22.04 LTS, 24.04 LTS (Recommended)
*   **Enterprise/Server:** Debian 11/12, Rocky Linux 8/9, AlmaLinux 8/9, RHEL 8/9
*   **Desktop/Rolling:** Arch Linux, Fedora 38+, openSUSE Tumbleweed
*   **Incompatible:** WSL2 (Windows Subsystem for Linux), CentOS 7 (Kernel too old), Synology/QNAP (unless using custom kernels).

##🚀 Quick Start** (Ubuntu 24.04)

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
git clone https://github.com/huskhoard/huskhoard.git
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
Let's set up a safe testing area right inside the project folder. We will create a `hot_tier` directory (on your SSD) and a 100MB file to act as your physical "Tape Volume".

```bash
# Ensure you are still in the 'huskhoard' project directory
mkdir -p hot_tier
fallocate -l 100M my_hoard.img
```

Next, format the tape volume. Running this command for the first time will automatically generate a `husk_config.toml` file in your current directory.

```bash
./target/release/huskhoard format --tape-dev my_hoard.img
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
Husk includes built-in tools to manage your storage volumes.
Check Capacity, Usage & Reclaimable Space
```bash
./target/release/huskhoard info --tape-dev my_hoard.img
```
Scrub a Volume for Bit-Rot
```bash
./target/release/huskhoard scrub --tape-dev my_hoard.img
```
Manual PITR Restore (Rollback to Version 1)
```bash
./target/release/huskhoard restore --file-path $(pwd)/hot_tier/[your file name] --version 1 --dest-path ./[your new file name]
```
Repack (Garbage Collect) an old Volume to a new one
```bash
./target/release/huskhoard repack --source-tape my_hoard.img --dest-tape my_new_hoard.img
```
###  Contributing & Roadmap
We are building the ultimate open-source storage tiering solution. Pull requests are welcome!

[Planned] Web UI / Dashboard for real-time Tank Gauge monitoring.

[Planned] Prometheus metrics endpoint (/metrics) for Grafana integration.

[Planned] Pre-packaged Docker container / Appliance OS.

### License
Husk is licensed under the AGPL v3. We believe infrastructure software should remain free, open, and permanently protected from proprietary cloud-vendor exploitation


For commercial inquiries, please contact `info@huskhoard.com`.
