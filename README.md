# 🌾 Husk (Hybrid User-Space Storage Kernel)

![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)
![Built with Rust](https://img.shields.io/badge/Built_with-Rust-orange.svg)

**Husk** is an automated, transparent data-tiering engine for Linux. It turns your expensive NVMe drives into a bottomless file system by silently archiving cold data to cheap hard drives, raw disk images, or cloud buckets—while keeping the files fully visible and accessible to your OS.

It acts like an Enterprise Tape Library, but built for the modern homelab and data hoarder.

## 🔥 Why Husk?

Enterprise storage vendors charge thousands of dollars for automated storage tiering and lock your data inside proprietary black boxes. Husk does it for free, right in user-space, using standard open-source formats.

*   **The "Scrap Metal" Philosophy:** Bring your own hardware. Husk doesn't care if your "Tape Library" is a $10,000 SAN, a dusty USB drive, a raw `.img` file, or an Amazon S3 bucket. If you can mount it or pipe to it, Husk can use it.
*   **Zero-Overhead Transparent Stubbing:** Husk does **not** use FUSE. It uses the Linux `fanotify` kernel API. When a file gets cold, Husk punches a hole in it. The file still appears in `ls` and takes up 0 bytes of SSD space. 
*   **Instant Recalls:** If you try to open an archived file, Husk instantly intercepts the read, pulls the data back from "tape," and hands it to the application so fast the app doesn't even know it was missing.
*   **The "Easy Exit" Promise (No Vendor Lock-in):** We don't hold your data hostage. The index is a standard **SQLite** database. The payloads are standard **Zstd** streams verified by **BLAKE3**. If Husk ceased to exist tomorrow, you could extract all your data using a 50-line Python script.

## 🛠️ Features

*   **N-Way Replication:** Automatically mirror cold data across local drives and cloud buckets simultaneously via `rclone`.
*   **Point-in-Time Recovery (PITR):** Husk keeps historical versions of modified files. Roll back any file to yesterday's version instantly.
*   **Bit-Rot Scrubber:** Cryptographically verify the integrity of your offline storage with a single command.
*   **Garbage Collection (Repacker):** Reclaim space from deleted files or old versions by dynamically repacking tapes.

---

## 🚀 Quick Start (Ubuntu 24.04 / Debian)

### 1. Prerequisites
Install the required system tools and the Rust compiler:

```bash
sudo apt update
sudo apt install -y build-essential rclone libcap2-bin attr pkg-config libsqlite3-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
# HuskHoard: The Infinite Hybrid SSD
### Enterprise-Grade HSM for Data Hoarders & Media Professionals.

![language-Rust](https://img.shields.io/badge/language-Rust-orange.svg)
![license-AGPLv3](https://img.shields.io/badge/license-AGPLv3-red.svg)
![platform-Linux](https://img.shields.io/badge/platform-Linux%20(Fanotify)-red.svg)

**HuskHoard** is a Hierarchical Storage Management (HSM) engine that turns your affordable external storage and cloud buckets into a high-performance, infinite archive. It "freezes" cold files into a sequential vault while leaving behind a **Husk**—a zero-byte stub that rehydrates instantly when accessed.

---

## 🏗️ Hardware-Aware Architecture

### 🧊 SMR-Native by Design (Shingled Magnetic Recording)
Modern high-capacity drives (8TB+) often use SMR, which suffers from a "write wall" during random writes. HuskHoard embraces this by using a **Strict Log-Structured Format**. By writing in one continuous, sequential stream, HuskHoard eliminates shingle-overlap overhead, allowing budget-friendly USB drives to perform like enterprise-grade hardware.

### 🌿 Sustainability & Drive Longevity
*   **Reduced Duty Cycle:** Batching archival tasks allows your archive drives to stay spun down and idle 99% of the time.
*   **Eco-Acoustic Storage:** Minimizing active seeks reduces mechanical heat, noise, and vibration fatigue.
*   **Energy Efficient:** Large media collections don't need dozens of drives spinning 24/7. HuskHoard lets them sleep until you hit "Play."

---

## ☁️ Hybrid Cloud Replication
HuskHoard treats the cloud as a massive, sequential tape drive. Using **rclone** as its transport layer, HuskHoard can stream your archives to over 40 providers (S3, Backblaze B2, Google Drive, Dropbox, etc.) in a single pass.

*   **Multiplexed Writes:** HuskHoard writes to your local drive and your cloud bucket simultaneously.
*   **Cost Optimized:** By packing data into optimal 16MB Zstd-compressed frames, it minimizes API "PUT" requests and cloud metadata overhead.

---

## 🛡️ The Vault: Ransomware & Bit-Rot Defense
*   **The "Hole" Defense:** When an attacker hits a Husked file, they are merely encrypting a "hole." The actual data remains safely stored in the append-only Vault.
*   **Point-in-Time Rollback:** HuskHoard maintains historic file versions. If a file is deleted or corrupted, the `restore` command allows you to roll back to any previous version.
*   **🩺 Bit-Rot Protection:** Every block is **BLAKE3-hashed**. The built-in Scrubber periodically verifies the entire archive, detecting "bit-flips" before they become permanent.

---

## 🚀 Quick Start (Ubuntu 24.04)

### 1. Prerequisites
Install the required system tools and the Rust compiler:
```bash
sudo apt update
sudo apt install -y build-essential rclone libcap2-bin attr pkg-config libsqlite3-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### 2. Build and Grant Capabilities
HuskHoard uses the Linux `fanotify` kernel API to intercept file reads. You must grant the binary specific capabilities to run as a standard user:
```bash
cargo build --release
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/husk
```

### 3. Configure for "Test Mode"
Create a folder for your "Hot" files and a dummy file to act as your "Tape":
```bash
mkdir hot_tier
fallocate -l 100M my_vault.img
./target/release/husk format --tape-dev my_vault.img
```

Edit `husk_config.toml` with Instant Archiving enabled:
```toml
primary_volumes = ["my_vault.img"]
max_age_days = 0 # TEST MODE: Archive immediately
janitor_interval_secs = 10
```

### 4. Launch the Daemon
```bash
./target/release/husk daemon
```
Drop any file into `hot_tier`. Watch it turn into a Husk while the data is moved to `my_vault.img`.

---

## 🕹️ Command Center

**Check "Tank Gauge" (Usage & Reclaimable Space)**
```bash
./target/release/husk info --tape-dev my_vault.img
```

**Scrub for Bit-Rot**
```bash
./target/release/husk scrub --tape-dev my_vault.img
```

**Manual PITR Restore (Rollback to Version 1)**
```bash
./target/release/husk restore --file-path $(pwd)/hot_tier/report.pdf --version 1 --dest-path ./recovered.pdf
```

---

## ⚖️ License & Commercial Use
HuskHoard is licensed under the **GNU Affero General Public License v3 (AGPL-3.0)**.

*   **Individuals & Hobbyists:** Use it for free, forever. It is our gift to the r/DataHoarder community.
*   **Enterprise:** We offer Commercial Licensing and a Proprietary Sidecar for organizations requiring:
    *   **MAID Power Control:** Advanced HBA/Backplane power management (Pin-3 PWDIS).
    *   **Global Grid Support:** Multi-node replication between worldwide offices.
    *   **Cloud Immutable Locking:** WORM (Write Once Read Many) compliance for S3/Backblaze/Azure.

For commercial inquiries, please contact `[Your Contact Info]`.
