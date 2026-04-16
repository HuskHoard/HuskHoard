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
