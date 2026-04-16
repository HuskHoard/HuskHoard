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
sudo apt install -y build-essential rclone libcap2-bin attr
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
