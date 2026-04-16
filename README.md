HuskHoard: The Infinite Hybrid SSD
Enterprise-Grade HSM for Data Hoarders & Media Professionals.

HuskHoard is a Hierarchical Storage Management (HSM) engine that turns your affordable external storage into a high-performance, infinite archive. It "freezes" cold files into a sequential vault while leaving behind a Husk—a zero-byte stub that rehydrates instantly when accessed.

🏗️ Hardware-Aware Architecture: The "Tape" Philosophy
Most filesystems are designed for random-access SSDs. They kill high-capacity hard drives with "write amplification" and fragmented seeks. HuskHoard is built differently.

🧊 SMR-Native by Design (Shingled Magnetic Recording)
Modern high-capacity drives (8TB+) often use SMR, which suffers from a "write wall" during random writes. HuskHoard embraces this by using a Strict Log-Structured Format. By writing in one continuous, sequential stream, HuskHoard eliminates shingle-overlap overhead, allowing budget-friendly USB drives to perform like enterprise-grade hardware.

☁️ Hybrid Cloud Replication
HuskHoard treats the cloud as a massive, sequential tape drive. Using rclone as its transport layer, HuskHoard can stream your archives to over 40 providers (S3, Backblaze B2, Google Drive, Dropbox, etc.) in a single pass.
Multiplexed Writes: HuskHoard writes to your local drive and your cloud bucket simultaneously.
Cost Optimized: By packing data into optimal 16MB Zstd-compressed frames, it minimizes API "PUT" requests and cloud metadata overhead.

🌿 Sustainability & Drive Longevity
HuskHoard is designed to extend the life of your mechanical hardware:
Reduced Duty Cycle: Batching archival tasks allows your archive drives to stay spun down and idle 99% of the time.
Eco-Acoustic Storage: By minimizing active seeks, HuskHoard reduces mechanical heat, noise, and vibration fatigue.
Energy Efficient: Large media collections don't need dozens of drives spinning 24/7. HuskHoard lets them sleep until you hit "Play."

🛡️ The Hoard: Ransomware & Bit-Rot Defense
Ransomware-Immune Backends
In a typical attack, malware attempts to encrypt every file it finds.
The "Hole" Defense: When an attacker hits a Husked file, they are merely encrypting a "hole." The actual data remains safely stored in the append-only Vault.
Point-in-Time Rollback (The Wasteland): HuskHoard maintains historic file versions. If a file is deleted or corrupted, the restore command allows you to roll back to any previous version indexed in the catalog.

🩺 Bit-Rot Protection
HuskHoard uses BLAKE3 hashing for every block. The built-in Scrubber periodically verifies the entire archive, detecting "bit-flips" or silent data corruption before they become permanent.

🌪️ How the "Husk" Works
Freeze: The Janitor compresses, hashes, and streams cold data to your Local Tape and Cloud Bucket simultaneously.
Hole Punching: Using fallocate, we tell the SSD to delete the data but keep the file entry.
The Result: ls -l shows a 50GB video. du -h shows 0 bytes used on your SSD.
Instant Rehydration: You access the file. The Interceptor catches the request and streams the data back from the vault. The video starts playing before the OS even knows the data was gone.

🚀 Quick Start (Ubuntu 24.04)#Requirement because of kernel feature set
1. Prerequisites
Install the required system tools and the Rust compiler:
code
Bash
sudo apt update
sudo apt install -y build-essential rclone libcap2-bin attr
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
2. Build and Grant Capabilities
HuskHoard uses the Linux fanotify kernel API to intercept file reads. You must grant the binary specific capabilities to run as a standard user:
code
Bash
cargo build --release
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/huskhoard
3. Configure for "Test Mode"
Create a folder for your "Hot" files and a dummy file to act as your "Tape":
code
Bash
mkdir hot_tier
fallocate -l 100M my_vault.img
./target/release/huskhoard format --tape-dev my_vault.img
Create huskhoard_config.toml with Instant Archiving enabled:
code
Toml
hot_tier = "hot_tier"
db_path = "huskhoard_catalog.db"
primary_volumes = ["my_vault.img"]
replication_volumes = []
max_age_days = 0 # TEST MODE: Archive immediately
janitor_interval_secs = 10 
log_level = "info"
4. Launch the Daemon
code
Bash
./target/release/huskhoard daemon
Drop any file into hot_tier. Watch it turn into a Husk while the data is moved to my_vault.img.
🕹️ Command Center
Check "Tank Gauge" (Usage & Reclaimable Space)
code
Bash
./huskhoard info --tape-dev /dev/sdb
Scrub for Bit-Rot
code
Bash
./huskhoard scrub --tape-dev /dev/sdb
Manual PITR Restore (Rollback to Version 1)
code
Bash
./huskhoard restore --file-path /data/docs/report.pdf --version 1 --dest_path ./recovered.pdf
⚖️ License & Commercial Use
HuskHoard is licensed under the GNU Affero General Public License v3 (AGPL-3.0).
Individuals & Hobbyists: Free forever. It is our gift to the r/DataHoarder community.
Enterprise: Commercial Licensing and a Proprietary Sidecar are available for:
MAID Power Control: Advanced HBA/Backplane power management (Pin-3 PWDIS).
Global Grid Support: Multi-node replication between worldwide offices.
Cloud Immutable Locking: WORM (Write Once Read Many) compliance for S3/B2.
For commercial inquiries, please contact [Your Contact Info].
