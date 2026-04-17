Section 2: Architecture (architecture.md)
Architecture
HuskHoard is built as a multi-threaded daemon that sits between the Linux Kernel and your storage hardware.
The Lifecycle of a File
A file managed by HuskHoard moves through four distinct states:
HOT: The file lives entirely on your SSD.
COOLING: The Janitor monitors the file. If it isn't touched for max_age_days, it is queued for archiving.
ARCHIVING: The Worker streams the file, compresses it, and replicates it across your storage pool.
STUBBED (The Husk): The file's blocks are freed on the SSD. It occupies 0 bytes of physical space but remains 100% visible to the OS.
Core Components
1. The Interceptor (fanotify)
The Interceptor is the "front line." It uses the Linux fanotify kernel API to listen for open and read events. When it detects a read on a Stubbed file, it blocks the calling process, triggers a Recall, and releases the process once the data is restored.
2. The Janitor (Policy Engine)
The Janitor is a SQLite-driven scheduler. It periodically scans the Active Tracking table to find files that meet your archival criteria (age, extension, or specific directory rules).
3. The Archive Worker
The Worker handles the heavy lifting. It performs:
BLAKE3 Hashing: For bit-rot protection.
Zstd Compression: For space efficiency.
Multiplexed Writing: Simultaneously writing to local disks and Cloud remotes (via rclone) in a single pass.
4. The Catalog (SQLite)
The Catalog is the "Brain." It tracks every version of every file and its exact byte-offset on your "tapes."
Resilience Note: The Catalog is periodically backed up to the archive volumes themselves. This creates a "Database Anchor," allowing for total disaster recovery even if the host SSD is wiped.
