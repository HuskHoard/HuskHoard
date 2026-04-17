Section 4: Volume Management (volumes.md)
Volume Management
HuskHoard is designed to be hardware-agnostic. Whether you are using a $20 shingled USB drive or a multi-terabyte S3 bucket, HuskHoard treats them as a unified "Hoard."
1. Local Volumes: SMR vs. CMR
Unlike traditional filesystems (EXT4, ZFS), HuskHoard is SMR-Native.
Sequential Log Format: Because HuskHoard writes data in one continuous stream and never overwrites "in-place," it eliminates the catastrophic performance degradation usually seen in Shingled Magnetic Recording (SMR) drives.
O_DIRECT Compatibility: By default, HuskHoard attempts to use O_DIRECT for local volumes to bypass the Linux kernel cache, ensuring data is written directly to the platter for maximum integrity.
2. Cloud Targets (rclone)
HuskHoard uses rclone as its transport layer for cloud storage.
Setup: First, configure your remote using rclone config.
Integration: In husk_config.toml, add your remote using the prefix rclone:.
Example: replication_volumes = ["rclone:my_s3_bucket:archive_folder"]
Multiplexed Stream: When archiving, HuskHoard streams data to the cloud in 16MB compressed frames. This optimizes "PUT" requests and minimizes cloud provider costs.
3. Tiered Replication
HuskHoard organizes volumes into three priority tiers:
Primary: Your main local archive drive.
Failover: If the Primary is full or disconnected, HuskHoard automatically spills over to these volumes.
Replication: These volumes receive a copy of everything written to the Primary/Failover tiers. This is where you put your Cloud remotes or secondary "Mirror" drives.
4. Hardware Identity (UUIDs & Serials)
HuskHoard does not rely on fragile Linux device paths like /dev/sdb1.
Volume UUIDs: Every formatted volume has a unique ID stored in its 4KB header.
Hardware Serials: For physical drives, HuskHoard records the factory serial number.
Auto-Rescan: If you move a USB drive to a different port, HuskHoard will scan /sys/block at startup, find the serial number, and automatically update the Catalog with the new path.

5. The Cold Library: Shelf Management
HuskHoard was designed with the understanding that you may have more data than you have SATA ports. It treats your archive drives like a Tape Library, where volumes can be "Ejected" and stored on a shelf.
Persistent Hardware Identity
When you format a volume, HuskHoard captures two critical pieces of metadata:
Volume UUID: A unique software identifier stored in the USTD Header.
Drive Serial Number: The factory hardware ID (e.g., WDC_WD140EDGZ_ZEK12345) pulled from the disk controller.
This information is stored permanently in your husk_catalog.db. If you unplug a drive, the Catalog does not forget it; it simply marks it as OFFLINE.
Handling Offline Recalls
If a user or application attempts to read a "Husk" file that resides on a drive currently sitting on your shelf, the following happens:
The Interceptor Pauses: The application requesting the file is placed in a "wait" state by the Kernel.
The Daemon Alerts: The HuskHoard daemon detects that the required Volume UUID is not currently mounted.
The CLI Prompt: The daemon logs a high-priority alert to the terminal and system logs:
⚠️ OFFLINE RECALL: File 'family_photo_2012.raw' is stored on an offline volume.
👉 PLEASE INSERT: Volume UUID [c659bb93] (Drive Serial: ZEK12345)
Auto-Resume: As soon as you plug the drive into any USB or SATA port, the daemon rescans, identifies the Serial/UUID, and automatically resumes the rehydration. The application that was "frozen" will suddenly receive its data and continue without crashing.
Ejecting Drives Safely
Because HuskHoard uses O_DIRECT and performs sequential writes, it is very "safe" for removable media. To eject a drive:
Ensure the Archive Worker is idle (check logs).
Use standard Linux eject commands: sudo eject /dev/sdX.
HuskHoard will continue to show the files in your hot_tier, preserving your ability to browse your library even when the "tapes" are offline.
Why this is a game-changer for your project:
Energy Savings: Users don't need to keep 10-bay NAS units spinning 24/7. They can keep their "Master Archive" on a shelf and only plug it in for restores or monthly scrubs.
Infinite Scaling: The "Hoard" is only limited by the user's shelf space, not their motherboard's SATA ports.
Organization: Most tools just say "File not found." HuskHoard tells you exactly which physical piece of hardware to go find in your cabinet.
