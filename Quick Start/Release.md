
## Quick Start (Pre-compiled Binary)

If you prefer not to build HuskHoard from source, you can use our pre-compiled release binaries. 

**1. Download the Latest Release**
Fetch the latest binary from the GitHub Releases page. :

```bash
# Download the latest release archive (Check the Releases page for the exact filename)
wget https://github.com/HuskHoard/HuskHoard/releases/download/v0.1.0/huskhoard

```

**2. Make the Binary Executable**
```bash
chmod +x huskhoard
```

**3. Grant Kernel Capabilities**
Because HuskHoard uses the Linux `fanotify` API to transparently intercept file reads without needing to run entirely as a dangerous root process, you must grant the executable specific capabilities:

```bash
sudo setcap cap_sys_admin,cap_sys_ptrace,cap_dac_read_search+ep ./huskhoard
```
*(Note: this is for a ubuntu environment. If your system uses different capabilities, refer to the other quick start guides).*


#### 4. Configure Your "Test Environment"
Set up a safe testing area right inside the project folder. We will create a `hot_tier` directory (on your SSD) and a 100MB file to act as your physical "Volume".

```bash
# Ensure you are still in the 'huskhoard' project directory
mkdir -p hot_tier
fallocate -l 100M my_archive.img
fallocate -l 100M replication_archive.img
```

Next, format the volume. Running this command for the first time will automatically generate a `husk_config.toml` file in your current directory.

```bash
./huskhoard format --tape-dev my_archive.img
./huskhoard format --tape-dev replication_archive.img
```
```bash
# OR: Format a physical LTO tape drive
./huskhoard format --tape-dev /dev/nst0
```

Open the newly generated `husk_config.toml` in your text editor. Update these lines to enable **Instant Archiving** so you can see it work immediately. *(Note: Using absolute paths is highly recommended so the daemon always knows where your data is).*

```toml
primary_volumes = ["/home/YOUR_USERNAME/huskhoard/my_archive.img"]
replication_volumes = ["/home/YOUR_USERNAME/huskhoard/replication_archive.img"]
hot_tier = "/home/YOUR_USERNAME/huskhoard/hot_tier"  # Ensure this points to your hot tier
max_age_days = 0 # TEST MODE: Archive files immediately
janitor_interval_secs = 60
http_port = 8080 # Port for the Streaming Gateway
# --- Safety Settings ---
# Trigger emergency archiving if the Hot Tier exceeds 80% capacity
hot_tier_max_usage_percent = 80 
# The Janitor will try to keep at least this much space (in GB) strictly free, set to 0 for test
min_free_space_gb = 0
```

#### 5. Launch the Daemon
Start the HuskHoard background engine:

```bash
./huskhoard daemon
```

#### 6. Test it
Leave the daemon running and open a **second terminal window**. 

Drop a large file into `hot_tier`.

#### Generate a 12MB dummy file filled with random data
```bash
dd if=/dev/urandom of=hot_tier/dummy_data.bin bs=1M count=12
```
Wait 10 seconds. 
* Run `ls -ls hot_tier`. You will see the file's allocated size drop to 4K bytes, which is the sparse file data, while its logical size remains intact. 
* Run `du -h hot_tier`. It has become a Husk. 
* Open the file, and watch the Daemon instantly recall it from `my_archive.img`.

---

### 🕹️ Command Center

**Stream a file directly from Tape (Zero-Disk):**
```bash
./huskhoard cat --file-path /media/movies/scifi.mp4 | mpv -
```

**Export Catalog for Data Engineering (Parquet):**
```bash
./huskhoard export --format parquet --output my_catalog.parquet
# Query it instantly with DuckDB:
# duckdb -c "SELECT file_path, payload_size FROM 'my_catalog.parquet' WHERE payload_size > 1e9"
```

**Check Capacity & "Wasteland" statistics:**
```bash
./huskhoard info --tape-dev /dev/nst0
```

**Scrub a Volume for Bit-Rot:**
```bash
./huskhoard scrub --tape-dev my_archive.img
```

**Repack (Garbage Collect) an old Volume:**
```bash
./huskhoard repack --source-tape old_drive.img --dest-tape new_drive.img
```

---

### 🚀 Roadmap
*   [Complete] **StreamGate HTTP:** A local web-gateway allowing video players to seek through tapes via HTTP Range requests.
*   [Complete] **Parquet Export:** Native support for exporting metadata to big-data formats.
*   [Planned] **Web Dashboard:** Real-time visual "Tank Gauge" monitoring.

### License
Husk is licensed under the AGPL v3. Infrastructure software should remain free, open, and permanently protected from proprietary exploitation.

For commercial inquiries, contact `info@huskhoard.com`.

You are now ready to configure your storage tiers and start the archiving engine. 
```
