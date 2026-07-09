
## Quick Start (Pre-compiled Binary)

If you prefer not to build HuskHoard from source, you can use our pre-compiled release binaries. 

**1. Create a setup directory and download the latest release**
First, let's create a dedicated folder to keep things organized, move into it, and download the latest binary:

```bash
mkdir -p ~/huskhoard_setup
cd ~/huskhoard_setup

# Download the release binary
wget https://github.com/HuskHoard/HuskHoard/releases/download/v0.1.0/huskhoard
```

**2. Make Executable and Move to PATH**
Moving the binary to `/usr/local/bin` allows it to be accessed globally, which is required for running it as a secure background service.

```bash
chmod +x huskhoard
sudo mv ./huskhoard /usr/local/bin/huskhoard
```

**3. Grant Kernel Capabilities**
Because HuskHoard uses the Linux `fanotify` API to transparently intercept file reads without needing to run entirely as a dangerous root process, you must grant the executable specific capabilities:

```bash
sudo setcap cap_sys_admin,cap_sys_ptrace,cap_dac_read_search+ep /usr/local/bin/huskhoard
```
*(Note: this is for a Ubuntu environment. If your system uses different capabilities, refer to the advanced quick start guides).*

#### 4. Configure Your "Test Environment"
Set up a safe testing area right inside your setup folder. We will create a `hot_tier` directory (on your SSD) and two 100MB files to act as your physical "Volumes".

```bash
# Ensure you are still in your setup directory
cd ~/huskhoard_setup
mkdir -p hot_tier
fallocate -l 100M my_archive.img
fallocate -l 100M replication_archive.img
```

Next, format the volumes. Running this command for the first time will automatically generate a `husk_config.toml` file in your current directory.

```bash
huskhoard format --tape-dev my_archive.img
huskhoard format --tape-dev replication_archive.img
```
*(OR: Format a physical LTO tape drive with `huskhoard format --tape-dev /dev/nst0`)*

Open the newly generated `husk_config.toml` in your text editor. Update these lines to enable **Instant Archiving**. 
*(Note: Because this will run as a background service, using **absolute paths** is strictly required so the daemon always knows where your data is).*

```toml
primary_volumes = ["/home/YOUR_USERNAME/huskhoard_setup/my_archive.img"]
replication_volumes = ["/home/YOUR_USERNAME/huskhoard_setup/replication_archive.img"]
hot_tier = "/home/YOUR_USERNAME/huskhoard_setup/hot_tier"  # Ensure this points to your hot tier
db_path = "/home/YOUR_USERNAME/huskhoard_setup/husk_catalog.db"
max_age_days = 0 # TEST MODE: Archive files immediately
janitor_interval_secs = 60
http_port = 8080 # Port for the Streaming Gateway
# --- Safety Settings ---
hot_tier_max_usage_percent = 80 
min_free_space_gb = 0
```

#### 5. Setup the Background Service (systemd)
Instead of leaving a terminal window open forever, let's register HuskHoard as a background service so it automatically starts on boot and restarts if it fails.

Create a new service file using nano:
```bash
sudo nano /etc/systemd/system/huskhoard.service
```

Paste the following configuration into the file (Be sure to replace `YOUR_USERNAME` with your actual Linux username!):

```ini
[Unit]
Description=HuskHoard Archiving Engine
After=network.target

[Service]
Type=simple
User=YOUR_USERNAME
WorkingDirectory=/home/YOUR_USERNAME/huskhoard_setup
ExecStart=/usr/local/bin/huskhoard daemon
Environment="RUST_LOG=info"
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X`). Now, enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable huskhoard
sudo systemctl start huskhoard
```

*(You can watch the engine's live logs at any time by typing: `journalctl -u huskhoard -f`)*

#### 6. Test it
With the background service running, open your terminal and drop a large file into your `hot_tier`.

#### Generate a 12MB dummy file filled with random data
```bash
cd ~/huskhoard_setup
dd if=/dev/urandom of=hot_tier/dummy_data.bin bs=1M count=12
```
Wait 60 seconds (for the Janitor interval to trigger). 
* Run `ls -ls hot_tier`. You will see the file's allocated size drop to 4K bytes, which is the sparse file data, while its logical size remains intact. 
* Run `du -h hot_tier`. It has become a Husk. 
* Open the file, and watch the background daemon instantly recall it from `my_archive.img`.

---

### 🕹️ Command Center

**Stream a file directly from Tape (Zero-Disk):**
```bash
huskhoard cat --file-path /media/movies/scifi.mp4 | mpv -
```

**Export Catalog for Data Engineering (Parquet):**
```bash
huskhoard export --format parquet --output my_catalog.parquet
# Query it instantly with DuckDB:
# duckdb -c "SELECT file_path, payload_size FROM 'my_catalog.parquet' WHERE payload_size > 1e9"
```

**Check Capacity & "Wasteland" statistics:**
```bash
huskhoard info --tape-dev /dev/nst0
```

**Scrub a Volume for Bit-Rot:**
```bash
huskhoard scrub --tape-dev my_archive.img
```

**Repack (Garbage Collect) an old Volume:**
```bash
huskhoard repack --source-tape old_drive.img --dest-tape new_drive.img
```

---

### 🚀 Roadmap
*   [Complete] **StreamGate HTTP:** A local web-gateway allowing video players to seek through tapes via HTTP Range requests.
*   [Complete] **Parquet Export:** Native support for exporting metadata to big-data formats.
*   [Planned] **Web Dashboard:** Real-time visual "Tank Gauge" monitoring.

### License
Husk is licensed under the AGPL v3. Infrastructure software should remain free, open, and permanently protected from proprietary exploitation.

For commercial inquiries, contact `info@huskhoard.com`.
```
