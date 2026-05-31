

# 🚀 Quick Start (TrueNAS SCALE / Debian Jail)

⚠️ **Note:** This guide assumes you have [Jailmaker](https://github.com/Jip-Hop/jailmaker) installed on your TrueNAS SCALE box. By running Husk in a jail, you keep the software persistent across OS updates while getting full, unrestricted access to the ZFS datasets and physical tape hardware.

### 1. Create the Jailmaker Config

Before starting the jail, you need to edit the config file to allow kernel hooks and hardware access. In your TrueNAS shell, create a jail called `husk` and paste this into the configuration file.

```ini
# Husk Jailmaker Template
distro=debian
release=bookworm

# Use host networking so the HTTP Gateway (8080) is reachable on your LAN
network=host

# Husk needs high-level permissions for fanotify and hardware access
systemd_nspawn_user_args=(
    # Allow the jail to talk to the host kernel for filesystem events
    --capability=all
    --system-call-filter=@system-service
    
    # 1. Bind mount your ZFS dataset (The Hot Tier)
    # Change /mnt/tank/media to your actual TrueNAS dataset path
    --bind=/mnt/tank/media:/mnt/hot_tier
    
    # 2. Pass through your physical tape or USB hardware
    # For physical tape: pass through the raw SCSI device
    --property="DeviceAllow=/dev/nst0 rwm"
    --bind=/dev/nst0
    
    # 3. Keep the Husk database and binary on your ZFS pool
    # This ensures your catalog and application survive if you rebuild the jail
    --bind=/mnt/tank/apps/huskhoard:/opt/huskhoard
)
```

### 2. Prerequisites
Start your jail, open a shell inside it, and run these commands to prepare the Debian environment.

```bash
# Update the package database
sudo apt update
sudo apt upgrade -y

# Install build tools and dependencies
sudo apt install -y build-essential libcap2-bin rclone libattr1-dev attr libsqlite3-dev pkg-config git mt-st mtx

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### 3. Download and Build
Because we mapped `/opt/huskhoard` to a persistent ZFS dataset in step 1, we will build the app there.

```bash
cd /opt
git clone https://github.com/huskhoard/huskhoard.git
cd huskhoard

# Build the project
cargo build --release
```

### 4. Grant Kernel Capabilities
This allows Husk to monitor the filesystem (`fanotify`) and send low-level commands to your tape hardware without needing to be run as the root user.

```bash
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/husk
```

### 5. Configure Your Environment
We will use the persistent paths established in our Jailmaker config.

```bash
cd /opt/huskhoard

# Create a dummy archive file (if testing without a physical tape drive)
fallocate -l 100M my_archive.img

# Format the volume (this also generates your husk_config.toml)
./target/release/husk format --tape-dev my_archive.img
```

**Update `husk_config.toml` to run in a test mode:**
```toml
primary_volumes = ["/opt/huskhoard/my_archive.img"]
hot_tier = "/mnt/hot_tier"
max_age_days = 0 
janitor_interval_secs = 60
```

### 6. Launch the Daemon
```bash
./target/release/husk daemon
```

---

### 💡 Why TrueNAS SCALE + HuskHoard?

1. **ZFS Infinite Expansion:** Instead of buying more disks, Husk automatically moves cold data to tape or cloud remotes. TrueNAS ZFS handles the hole-punching natively, meaning the files stay perfectly visible in your SMB/NFS shares but take up zero blocks on your storage pool.
2. **Jailmaker Safety:** By using `systemd-nspawn` via Jailmaker, you get a persistent Debian environment that survives TrueNAS OS updates while keeping the binary safely contained away from the host OS.
3. **Hardware Passthrough:** The `DeviceAllow` property in the config is the pro way to handle SCSI tape drives. It lets Husk's Rust code send raw `ioctl` commands directly to the hardware without compromising the main OS or requiring custom TrueNAS kernel tweaks.
4. **Zero-Copy StreamGate:** Husk's mathematical jump tables allow you to stream media directly from a tape drive or cloud remote back to your ZFS pool via the HTTP Gateway, making your archives act like a bottomless SSD distribution node for Plex or Jellyfin.
5. A few more notes about integrating with TrueNAS can be found here: https://www.huskhoard.com/blog-post-true.html```
