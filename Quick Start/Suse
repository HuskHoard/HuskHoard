
### 🚀 Quick Start (SUSE Linux Enterprise / SLES 15)
⚠️ **Note for Minimal Builds:** Ensure your system is registered via `SUSEConnect` to access the official repositories.

#### 1. Prerequisites
On a minimal SLES install, we need to install the development pattern and specific libraries:

```bash
# Update the package database
sudo zypper refresh
sudo zypper update -y

# Install the Development Tools pattern (compilers, make, etc.)
sudo zypper install -t pattern devel_basis -y

# Install HuskHoard dependencies
sudo zypper install -y libcap-progs rclone libattr-devel attr sqlite3-devel pkg-config git mt-st mtx

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

#### 2. Download and Build
```bash
git clone https://github.com/huskhoard/huskhoard.git
cd huskhoard

# Build the project
cargo build --release
```

#### 3. Grant Kernel Capabilities
SLES is security-hardened. Granting these capabilities allows HuskHoard to monitor the file system without being root:

```bash
sudo setcap cap_sys_admin,cap_dac_read_search+ep target/release/huskhoard
```

#### 4. Configure Your Environment
SLES handles sparse files and hole-punching (`fallocate`) very well on its default XFS or Btrfs partitions.

```bash
mkdir -p hot_tier
fallocate -l 100M my_archive.img

# Format the volume (generates husk_config.toml)
./target/release/huskhoard format --tape-dev my_archive.img
```

**Update `husk_config.toml`:**
```toml
primary_volumes = ["/home/YOUR_USERNAME/huskhoard/my_archive.img"]
hot_tier = "/home/YOUR_USERNAME/huskhoard/hot_tier"
max_age_days = 0 
janitor_interval_secs = 60
```

#### 5. Launch the Daemon
```bash
./target/release/huskhoard daemon
```

---

### Important notes for SLES users and how this differs from the Ubuntu install:

1.  **Package Manager:** Uses `zypper`.
2.  **Patterns:** SLES uses "Patterns" instead of "Groups." The `devel_basis` pattern is the equivalent of `build-essential`.
3.  **Library Names:** SUSE naming conventions can differ slightly. For example, `libcap-progs` provides the `setcap` tool, and headers usually end in `-devel`.
4.  **Rclone on SLES:** If `rclone` is not found in your standard SLES repositories (it depends on which modules you have enabled), you can install it using the official script: 
    `sudo curl https://rclone.org/install.sh | sudo bash`
5.  **Firewall:** SLES (and openSUSE) often has `firewalld` enabled by default. If you plan to use the **HTTP Streaming Gateway**, you will need to open the port:
    `sudo firewall-cmd --add-port=8080/tcp --permanent`
    `sudo firewall-cmd --reload`
6.  **AppArmor:** SLES uses AppArmor. If the daemon is blocked from reading certain directories, you may need to check the logs (`dmesg | grep -i apparmor`), though generally, user-space tools in home directories are unconfined.
