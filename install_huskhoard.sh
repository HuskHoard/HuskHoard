#!/usr/bin/env bash
# HuskHoard Universal Installer - Enterprise Edition
# Optimized for: SLES 16, Ubuntu, RHEL, Arch, and Minimal Cloud Images

set -e 

echo "=================================================="
echo "      HuskHoard Universal Installer & Setup       "
echo "=================================================="

# 1. Pre-Flight Checks
if [ "$EUID" -eq 0 ]; then
  echo " ERROR: Please run as a standard user with sudo privileges, NOT root."
  exit 1
fi

# 2. Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
    OS_LIKE=$ID_LIKE
else
    echo " ERROR: Cannot detect OS."
    exit 1
fi

echo " Preparing dependencies for $PRETTY_NAME..."

# 3. Super-Dependency Phase (Addressing the SUSE Minimal/EC2 gaps)
# This installs the basics required for the script itself to function correctly.
case "$OS" in
    "sles"|"opensuse"|"opensuse-leap"|"opensuse-tumbleweed")
        echo " Installing SUSE Essentials..."
        sudo zypper refresh
        # Added: unzip, tar, wget, which, libcap-progs, xz, hostname
        sudo zypper install -y -l \
            gcc gcc-c++ make rclone libcap-progs attr pkg-config sqlite3-devel \
            git curl xfsprogs unzip tar gzip wget which xz hostname
        ;;
    "ubuntu"|"debian"|*"debian"*|*"ubuntu"*)
        echo " Installing Debian/Ubuntu Essentials..."
        sudo apt-get update
        sudo apt-get install -y \
            build-essential rclone libcap2-bin attr pkg-config libsqlite3-dev \
            git curl xfsprogs unzip tar gzip wget which xz-utils
        ;;
    "fedora"|"rhel"|"centos"|"almalinux"|"rocky"|*"rhel"*|*"fedora"*)
        echo " Installing RHEL/Fedora Essentials..."
        sudo dnf install -y \
            gcc gcc-c++ make rclone libcap attr pkgconf-pkg-config sqlite-devel \
            git curl xfsprogs unzip tar gzip wget which xz hostname
        ;;
    "arch"|*"arch"*)
        echo " Installing Arch Essentials..."
        sudo pacman -Sy --needed --noconfirm \
            base-devel rclone libcap attr pkgconf sqlite git curl xfsprogs unzip tar
        ;;
    *)
        echo " Unknown OS. Attempting to proceed with generic tools..."
        ;;
esac

# 4. Rclone Check
# If zypper/apt didn't have rclone, we use the web installer, 
# but now we KNOW unzip is present.
if ! command -v rclone &> /dev/null; then
    echo "Rclone not in repo. Installing via rclone.org..."
    curl https://rclone.org/install.sh | sudo bash
fi

# 5. Install Rust
echo "Checking for Rust..."
if ! command -v cargo &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
else
    echo " Rust is already installed."
fi
source "$HOME/.cargo/env" || true

# 6. Clone and Build
echo " Cloning HuskHoard..."
[ -d "huskhoard" ] || git clone https://github.com/huskhoard/huskhoard.git
cd huskhoard
cargo build --release

# 7. Grant Kernel Capabilities
# On SLES, /usr/sbin might not be in the user's PATH even with sudo.
# We find the setcap binary specifically.
SETCAP_PATH=$(command -v setcap || echo "/usr/sbin/setcap")
echo " Granting capabilities using $SETCAP_PATH..."
sudo "$SETCAP_PATH" cap_sys_admin,cap_dac_read_search+ep target/release/huskhoard

# 8. Filesystem Compliance Check
echo " Checking filesystem compliance..."
CURRENT_FS=$(df -T . | tail -n 1 | awk '{print $2}')
mkdir -p hot_tier

if [[ "$CURRENT_FS" =~ ^(xfs|ext4|btrfs|zfs)$ ]]; then
    echo " Filesystem Check Passed: $CURRENT_FS"
else
    echo " Creating Virtual XFS drive for hot_tier..."
    dd if=/dev/zero of=hot_tier_xfs.img bs=1M count=500 status=none
    sudo mkfs.xfs -f hot_tier_xfs.img > /dev/null 2>&1
    sudo mount -t xfs -o loop hot_tier_xfs.img hot_tier
    sudo chown -R "$USER":"$(id -gn)" hot_tier
fi

# 9. Virtual Tapes & Config
fallocate -l 100M my_archive.img || dd if=/dev/zero of=my_archive.img bs=1M count=100
fallocate -l 100M replication_archive.img || dd if=/dev/zero of=replication_archive.img bs=1M count=100

./target/release/huskhoard format --tape-dev my_archive.img
./target/release/huskhoard format --tape-dev replication_archive.img

ABS_PATH=$(pwd)
CONFIG_FILE="husk_config.toml"
sed -i "s|^primary_volumes = .*|primary_volumes = [\"$ABS_PATH/my_archive.img\"]|" "$CONFIG_FILE"
sed -i "s|^replication_volumes = .*|replication_volumes = [\"$ABS_PATH/replication_archive.img\"]|" "$CONFIG_FILE"
sed -i "s|^hot_tier = .*|hot_tier = \"$ABS_PATH/hot_tier\"|" "$CONFIG_FILE"
sed -i "s|^max_age_days = .*|max_age_days = 0|" "$CONFIG_FILE"
sed -i "s|^janitor_interval_secs = .*|janitor_interval_secs = 10|" "$CONFIG_FILE"

echo "=================================================="
echo "  HuskHoard Setup Complete on $OS!"
echo "=================================================="
echo "Run the daemon:"
echo "  ./target/release/huskhoard daemon"
