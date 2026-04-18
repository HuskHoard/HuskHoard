# This tests all of the main features. run it with sudo. it will launch the huskhoard main and create storage volumes and catalogs.
# It then populates the volumes and creates replica versions. 
# the log will print all actions 







import os
import time
import subprocess
import logging
import shutil
import textwrap
import sqlite3

# Configuration
CONFIG_FILE = "huskhoard_test_config.toml"
HOT_TIER = "hot_tier"
DB_PATH = "huskhoard_test_catalog.db"
TAPE_PRIMARY = "/tmp/tape_primary.img"
TAPE_REPLICA = "/tmp/tape_replica.img"
TAPE_REPACK = "/tmp/tape_repack.img"
CLOUD_MOCK_DIR = "/tmp/huskhoard_cloud_remote" # Handled natively by rclone local adapter
LOG_FILE = "huskhoard_test_runner.log"

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

def run_cmd(cmd, capture=False):
    """Helper to run shell commands."""
    try:
        res = subprocess.run(cmd, capture_output=capture, text=True, check=True)
        return res.stdout if capture else True
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {' '.join(cmd)}\n{e.stderr}")
        return False

def is_stubbed(filepath):
    """Check if a file has been stubbed by looking for the updated Husk xattr."""
    try:
        res = subprocess.run(["sudo", "getfattr", "-n", "trusted.husk.status", filepath], capture_output=True, text=True)
        return "trusted.husk.status" in res.stdout
    except Exception:
        return False

def wait_for_stubbing(filepath, timeout=130):
    """Poll a file until it becomes a stub."""
    logging.info(f"⏳ Waiting for Janitor to stub '{filepath}'...")
    for i in range(timeout // 2):
        time.sleep(2)
        if is_stubbed(filepath):
            logging.info(f"❄️ SUCCESS: '{filepath}' has been stubbed!")
            return True
    logging.error(f"❌ FAILED: '{filepath}' was not stubbed in time.")
    return False

def generate_toml():
    """Generates the huskhoard_config.toml dynamically for the test environment."""
    toml = textwrap.dedent(f"""\
        # Auto-generated Test Config
        hot_tier = "{HOT_TIER}"
        db_path = "{DB_PATH}"
        log_level = "info"
        
        primary_volumes = ["{TAPE_PRIMARY}"]
        failover_volumes = []
        replication_volumes = ["{TAPE_REPLICA}", "rclone:{CLOUD_MOCK_DIR}"]
        replicas = 2
        
        janitor_schedule_time = "none"
        janitor_interval_secs = 5
        max_age_days = 0   # <--- Changed to 0 so the test archives instantly
        max_versions = 3
        
        exclude_dirs = ["/.git/", "/node_modules/"]
        temp_extensions = [".swp", ".tmp", "~"]
        immediate_archive_extensions = ["mp4", "iso"]
        immediate_archive_dirs = ["/ArchiveDrop/"]
    """)
    with open(CONFIG_FILE, "w") as f:
        f.write(toml)

def cleanup_environment():
    """Wipes old test files to ensure a clean run."""
    logging.info("🧹 Cleaning up old test environment...")
    subprocess.run(["sudo", "rm", "-f", DB_PATH, f"{DB_PATH}-shm", f"{DB_PATH}-wal", TAPE_PRIMARY, TAPE_REPLICA, TAPE_REPACK, CONFIG_FILE])
    subprocess.run(["sudo", "rm", "-rf", HOT_TIER])
    subprocess.run(["sudo", "rm", "-rf", CLOUD_MOCK_DIR])
    os.makedirs(HOT_TIER, exist_ok=True)
    os.makedirs(CLOUD_MOCK_DIR, exist_ok=True)

def main():
    logging.info("🚀 STARTING HUSKHOARD ARCHIVER COMPREHENSIVE TEST")
    cleanup_environment()
    generate_toml()

    # Verify rclone exists (Optional warning)
    if not shutil.which("rclone"):
        logging.warning("⚠️ 'rclone' is not installed! Cloud replication tests will fail or be skipped by Husk.")

    # 1. Format Multiple Tapes
    logging.info("📼 Formatting Primary and Local Replica Tapes...")
    for tape in [TAPE_PRIMARY, TAPE_REPLICA]:
        run_cmd(["fallocate", "-l", "500M", tape])
        run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "format", "--tape-dev", tape])

    # 2. Start the Daemon
    logging.info("🎧 Starting HuskHoard Daemon (Grid Mode: Primary + Replica + Cloud Mock)...")
    daemon_cmd = ["sudo", "-E", "./target/release/huskhoard", "--config", CONFIG_FILE, "daemon"]
    daemon_env = os.environ.copy()
    daemon_env["RUST_LOG"] = "info"
    daemon_log_file = open("huskhoard_daemon_output.log", "w")
    
    daemon_process = subprocess.Popen(
        daemon_cmd, 
        stdin=subprocess.DEVNULL,      # <--- Disconnects daemon's input from your terminal
        stdout=daemon_log_file, 
        stderr=subprocess.STDOUT, 
        env=daemon_env,
        start_new_session=True         # <--- Fully isolates the process group
    )
    time.sleep(3) # Give daemon time to init

    try:
        # 3. Test Immediate Archive Feature
        logging.info("🎬 Testing Immediate Archive Extension (.mp4)...")
        mp4_file = os.path.join(HOT_TIER, "holiday_video.mp4")
        with open(mp4_file, "w") as f:
            f.write("FAKE VIDEO" * 1000)
        # Should stub very quickly since it matches immediate_archive_extensions
        wait_for_stubbing(mp4_file, timeout=20)

        # 4. Test Subfolder Discovery & Background Rescan
        logging.info("📁 Creating dynamic subfolders to test recursive Fanotify...")
        sub_dir = os.path.join(HOT_TIER, "deep_project_folder")
        os.makedirs(sub_dir, exist_ok=True)
        
        logging.info("⏳ Waiting 16s for the Daemon's background thread to attach to the new folder...")
        time.sleep(16)

        sub_file = os.path.join(sub_dir, "deep_data.txt")
        with open(sub_file, "w") as f:
            f.write("DEEP FOLDER DATA " * 500)
            
        wait_for_stubbing(sub_file)

        # Rehydrate Subfolder file
        logging.info("📖 Reading subfolder file to test Rehydration...")
        with open(sub_file, "r") as f:
            content = f.read(50)
            logging.info(f"   💧 Rehydrated Data: {content.strip()}")

        # 5. Test Versioning & Point-in-Time Rollback
        logging.info("✍️ Testing Versioning Engine...")
        version_file = os.path.join(HOT_TIER, "financial_report.csv")
        
        # Write V1
        with open(version_file, "w") as f:
            f.write("VERSION 1: 2023 Revenue Data\n" * 100)
        wait_for_stubbing(version_file)
        
        # Modify to create V2
        logging.info("📝 Modifying file to create V2...")
        with open(version_file, "a") as f:
            f.write("VERSION 2: 2024 Revenue Data Added\n" * 100)
        wait_for_stubbing(version_file)

        # Delete the file to create Wasteland
        logging.info("🗑️ Deleting file to test Wasteland / Orphaned Data...")
        os.remove(version_file)
# 5.1 Test Exclusion Zones & Temp Files
        logging.info("🚫 Testing Exclusion Zones and Temp Files...")
        exclude_dir = os.path.join(HOT_TIER, "node_modules")
        os.makedirs(exclude_dir, exist_ok=True)
        exclude_file = os.path.join(exclude_dir, "ignore_me.txt")
        temp_file = os.path.join(HOT_TIER, "working.tmp")
        
        with open(exclude_file, "w") as f:
            f.write("IGNORED DATA")
        with open(temp_file, "w") as f:
            f.write("TEMP DATA")
        
        time.sleep(10) # Give janitor time to scan
        if is_stubbed(exclude_file) or is_stubbed(temp_file):
            logging.error("❌ FAILED: Excluded files were incorrectly stubbed to tape!")
        else:
            logging.info("   ✅ SUCCESS: Exclusions respected. Files remained on SSD.")

        # 5.2 Test Metadata (Xattr) Preservation
        logging.info("🏷️ Testing Metadata (Xattr) Preservation...")
        xattr_file = os.path.join(HOT_TIER, "tagged_data.txt")
        with open(xattr_file, "w") as f:
            f.write("XATTR TEST DATA")
        
        # Apply a custom Linux extended attribute
        run_cmd(["sudo", "setfattr", "-n", "user.author", "-v", "JM", xattr_file])
        wait_for_stubbing(xattr_file)
        
        # Read the file to trigger a rehydrate from tape
        with open(xattr_file, "r") as f:
            f.read()
        
        # Verify the custom attribute survived the tape extraction
        res = subprocess.run(["sudo", "getfattr", "--only-values", "-n", "user.author", xattr_file], capture_output=True, text=True)
        if "JM" in res.stdout:
            logging.info("   ✅ SUCCESS: Extended attributes survived the tape round-trip!")
        else:
            logging.error(f"❌ FAILED: Xattrs lost! Got: {res.stdout}")
    finally:
        # Stop Daemon Gracefully
        logging.info("🛑 Stopping HuskHoard Daemon safely...")
        subprocess.run(["sudo", "pkill", "-SIGINT", "husk"])
        daemon_process.wait()

    # 6. Manual PITR Restore (Offline)
    logging.info("🕰️ Testing Point-In-Time Rollback (Restoring V1 from offline catalog)...")
    restore_dest = "./restored_V1_report.csv"
    abs_version_file = os.path.realpath(version_file)
    run_cmd([
        "sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "restore",
        "--file-path", abs_version_file,
        "--dest-path", restore_dest,
        "--version", "1"
    ])
    
    with open(restore_dest, "r") as f:
        logging.info(f"   ✅ Successfully extracted old data: {f.read(30).strip()}")
    os.remove(restore_dest)

    # 7. Scrubber Test
    logging.info("🩺 Running Scrubber on Primary Tape to verify BLAKE3 integrity...")
    run_cmd(["sudo", "./target/release/husk", "--config", CONFIG_FILE, "scrub", "--tape-dev", TAPE_PRIMARY])

    # 8. Repacker (Garbage Collection) Test
    logging.info("♻️ Testing Repacker (Garbage Collection)...")
    run_cmd(["fallocate", "-l", "500M", TAPE_REPACK])
    # The new tape must be formatted before repacking
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "format", "--tape-dev", TAPE_REPACK])
    
    run_cmd([
        "sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "repack",
        "--source-tape", TAPE_PRIMARY,
        "--dest-tape", TAPE_REPACK
    ])
    
    logging.info("📊 Final Tape Gauge (Repacked Tape):")
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "info", "--tape-dev", TAPE_REPACK])
    
    logging.info("☁️  Final Tape Gauge (Mock Cloud Target):")
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "info", "--tape-dev", f"rclone:{CLOUD_MOCK_DIR}"])
# 9. Verify Auto-Catalog Mirroring (Idle Backup)
    logging.info("🪞 Verifying Auto-Catalog Mirroring (Idle Backup)...")
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT version FROM catalog WHERE file_path = '__HUSK_CATALOG_BACKUP__'")
        res = c.fetchone()
        if res:
            logging.info(f"   ✅ SUCCESS: Database anchor mirrored to tape (Version {res[0]})!")
        else:
            logging.error("❌ FAILED: Database mirror not found in catalog!")
        conn.close()
    except Exception as e:
        logging.error(f"❌ FAILED to query DB for mirror: {e}")

    # 10. Disaster Recovery (Rebuild DB from Tape)
    logging.info("🚑 Testing Disaster Recovery (Catalog Rebuild from Tape)...")
    recovered_db = "huskhoard_recovered_test.db"
    subprocess.run(["sudo", "rm", "-f", recovered_db])
    
    # We use the primary tape to rebuild the catalog from scratch
    run_cmd([
        "sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "rebuild", 
        "--tape-dev", TAPE_PRIMARY, 
        "--output-db", recovered_db
    ])
    
    try:
        conn = sqlite3.connect(recovered_db)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM catalog")
        count = c.fetchone()[0]
        if count > 0:
            logging.info(f"   ✅ SUCCESS: Deep Scan rebuilt catalog with {count} records!")
        else:
            logging.error("❌ FAILED: Rebuilt catalog is empty!")
        conn.close()
    except Exception as e:
        logging.error(f"❌ FAILED to read recovered DB: {e}")
    logging.info("🎉 COMPREHENSIVE TEST COMPLETE.")

if __name__ == "__main__":
    main()
