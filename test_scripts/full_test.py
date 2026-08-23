# This tests all of the main features. move it to the huskhoard folder and run it with sudo, after you have installed the dependentcies. it will launch the huskhoard main and create storage volumes and catalogs.
# It then populates the volumes and creates replica versions. 
# the log will print all actions. this is ai generated code as you can see by the emoji intensity, but i decided to leave them in. 







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
TAPE_PRIMARY_1 = "/tmp/tape_primary_1.img"
TAPE_PRIMARY_2 = "/tmp/tape_primary_2.img"
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
        http_port = 8080
        min_free_space_gb = 0
        
        primary_volumes = ["{TAPE_PRIMARY_1}", "{TAPE_PRIMARY_2}"]
        failover_volumes = []
        replication_volumes = ["{TAPE_REPLICA}", "rclone:{CLOUD_MOCK_DIR}"]
        replicas = 2
        
        janitor_schedule_time = "none"
        janitor_interval_secs = 5
        max_age_days = 0   
        max_versions = 3
        retention_days = 0
        
        exclude_dirs = ["/.git/", "/node_modules/"]
        temp_extensions = [".swp", ".tmp", "~"]
        immediate_archive_extensions = ["mp4", "iso"]
        immediate_archive_dirs = ["/ArchiveDrop/"]
        no_compress_extensions = ["mp4", "iso", "zip", "jpg"]
    """)
    with open(CONFIG_FILE, "w") as f:
        f.write(toml)

def cleanup_environment():
    """Wipes old test files to ensure a clean run."""
    logging.info("🧹 Cleaning up old test environment...")
    subprocess.run(["sudo", "rm", "-f", DB_PATH, f"{DB_PATH}-shm", f"{DB_PATH}-wal", TAPE_PRIMARY_1, TAPE_PRIMARY_2, TAPE_REPLICA, TAPE_REPACK, CONFIG_FILE])
    subprocess.run(["sudo", "rm", "-rf", HOT_TIER])
    subprocess.run(["sudo", "rm", "-rf", CLOUD_MOCK_DIR])
    os.makedirs(HOT_TIER, exist_ok=True)
    os.makedirs(CLOUD_MOCK_DIR, exist_ok=True)

def simulate_jbod_lifecycle():
    """
    Phase 2: Enterprise JBOD Simulation.
    Validates Sequential Fill, fragmentation over time, and Garbage Collection (Repack).
    """
    logging.info("\n" + "="*60)
    logging.info("🏢 PHASE 2: ENTERPRISE JBOD LIFECYCLE SIMULATION")
    logging.info("="*60)

    JBOD_TIER = "jbod_hot_tier"
    JBOD_DB = "jbod_catalog.db"
    JBOD_CONFIG = "jbod_config.toml"
    DRIVES = [f"/tmp/jbod_drive_{i}.img" for i in range(1, 5)]

    # 1. Clean and Provision
    logging.info("💾 Provisioning 4-Bay JBOD (300MB per drive)...")
    subprocess.run(["sudo", "rm", "-rf", JBOD_TIER, JBOD_DB, JBOD_CONFIG] + DRIVES)
    os.makedirs(JBOD_TIER, exist_ok=True)
    
    for drive in DRIVES:
        run_cmd(["fallocate", "-l", "300M", drive])
        run_cmd(["sudo", "./target/release/huskhoard", "--config", JBOD_CONFIG, "format", "--tape-dev", drive])

    # Generate JBOD specific config (No replicas, Sequential Fill focus)
    toml = textwrap.dedent(f"""\
        hot_tier = "{JBOD_TIER}"
        db_path = "{JBOD_DB}"
        log_level = "info"
        min_free_space_gb = 0
        primary_volumes = {DRIVES}
        failover_volumes = []
        replication_volumes = []
        replicas = 0
        janitor_schedule_time = "none"
        janitor_interval_secs = 2
        max_age_days = 0   
        max_versions = 2
        exclude_dirs = []
        temp_extensions = []
        immediate_archive_extensions = ["bin"]
        immediate_archive_dirs = []
        no_compress_extensions = ["bin"]
    """)
    with open(JBOD_CONFIG, "w") as f: f.write(toml)

    # Start JBOD Daemon
    logging.info("🎧 Starting Dedicated JBOD Daemon...")
    daemon_env = os.environ.copy()
    daemon_env["RUST_LOG"] = "info"
    jbod_log = open("jbod_daemon.log", "w")
    daemon_proc = subprocess.Popen(
        ["sudo", "-E", "./target/release/huskhoard", "--config", JBOD_CONFIG, "daemon"],
        stdin=subprocess.DEVNULL, stdout=jbod_log, stderr=subprocess.STDOUT, env=daemon_env, start_new_session=True
    )
    time.sleep(3)

    try:
        def make_file(name, size_mb):
            path = os.path.join(JBOD_TIER, name)
            logging.info(f"   Writing {size_mb}MB to {name}...")
            with open(path, "wb") as f:
                f.write(os.urandom(size_mb * 1024 * 1024))
            wait_for_stubbing(path, timeout=60)
            return path

        # 2. Test Sequential Fill (Sticky Drive)
        logging.info("🪣 Testing Sequential Fill Logic...")
        file_a = make_file("dataset_A.bin", 100) # Goes to Drive 1
        file_b = make_file("dataset_B.bin", 100) # Goes to Drive 1 (Drive 1 now has 200MB / 300MB)
        file_c = make_file("dataset_C.bin", 120) # Won't fit on Drive 1! Should spill to Drive 2.

        # Verify Placements
        conn = sqlite3.connect(JBOD_DB)
        c = conn.cursor()
        loc_a = c.execute("SELECT t.device_path FROM catalog c JOIN tapes t ON c.tape_uuid=t.tape_uuid WHERE c.file_path=? ORDER BY version DESC LIMIT 1", (os.path.abspath(file_a),)).fetchone()[0]
        loc_b = c.execute("SELECT t.device_path FROM catalog c JOIN tapes t ON c.tape_uuid=t.tape_uuid WHERE c.file_path=? ORDER BY version DESC LIMIT 1", (os.path.abspath(file_b),)).fetchone()[0]
        loc_c = c.execute("SELECT t.device_path FROM catalog c JOIN tapes t ON c.tape_uuid=t.tape_uuid WHERE c.file_path=? ORDER BY version DESC LIMIT 1", (os.path.abspath(file_c),)).fetchone()[0]
        conn.close()

        if loc_a == DRIVES[0] and loc_b == DRIVES[0] and loc_c == DRIVES[1]:
            logging.info("   ✅ SUCCESS: Sequential Fill worked perfectly! Drive 1 filled, then spilled to Drive 2.")
        else:
            logging.error(f"   ❌ FAILED: Drives filled out of order! (A:{loc_a}, B:{loc_b}, C:{loc_c})")

       # 3. Simulate Fragmentation (Wasteland generation)
        logging.info("🌪️ Simulating user edits to generate Wasteland on Drive 1...")
        # Overwriting File A with a tiny file creates a new version, leaving the old 100MB version as wasteland!
        # Fix: Use "w+b" to request Read/Write. This triggers the daemon's read-interceptor
        # and its O_TRUNC fast-path bypass, removing the stub flag so it gets re-archived.
        with open(file_a, "w+b") as f:
            f.write(b"TINY_NEW_VERSION_DATA")
        wait_for_stubbing(file_a, timeout=60)

        # Print Drive 1 Health (Should show high reclaimable space)
        logging.info(f"📊 Drive 1 Health BEFORE Repack:")
        run_cmd(["sudo", "./target/release/huskhoard", "--config", JBOD_CONFIG, "info", "--tape-dev", DRIVES[0]])

        # 4. Repack (Garbage Collection)
        logging.info("♻️ Running Repacker: Moving surviving data from Drive 1 -> Drive 4...")
        run_cmd([
            "sudo", "./target/release/huskhoard", "--config", JBOD_CONFIG, "repack",
            "--source-tape", DRIVES[0],
            "--dest-tape", DRIVES[3]
        ])

        # 5. Verify Garbage Collection
        conn = sqlite3.connect(JBOD_DB)
        c = conn.cursor()
        loc_b_new = c.execute("SELECT t.device_path FROM catalog c JOIN tapes t ON c.tape_uuid=t.tape_uuid WHERE c.file_path=? ORDER BY version DESC LIMIT 1", (os.path.abspath(file_b),)).fetchone()[0]
        count_drive_1 = c.execute("SELECT COUNT(*) FROM catalog c JOIN tapes t ON c.tape_uuid=t.tape_uuid WHERE t.device_path=?", (DRIVES[0],)).fetchone()[0]
        conn.close()

        if loc_b_new == DRIVES[3]:
            logging.info("   ✅ SUCCESS: Surviving active data (Dataset B) successfully evacuated to Drive 4.")
        else:
            logging.error(f"   ❌ FAILED: Data was not moved to Drive 4! It is on {loc_b_new}")

        if count_drive_1 == 0:
            logging.info("   ✅ SUCCESS: Drive 1 is completely purged from the catalog and can be physically formatted or swapped!")
        else:
            logging.error(f"   ❌ FAILED: Drive 1 still has {count_drive_1} ghost records in the DB.")

        # Print Drive 4 Health (Should show 100MB used, 0 Wasteland)
        logging.info(f"📊 Drive 4 Health AFTER Repack:")
        run_cmd(["sudo", "./target/release/huskhoard", "--config", JBOD_CONFIG, "info", "--tape-dev", DRIVES[3]])

    finally:
        logging.info("🛑 Stopping JBOD Daemon...")
        subprocess.run(["sudo", "pkill", "-SIGINT", "husk"])
        daemon_proc.wait()
        logging.info("🏢 JBOD PHASE COMPLETE.")

def main():
    logging.info("🚀 STARTING HUSKHOARD ARCHIVER COMPREHENSIVE TEST")
    cleanup_environment()
    generate_toml()

    # Verify rclone exists (Optional warning)
    if not shutil.which("rclone"):
        logging.warning("⚠️ 'rclone' is not installed! Cloud replication tests will fail or be skipped by Husk.")

    # 1. Format Multiple Tapes
    logging.info("📼 Formatting Primary and Local Replica Tapes...")
    run_cmd(["fallocate", "-l", "50M", TAPE_PRIMARY_1])
    run_cmd(["fallocate", "-l", "60M", TAPE_PRIMARY_2])
    run_cmd(["fallocate", "-l", "500M", TAPE_REPLICA])
    
    for tape in [TAPE_PRIMARY_1, TAPE_PRIMARY_2, TAPE_REPLICA]:
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

        # Verify it bypassed compression! (Daemon stores absolute paths)
        abs_mp4_file = os.path.abspath(mp4_file)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT compression_type, payload_size, compressed_size FROM catalog WHERE file_path = ?", (abs_mp4_file,))
        db_res = c.fetchone()
        conn.close()
        
        if db_res and db_res[0] == 0 and db_res[1] == db_res[2]:
            logging.info("   ✅ SUCCESS: .mp4 file correctly bypassed compression (Raw bytes written to volume).")
        else:
            logging.error(f"   ❌ FAILED: .mp4 file was compressed or not found! (DB Data: {db_res})")
            
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
            
        # 5.3 Test StreamGate (Method 1: Zstd Compressed Frames)
        logging.info("🚪 Testing StreamGate Method 1: Zstd Compressed Frames...")
        sg_file = os.path.realpath(os.path.join(HOT_TIER, "streamgate_test.bin"))
        abs_sg_file = sg_file
        
        target_offset = 18 * 1024 * 1024  # 18 MB offset to force it into Frame 2
        secret_payload = "STREAMGATE_SECRET_PAYLOAD_DATA"
        
        logging.info("   Writing ~35MB StreamGate test file (Incompressible data)...")
        with open(sg_file, "wb") as f:
            chunk = 256 * 1024
            remain = target_offset
            while remain > 0:
                f.write(os.urandom(min(chunk, remain)))
                remain -= chunk
            f.write(secret_payload.encode("utf-8"))
            remain = 17 * 1024 * 1024
            while remain > 0:
                f.write(os.urandom(min(chunk, remain)))
                remain -= chunk
            
        wait_for_stubbing(sg_file)
        
        # Verify Frame Tracking
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT uncompressed_offset, compressed_offset, compressed_size FROM object_frames WHERE file_path = ?", (abs_sg_file,))
            frames = c.fetchall()
            logging.info(f"   [DB TELEMETRY] Jump Table for {abs_sg_file}:")
            for idx, fr in enumerate(frames):
                logging.info(f"      Frame {idx}: Uncompressed Start: {fr[0]}, Compressed Start: {fr[1]}, Compressed Size: {fr[2]}")
            conn.close()
        except Exception as e:
            logging.error(f"   [DB TELEMETRY] Failed to read jump table: {e}")

        cat_env = os.environ.copy()
        cat_env["RUST_LOG"] = "debug"
        cat_env["RUST_BACKTRACE"] = "1"

        cat_res = subprocess.run([
            "sudo", "-E", "./target/release/huskhoard", "--config", CONFIG_FILE, "cat",
            "--file-path", abs_sg_file,
            "--offset", str(target_offset),
            "--length", str(len(secret_payload))
        ], capture_output=True, text=True, env=cat_env)
        
        if cat_res.stdout == secret_payload:
            logging.info("   ✅ SUCCESS: StreamGate extracted the exact bytes from Frame 2!")
        else:
            logging.error("   ❌ FAILED: StreamGate extraction error!")
            logging.error(f"   --- CAT STDERR ---\n{cat_res.stderr}\n   ------------------")

        # 5.4 Test StreamGate (Method 2: Native Video & Metadata Hoisting)
        logging.info("🎥 Testing StreamGate Method 2: Native Video & Metadata Hoisting...")
        mp4_file = os.path.realpath(os.path.join(HOT_TIER, "streamgate_video.mp4"))
        
        mdat_size = 5 * 1024 * 1024 # 5 MB video payload
        moov_payload = b"FAKE_MOOV_METADATA_" * 300 # ~5700 bytes (forces TLV buffer overflow & extension blocks > 0)
        video_target_offset = 8 + (2 * 1024 * 1024) # 2MB into the mdat payload
        expected_video_text = "NATIVE_VIDEO_DIRECT_SEEK_TARGET_DATA_12345"
        
        logging.info("   Generating fake MP4 with 'moov' atom at the EOF...")
        with open(mp4_file, "wb") as f:
            f.write((mdat_size + 8).to_bytes(4, 'big') + b"mdat")
            f.write(os.urandom(2 * 1024 * 1024))
            f.write(expected_video_text.encode("utf-8"))
            
            remain = mdat_size - (2 * 1024 * 1024) - len(expected_video_text)
            f.write(os.urandom(remain))
            
            f.write((len(moov_payload) + 8).to_bytes(4, 'big') + b"moov")
            f.write(moov_payload)
            
        wait_for_stubbing(mp4_file)
        
        # Test A: Verify Metadata Hoisting by checking Extension Blocks in DB
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT ext_blocks, compression_type FROM catalog WHERE file_path = ? ORDER BY version DESC LIMIT 1", (mp4_file,))
        mp4_db = c.fetchone()
        conn.close()
        
        if mp4_db and mp4_db[1] == 0:
            logging.info("   ✅ SUCCESS: Video compression bypassed properly (compression_type = 0).")
        else:
            logging.error(f"   ❌ FAILED: Video was compressed! DB returned: {mp4_db}")

        if mp4_db and mp4_db[0] > 0:
            logging.info(f"   ✅ SUCCESS: 'moov' atom was hoisted! Catalog shows {mp4_db[0]} extension block(s) used in Volume Header.")
        else:
            logging.error(f"   ❌ FAILED: Metadata was not hoisted or extension blocks missing! DB returned: {mp4_db}")

        # Test B: O(1) Seek Extraction
        cat_res_vid = subprocess.run([
            "sudo", "-E", "./target/release/huskhoard", "--config", CONFIG_FILE, "cat",
            "--file-path", mp4_file,
            "--offset", str(video_target_offset),
            "--length", str(len(expected_video_text))
        ], capture_output=True, text=True, env=cat_env)
        
        if cat_res_vid.stdout == expected_video_text:
            logging.info("   ✅ SUCCESS: Native Video O(1) seek instantly extracted correct bytes!")
        else:
            logging.error("   ❌ FAILED: Native video seek extraction error!")
            logging.error(f"   --- CAT STDERR ---\n{cat_res_vid.stderr}\n   ------------------")

        
        # ---------------------------------------------------------
        # EDGE CASE BATTERY
        # ---------------------------------------------------------
        
        # Edge Case A: Zero-Byte Files
        logging.info("👻 Edge Case A: Testing Zero-Byte File Handling...")
        empty_file = os.path.join(HOT_TIER, "empty_ghost.mp4") # .mp4 triggers immediate archive
        with open(empty_file, "w") as f:
            pass # Literally zero bytes
            
        if not wait_for_stubbing(empty_file, timeout=30):
            logging.error("   ❌ FAILED: Zero-byte file failed to stub (Daemon fallocate kernel rejection?)")
        else:
            with open(empty_file, "r") as f:
                if f.read() != "":
                    logging.error("   ❌ FAILED: Zero-byte file contains garbage data after rehydration!")
                else:
                    logging.info("   ✅ SUCCESS: Zero-byte file archived and rehydrated flawlessly.")

        # Edge Case B: O_TRUNC Fast-Path Bypass
        logging.info("⚡ Edge Case B: Testing O_TRUNC Overwrite Bypass...")
        bypass_file = os.path.join(HOT_TIER, "fast_bypass.mp4")
        with open(bypass_file, "w") as f:
            f.write("OLD DATA")
        wait_for_stubbing(bypass_file)
        
        start_time = time.time()
        # Python 'w+' asks for read/write. This forces FAN_ACCESS_PERM to trigger, 
        # allowing us to truly measure the O_TRUNC daemon bypass logic.
        with open(bypass_file, "w+") as f: 
            f.write("NEW DATA OVERWRITE")
        elapsed = time.time() - start_time
        
        if elapsed < 0.5:
            logging.info(f"   ✅ SUCCESS: O_TRUNC instantly bypassed tape read! (Took {elapsed:.4f}s)")
        else:
            logging.error(f"   ❌ FAILED: O_TRUNC took too long, likely triggered an unnecessary tape restore. ({elapsed:.4f}s)")

        # Edge Case C: Concurrent Rehydration (Mutex check)
        import threading
        logging.info("🏃 Edge Case C: Testing Concurrent Rehydration (Mutex Lock Check)...")
        race_file = os.path.join(HOT_TIER, "race_condition.mp4")
        with open(race_file, "w") as f:
            f.write("RACE_DATA " * 50)
        wait_for_stubbing(race_file)
        
        race_results = []
        def concurrent_read(tid):
            try:
                with open(race_file, "r") as f:
                    race_results.append(f.read(9) == "RACE_DATA")
            except Exception as e:
                logging.error(f"      Thread {tid} crashed: {e}")
                race_results.append(False)
                
        threads = [threading.Thread(target=concurrent_read, args=(i,)) for i in range(5)]
        for t in threads: t.start()
        for t in threads: t.join()
        
        if all(race_results) and len(race_results) == 5:
            logging.info("   ✅ SUCCESS: 5 concurrent reads safely queued and rehydrated without crashing or locking up!")
        else:
            logging.error(f"   ❌ FAILED: Concurrent reads caused corruption or deadlock! Results: {race_results}")

# ---------------------------------------------------------
        # HTTP GATEWAY BATTERY (Plex/VLC Simulation)
        # ---------------------------------------------------------
        import http.client
        
        logging.info("🌐 Testing HTTP Streaming Gateway (Plex/VLC Bridge)...")
        http_file = os.path.join(HOT_TIER, "plex_mock.mp4")
        # Create a mock video file with distinct sections
        http_payload = b"HEADER_DATA" + (b"0" * 1024 * 1024) + b"MIDDLE_CHUNK" + (b"1" * 1024 * 1024) + b"FOOTER_DATA"
        with open(http_file, "wb") as f:
            f.write(http_payload)
        
        wait_for_stubbing(http_file)

        try:
            # 1. Test HEAD Request (Used by players to probe file size)
            logging.info("   Testing HTTP HEAD Request...")
            conn = http.client.HTTPConnection("127.0.0.1", 8080, timeout=5)
            conn.request("HEAD", "/stream/plex_mock.mp4")
            res = conn.getresponse()
            if res.status == 206 or res.status == 200:
                content_len = int(res.getheader("Content-Length", 0))
                if content_len == len(http_payload):
                    logging.info(f"      ✅ SUCCESS: HEAD returned correct Content-Length ({content_len})")
                else:
                    logging.error(f"      ❌ FAILED: HEAD returned wrong length: {content_len}")
            else:
                logging.error(f"      ❌ FAILED: HEAD returned HTTP {res.status}")
            conn.close()

            # 2. Test 206 Partial Content (Targeted byte range)
            logging.info("   Testing HTTP 206 Partial Content (Seeking)...")
            conn = http.client.HTTPConnection("127.0.0.1", 8080, timeout=5)
            conn.request("GET", "/stream/plex_mock.mp4", headers={"Range": "bytes=0-10"})
            res = conn.getresponse()
            partial_data = res.read()
            if res.status == 206 and partial_data == b"HEADER_DATA":
                logging.info("      ✅ SUCCESS: Range request accurately extracted exact bytes.")
            else:
                logging.error(f"      ❌ FAILED: Range request returned {res.status} | Data: {partial_data}")
            conn.close()

            # 3. Simulate Violent Interruption & Resume (The "Scrub" Test)
            logging.info("   Testing Connection Drop & Resume (Broken Pipe Squelching)...")
            conn = http.client.HTTPConnection("127.0.0.1", 8080, timeout=5)
            conn.request("GET", "/stream/plex_mock.mp4")
            res = conn.getresponse()
            
            # Read just a little bit, then brutally sever the TCP socket
            chunk1 = res.read(1024) 
            conn.close() 
            logging.info("      💥 Connection violently killed (Simulating user skipping forward)...")
            
            # Immediately open a new connection and ask for the rest
            time.sleep(0.5) # Give Husk 500ms to gracefully drop the broken thread
            conn2 = http.client.HTTPConnection("127.0.0.1", 8080, timeout=5)
            conn2.request("GET", "/stream/plex_mock.mp4", headers={"Range": f"bytes=1024-{len(http_payload)-1}"})
            res2 = conn2.getresponse()
            chunk2 = res2.read()
            conn2.close()

            # Reassemble and verify
            if chunk1 + chunk2 == http_payload:
                logging.info("      ✅ SUCCESS: Broken Pipe handled gracefully. Resume stitched perfectly!")
            else:
                logging.error("      ❌ FAILED: Resumed data was corrupted or misaligned.")

            # 4. Test HTTP REST API (Dashboard)
            logging.info("   Testing HTTP REST API (Dashboard)...")
            import json
            conn_api = http.client.HTTPConnection("127.0.0.1", 8080, timeout=5)
            conn_api.request("GET", "/api/dashboard")
            res_api = conn_api.getresponse()
            data = res_api.read().decode('utf-8')
            if res_api.status == 200 and "volumes" in json.loads(data):
                logging.info("      ✅ SUCCESS: Dashboard JSON API responded correctly.")
            else:
                logging.error(f"      ❌ FAILED: Dashboard API returned {res_api.status}")
            conn_api.close()

        except Exception as e:
            logging.error(f"   ❌ FAILED: HTTP Gateway threw an exception: {e}")

        # Edge Case D: Physical Media Failure (Replica Failover)
        logging.info("🔥 Edge Case D: Simulating Primary Drive Failure (Seamless Failover)...")
        failover_file = os.path.join(HOT_TIER, "mission_critical.mp4")
        with open(failover_file, "w") as f:
            f.write("CRITICAL DATA")
        wait_for_stubbing(failover_file)
        
        # "Unplug" the primary tapes by renaming them
        logging.info("   'Unplugging' Primary Tapes to force Replica failover...")
        run_cmd(["sudo", "mv", TAPE_PRIMARY_1, TAPE_PRIMARY_1 + ".offline"])
        run_cmd(["sudo", "mv", TAPE_PRIMARY_2, TAPE_PRIMARY_2 + ".offline"])
        
        try:
            with open(failover_file, "r") as f:
                if f.read() == "CRITICAL DATA":
                    logging.info("   ✅ SUCCESS: Daemon seamlessly caught the missing primary and read from the Replica!")
                else:
                    logging.error("   ❌ FAILED: Bad data read during failover.")
        except Exception as e:
            logging.error(f"   ❌ FAILED: Application crashed during failover attempt! {e}")
        finally:
            # "Plug" them back in so Steps 7 and 8 (Scrubber/Repacker) can finish later
            run_cmd(["sudo", "mv", TAPE_PRIMARY_1 + ".offline", TAPE_PRIMARY_1])
            run_cmd(["sudo", "mv", TAPE_PRIMARY_2 + ".offline", TAPE_PRIMARY_2])
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
    logging.info("🩺 Running Scrubber on Primary Tape 1 to verify BLAKE3 integrity...")
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "scrub", "--tape-dev", TAPE_PRIMARY_1])

    # 8. Repacker (Garbage Collection) Test
    logging.info("♻️ Testing Repacker (Garbage Collection)...")
    run_cmd(["fallocate", "-l", "500M", TAPE_REPACK])
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "format", "--tape-dev", TAPE_REPACK])
    
    run_cmd([
        "sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "repack",
        "--source-tape", TAPE_PRIMARY_1,
        "--dest-tape", TAPE_REPACK
    ])
    
    logging.info("📊 Final Tape Gauge (Repacked Tape):")
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "info", "--tape-dev", TAPE_REPACK])
    
    logging.info("☁️  Final Tape Gauge (Mock Cloud Target):")
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "info", "--tape-dev", f"rclone:{CLOUD_MOCK_DIR}"])

    # 9. Auto-Catalog Mirroring (Idle Backup) -- SKIPPED
    # run_archive_worker() in daemon.rs gates this behind a hardcoded
    # rx.recv_timeout(Duration::from_secs(3600)) -- a full hour of queue
    # silence -- with no config override. Not reachable in a short-lived
    # test run, so we don't assert on it here.
    logging.info("🪞 Skipping Auto-Catalog Mirroring check (hardcoded 1hr idle timer, not exercisable in this run).")

    # 9.0 Restart daemon so the Janitor can actually stub files for the rm/prune tests below
    logging.info("🎧 Restarting HuskHoard Daemon for Hard Remove / Prune tests...")
    daemon_env = os.environ.copy()
    daemon_env["RUST_LOG"] = "info"
    daemon_log_file2 = open("huskhoard_daemon_output_phase2.log", "w")
    daemon_process2 = subprocess.Popen(
        ["sudo", "-E", "./target/release/huskhoard", "--config", CONFIG_FILE, "daemon"],
        stdin=subprocess.DEVNULL, stdout=daemon_log_file2, stderr=subprocess.STDOUT,
        env=daemon_env, start_new_session=True
    )
    time.sleep(3)

    # 9.1 Test Hard Remove (rm)
    logging.info("🗑️ Testing Hard Remove (CLI 'rm')...")
    rm_test_file = os.path.join(HOT_TIER, "rm_test.txt")
    with open(rm_test_file, "w") as f:
        f.write("TO BE DELETED FOREVER")
    wait_for_stubbing(rm_test_file)
    abs_rm_file = os.path.realpath(rm_test_file)
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "rm", "--file-path", abs_rm_file])
    
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM catalog WHERE file_path = ?", (abs_rm_file,)).fetchone()[0]
    conn.close()
    if not os.path.exists(rm_test_file) and count == 0:
        logging.info("   ✅ SUCCESS: File permanently removed from SSD and Catalog.")
    else:
        logging.error("   ❌ FAILED: File still exists on SSD or in Catalog after rm!")

    # 9.2 Test Catalog Prune
    logging.info("✂️ Testing Catalog Prune...")
    prune_test_file = os.path.join(HOT_TIER, "prune_test.txt")
    with open(prune_test_file, "w") as f:
        f.write("TO BE PRUNED")
    wait_for_stubbing(prune_test_file)
    abs_prune_file = os.path.realpath(prune_test_file)
    os.remove(prune_test_file) # User violently deletes stub without using husk rm
    
    logging.info("🕵️ DEBUG: Dumping active config file contents...")
    run_cmd(["cat", CONFIG_FILE], capture=False)
    logging.info("🕵️ DEBUG: Executing Prune...")
    
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "prune"])
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM catalog WHERE file_path = ?", (abs_prune_file,)).fetchone()[0]
    conn.close()
    if count == 0:
        logging.info("   ✅ SUCCESS: Prune detected missing stub and purged catalog record.")
    else:
        logging.error("   ❌ FAILED: Prune failed to remove orphaned catalog record!")

    logging.info("🛑 Stopping HuskHoard Daemon (Phase 2)...")
    subprocess.run(["sudo", "pkill", "-SIGINT", "husk"])
    daemon_process2.wait()

    # 9.3 Test Parquet Export
    logging.info("📊 Testing Parquet Data Export...")
    export_file = "test_export.parquet"
    run_cmd(["sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "export", "--format", "parquet", "--output", export_file])
    if os.path.exists(export_file) and os.path.getsize(export_file) > 0:
        logging.info("   ✅ SUCCESS: Parquet export created successfully.")
        os.remove(export_file)
    else:
        logging.error("   ❌ FAILED: Parquet export missing or empty!")
    logging.info("🚑 Testing Disaster Recovery (Catalog Rebuild from Tape 1)...")
    recovered_db = "huskhoard_recovered_test.db"
    subprocess.run(["sudo", "rm", "-f", recovered_db])
    
    run_cmd([
        "sudo", "./target/release/huskhoard", "--config", CONFIG_FILE, "rebuild", 
        "--tape-dev", TAPE_PRIMARY_1, 
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
    
    simulate_jbod_lifecycle()
        
    logging.info("🎉 COMPREHENSIVE TEST COMPLETE.")

if __name__ == "__main__":
    main() 
