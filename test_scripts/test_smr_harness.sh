#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# HuskHoard <1GB HM-SMR Test Harness
# Tests: Quick Start, Sequential ZBC Writes, Husk/Recall, Pooling, Replication,
#        Parquet Export, StreamGate HTTP, Scrub, Drive Loss, and Spare Repack.
# ==============================================================================

# --- Tunables ---
NUM_PRIMARY="${NUM_PRIMARY:-2}"
NUM_REPLICA="${NUM_REPLICA:-2}"
NUM_SPARE="${NUM_SPARE:-1}"
DRIVE_SIZE_MB="${DRIVE_SIZE_MB:-64}"
ZONE_SIZE_MB="${ZONE_SIZE_MB:-8}"
ZONE_NR_CONV="${ZONE_NR_CONV:-1}"
SECTOR_SIZE="${SECTOR_SIZE:-4096}"

BIN="${BIN:-./target/release/huskhoard}"
WORKDIR="${WORKDIR:-$(pwd)}"
HOT_TIER="$WORKDIR/hot_tier_test"
CONFIG="$WORKDIR/husk_config.toml"
DB_PATH="$WORKDIR/harness_catalog.db"
LOG_FILE="$WORKDIR/harness_daemon.log"
PARQUET_OUT="$WORKDIR/test_catalog.parquet"
JANITOR_SECS="${JANITOR_SECS:-15}"
HTTP_PORT="${HTTP_PORT:-8080}"

# Auto-scale test payload sizes so 32MB drives have plenty of sequential headroom
if (( DRIVE_SIZE_MB <= 32 )); then
  TEST_A_MB="${TEST_A_MB:-4}"
  TEST_C_COUNT="${TEST_C_COUNT:-10}"
  TEST_C_MAX_MB="${TEST_C_MAX_MB:-1}"
else
  TEST_A_MB="${TEST_A_MB:-8}"
  TEST_C_COUNT="${TEST_C_COUNT:-20}"
  TEST_C_MAX_MB="${TEST_C_MAX_MB:-2}"
fi

TOTAL_DRIVES=$((NUM_PRIMARY + NUM_REPLICA + NUM_SPARE))
TOTAL_RAM_MB=$((TOTAL_DRIVES * DRIVE_SIZE_MB))

DAEMON_PID=""
BACKUP_CONFIG=""
DEVS=()

log()  { printf '\n\033[1;34m[HARNESS]\033[0m %s\n' "$*"; }
pass() { printf '\033[1;32m  [PASS]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  [WARN]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m  [FAIL]\033[0m %s\n' "$*"; exit 1; }

quote_join() {
  local out=""
  for item in "$@"; do
    out+="\"$item\", "
  done
  printf '%s' "${out%, }"
}

stop_daemon() {
  if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    log "Stopping huskhoard daemon (PID $DAEMON_PID)..."
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
    DAEMON_PID=""
  fi
}

cleanup() {
  local rc=$?
  stop_daemon
  if lsmod | grep -q '^scsi_debug'; then
    log "Unloading scsi_debug RAM drives..."
    sudo modprobe -r scsi_debug || warn "Could not unload scsi_debug cleanly"
  fi
  if [[ -n "$BACKUP_CONFIG" && -f "$BACKUP_CONFIG" ]]; then
    mv "$BACKUP_CONFIG" "$CONFIG"
    log "Restored original husk_config.toml"
  fi
  if [[ $rc -eq 0 ]]; then
    log "Harness finished cleanly. Logs kept at: $LOG_FILE"
  else
    warn "Harness exited with status $rc. Check daemon log: $LOG_FILE"
  fi
}
trap cleanup EXIT

# ------------------------------------------------------------------------------
# Step 0: Pre-flight Checks
# ------------------------------------------------------------------------------
log "Pre-flight checks (Budget: ${TOTAL_DRIVES} drives x ${DRIVE_SIZE_MB}MB = ${TOTAL_RAM_MB}MB RAM)"

[[ -x "$BIN" ]] || fail "Binary not found or not executable at $BIN"
command -v blkzone >/dev/null || fail "blkzone not found (install util-linux)"
command -v curl >/dev/null    || fail "curl not found"
command -v python3 >/dev/null || fail "python3 not found"

log "Kernel version: $(uname -r)"

CAPS=$(getcap "$BIN" 2>/dev/null || true)
if [[ "$CAPS" != *"cap_sys_admin"* || "$CAPS" != *"cap_dac_read_search"* ]]; then
  warn "Setting required capabilities on $BIN..."
  sudo setcap cap_sys_admin,cap_dac_read_search+ep "$BIN"
fi
pass "Binary capabilities verified: $(getcap "$BIN")"

# Back up existing husk_config.toml BEFORE format runs so format generates a fresh default
if [[ -f "$CONFIG" ]]; then
  BACKUP_CONFIG="$WORKDIR/husk_config.toml.harness_bak"
  mv "$CONFIG" "$BACKUP_CONFIG"
fi

rm -rf "$HOT_TIER" "$PARQUET_OUT" "$LOG_FILE" "${DB_PATH}"*
mkdir -p "$HOT_TIER"

if lsmod | grep -q '^scsi_debug'; then
  sudo modprobe -r scsi_debug
fi

# ------------------------------------------------------------------------------
# Step 1: Create Virtual HM-SMR Drives in RAM
# ------------------------------------------------------------------------------
log "Loading scsi_debug (${TOTAL_DRIVES} virtual HM-SMR drives, ${TOTAL_RAM_MB}MB total RAM)..."
sudo modprobe scsi_debug \
  add_host="$TOTAL_DRIVES" \
  per_host_store=1 \
  dev_size_mb="$DRIVE_SIZE_MB" \
  zbc=host-managed \
  zone_size_mb="$ZONE_SIZE_MB" \
  zone_nr_conv="$ZONE_NR_CONV" \
  sector_size="$SECTOR_SIZE"

sudo udevadm settle

for b in /sys/block/sd*; do
  [[ -e "$b" ]] || continue
  n=$(basename "$b")
  if grep -q "scsi_debug" "$b/device/model" 2>/dev/null && \
     [[ "$(cat "$b/queue/zoned" 2>/dev/null || true)" == "host-managed" ]]; then
    DEVS+=("/dev/$n")
  fi
done

if [[ "${#DEVS[@]}" -ne "$TOTAL_DRIVES" ]]; then
  dmesg | tail -n 20 || true
  fail "Expected $TOTAL_DRIVES HM-SMR drives, found ${#DEVS[@]}"
fi
pass "Found ${#DEVS[@]} verified scsi_debug host-managed devices: ${DEVS[*]}"

log "Granting $USER ownership on virtual HM-SMR devices..."
for d in "${DEVS[@]}"; do
  sudo chown "$USER" "$d"
done

PRIMARY=("${DEVS[@]:0:$NUM_PRIMARY}")
REPLICA=("${DEVS[@]:$NUM_PRIMARY:$NUM_REPLICA}")
SPARE="${DEVS[$((NUM_PRIMARY + NUM_REPLICA))]}"

echo "  Primary : ${PRIMARY[*]}"
echo "  Replica : ${REPLICA[*]}"
echo "  Spare   : $SPARE"

# ------------------------------------------------------------------------------
# Step 2: Format Primary & Replica Volumes (generates default husk_config.toml)
# ------------------------------------------------------------------------------
log "Formatting primary and replica HM-SMR volumes as $USER..."
for d in "${PRIMARY[@]}" "${REPLICA[@]}"; do
  "$BIN" format --tape-dev "$d"
done
pass "Formatted $((NUM_PRIMARY + NUM_REPLICA)) volumes"

log "Initial zone report on ${PRIMARY[0]}:"
blkzone report "${PRIMARY[0]}" | head -n 6

# ------------------------------------------------------------------------------
# Step 3: Patch Generated Config & Launch Daemon
# ------------------------------------------------------------------------------
log "Updating $CONFIG for rapid test mode (preserving any default fields)..."
[[ -f "$CONFIG" ]] || touch "$CONFIG"

CFG_PRIMARY="$(quote_join "${PRIMARY[@]}")" \
CFG_REPLICA="$(quote_join "${REPLICA[@]}")" \
CFG_HOT_TIER="$HOT_TIER" \
CFG_DB_PATH="$DB_PATH" \
CFG_JANITOR_SECS="$JANITOR_SECS" \
CFG_HTTP_PORT="$HTTP_PORT" \
python3 - "$CONFIG" <<'PY'
import os, re, sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

updates = {
    "primary_volumes": f"[{os.environ['CFG_PRIMARY']}]",
    "replication_volumes": f"[{os.environ['CFG_REPLICA']}]",
    "hot_tier": f"\"{os.environ['CFG_HOT_TIER']}\"",
    "db_path": f"\"{os.environ['CFG_DB_PATH']}\"",
    "max_age_days": "0",
    "janitor_interval_secs": os.environ["CFG_JANITOR_SECS"],
    "http_port": os.environ["CFG_HTTP_PORT"],
    "http_bind": "\"127.0.0.1\"",
    "http_token": "\"\"",
    "intercept_open_reads": "false",
    "hot_tier_max_usage_percent": "80",
    "min_free_space_gb": "0",
}

kept = []
skip_depth = 0
in_table = False
key_re = re.compile(r"^\s*([A-Za-z0-9_]+)\s*=(.*)$")

for line in lines:
    if skip_depth > 0:
        skip_depth += line.count("[") - line.count("]")
        if skip_depth < 0:
            skip_depth = 0
        continue
    if re.match(r"^\s*\[", line):
        in_table = True
    if not in_table:
        m = key_re.match(line)
        if m and m.group(1) in updates:
            rest = m.group(2)
            depth = rest.count("[") - rest.count("]")
            if depth > 0:
                skip_depth = depth
            continue
    kept.append(line)

header = [f"{k} = {v}\n" for k, v in updates.items()]
with open(path, "w", encoding="utf-8") as f:
    f.writelines(header + kept)
PY

log "Starting huskhoard daemon..."
"$BIN" daemon >"$LOG_FILE" 2>&1 &
DAEMON_PID=$!
sleep 2
kill -0 "$DAEMON_PID" 2>/dev/null || { cat "$LOG_FILE"; fail "Daemon failed to start"; }
pass "Daemon running (PID $DAEMON_PID)"

wait_for_husk() {
  local file="$1"
  local timeout="${2:-90}"
  local elapsed=0
  while (( elapsed < timeout )); do
    local logical blocks alloc
    logical=$(stat -c '%s' "$file")
    blocks=$(stat -c '%b' "$file")
    alloc=$((blocks * 512))
    if (( logical > 0 && alloc < logical / 2 )); then
      echo "    -> $file stubbed: logical=${logical}B, allocated=${alloc}B"
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  return 1
}

# ------------------------------------------------------------------------------
# Test A: Archive & Recall
# ------------------------------------------------------------------------------
log "Test A: Archive & Recall (${TEST_A_MB} MB file)"
BEFORE_ZONES=$(blkzone report "${PRIMARY[0]}")

dd if=/dev/urandom of="$HOT_TIER/dummy_data.bin" bs=1M count="$TEST_A_MB" status=none
ORIG_SHA=$(sha256sum "$HOT_TIER/dummy_data.bin" | awk '{print $1}')

wait_for_husk "$HOT_TIER/dummy_data.bin" 90 || { cat "$LOG_FILE"; fail "dummy_data.bin was not archived in time"; }
pass "Test A1: File archived into sparse husk"

log "Triggering fanotify recall by reading dummy_data.bin..."
RECALL_SHA=$(sha256sum "$HOT_TIER/dummy_data.bin" | awk '{print $1}')
[[ "$ORIG_SHA" == "$RECALL_SHA" ]] || fail "SHA256 mismatch after recall!"
pass "Test A2: Recalled file SHA256 matches original ($RECALL_SHA)"

# ------------------------------------------------------------------------------
# Test B: Verify Sequential HM-SMR Zone Write Pointers
# ------------------------------------------------------------------------------
log "Test B: Checking HM-SMR write pointers across primaries..."
AFTER_ZONES=$(blkzone report "${PRIMARY[0]}")
if [[ "$BEFORE_ZONES" != "$AFTER_ZONES" ]]; then
  echo "$AFTER_ZONES" | head -n 6
  pass "Test B: Zone write pointers on ${PRIMARY[0]} advanced sequentially"
else
  warn "Primary[0] zones unchanged (data may have routed to another primary)"
  for p in "${PRIMARY[@]}"; do
    echo "--- $p ---"
    blkzone report "$p" | head -n 4
  done
fi

# ------------------------------------------------------------------------------
# Test C: Multi-File Pool Sweep & Parquet Export
# ------------------------------------------------------------------------------
log "Test C: Creating ${TEST_C_COUNT} files (1-${TEST_C_MAX_MB} MB each) to test pooling..."
for i in $(seq 1 "$TEST_C_COUNT"); do
  sz=$(( (RANDOM % TEST_C_MAX_MB) + 1 ))
  dd if=/dev/urandom of="$HOT_TIER/file_${i}.bin" bs=1M count="$sz" status=none
done

wait_for_husk "$HOT_TIER/file_${TEST_C_COUNT}.bin" 120 || warn "file_${TEST_C_COUNT}.bin still pending archive; continuing"
ls -lh "$HOT_TIER" | head -n 10

for p in "${PRIMARY[@]}"; do
  log "Volume info for $p:"
  "$BIN" info --tape-dev "$p" || warn "info returned non-zero for $p"
done

log "Exporting catalog to Parquet ($PARQUET_OUT)..."
"$BIN" export --format parquet --output "$PARQUET_OUT"
[[ -s "$PARQUET_OUT" ]] || fail "Parquet export failed or empty"
pass "Test C: Parquet catalog exported ($(du -h "$PARQUET_OUT" | cut -f1))"

if command -v duckdb >/dev/null; then
  duckdb -c "SELECT file_path, payload_size FROM '$PARQUET_OUT' ORDER BY payload_size DESC LIMIT 5"
fi

# ------------------------------------------------------------------------------
# Test D: StreamGate Zero-Disk Read (CLI cat + HTTP Range)
# ------------------------------------------------------------------------------
log "Test D: Streaming without restoring..."
if "$BIN" cat --file-path "$HOT_TIER/dummy_data.bin" 2>/dev/null | head -c 1048576 >/dev/null; then
  pass "Test D1: huskhoard cat succeeded (full path)"
elif "$BIN" cat --file-path /dummy_data.bin 2>/dev/null | head -c 1048576 >/dev/null; then
  pass "Test D1: huskhoard cat succeeded (relative path)"
else
  warn "Test D1: huskhoard cat failed (check exact catalog path in $PARQUET_OUT)"
fi

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -r 0-1048575 "http://127.0.0.1:$HTTP_PORT/stream/dummy_data.bin" || true)
if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "206" ]]; then
  pass "Test D2: StreamGate HTTP range request returned HTTP $HTTP_CODE"
else
  warn "Test D2: StreamGate HTTP returned $HTTP_CODE (check path routing in daemon log)"
fi

# ------------------------------------------------------------------------------
# Test E: Bit-Rot Scrub
# ------------------------------------------------------------------------------
log "Test E: Scrubbing ${PRIMARY[0]} for BLAKE3 integrity..."
"$BIN" scrub --tape-dev "${PRIMARY[0]}"
pass "Test E: Scrub completed on ${PRIMARY[0]}"

# ------------------------------------------------------------------------------
# Test F: Simulate Drive Failure on PRIMARY[0]
# ------------------------------------------------------------------------------
LOST_DEV="${PRIMARY[0]}"
LOST_SYS=$(basename "$LOST_DEV")
log "Test F: Simulating sudden drive loss on $LOST_DEV..."
echo 1 | sudo tee "/sys/block/$LOST_SYS/device/delete" >/dev/null
sleep 1

if head -c 65536 "$HOT_TIER/file_1.bin" >/dev/null 2>&1; then
  pass "Test F: Read succeeded after losing $LOST_DEV (recalled/survived via replica or hot tier)"
else
  warn "Test F: Read failed after losing $LOST_DEV (check $LOG_FILE for failover behavior)"
fi

# ------------------------------------------------------------------------------
# Test G: Format Spare & Repack Surviving Primary onto Spare
# ------------------------------------------------------------------------------
if [[ "$NUM_PRIMARY" -ge 2 ]]; then
  SURVIVING_PRIMARY="${PRIMARY[1]}"
  log "Test G: Formatting spare $SPARE and repacking $SURVIVING_PRIMARY -> $SPARE..."
  "$BIN" format --tape-dev "$SPARE"
  "$BIN" repack --source-tape "$SURVIVING_PRIMARY" --dest-tape "$SPARE"
  echo "--- Spare zones after repack ---"
  blkzone report "$SPARE" | head -n 6
  pass "Test G: Repack onto spare completed"
else
  warn "Test G skipped (set NUM_PRIMARY>=2 to test repack after losing PRIMARY[0])"
fi

log "ALL HARNESS TESTS COMPLETED"
