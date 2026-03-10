#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
RUNTIME_ROOT="${ROOT_DIR}/runtime"

VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT:-100}"
OPTICAL_NODE_COUNT="${OPTICAL_NODE_COUNT:-10000}"
OPTICAL_DISK_COUNT="${OPTICAL_DISK_COUNT:-10000}"

REAL_DISK_COUNT="${REAL_DISK_COUNT:-24}"
REAL_DISK_CAPACITY_BYTES="${REAL_DISK_CAPACITY_BYTES:-2199023255552}"   # 2TB
VIRTUAL_DISK_CAPACITY_BYTES="${VIRTUAL_DISK_CAPACITY_BYTES:-2199023255552}"  # 2TB
OPTICAL_DISK_CAPACITY_BYTES="${OPTICAL_DISK_CAPACITY_BYTES:-1099511627776}"  # 1TB
OPTICAL_MAX_IMAGE_SIZE_BYTES="${OPTICAL_MAX_IMAGE_SIZE_BYTES:-67108864}"
OPTICAL_SIMULATE_IO="${OPTICAL_SIMULATE_IO:-true}"
OPTICAL_READ_BYTES_PER_SEC="${OPTICAL_READ_BYTES_PER_SEC:-104857600}"
OPTICAL_WRITE_BYTES_PER_SEC="${OPTICAL_WRITE_BYTES_PER_SEC:-52428800}"
CACHE_READ_BYTES_PER_SEC="${CACHE_READ_BYTES_PER_SEC:-419430400}"
OPTICAL_CACHE_DISC_SLOTS="${OPTICAL_CACHE_DISC_SLOTS:-4}"
OPTICAL_STARTUP_SCAN_MODE="${OPTICAL_STARTUP_SCAN_MODE:-fast}"
HEARTBEAT_MS="${HEARTBEAT_MS:-2000}"

SCHEDULER_PORT="${SCHEDULER_PORT:-9100}"
MDS_PORT="${MDS_PORT:-9000}"
REAL_PORT="${REAL_PORT:-19080}"
VIRTUAL_PORT="${VIRTUAL_PORT:-29080}"
OPTICAL_PORT="${OPTICAL_PORT:-39080}"

REQUIRED_BINS=(
  scheduler_server
  real_node_server
  virtual_node_server
  optical_node_server
  mds_server
)

for bin in "${REQUIRED_BINS[@]}"; do
  if [[ ! -x "${BUILD_DIR}/${bin}" ]]; then
    echo "Missing binary: ${BUILD_DIR}/${bin}" >&2
    echo "Build first: cmake -S . -B build && cmake --build build -j" >&2
    exit 1
  fi
done

mkdir -p "${RUNTIME_ROOT}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RUNTIME_ROOT}/interaction_cluster_${RUN_ID}"
LOG_DIR="${RUN_DIR}/logs"
CONF_DIR="${RUN_DIR}/config"
DATA_DIR="${RUN_DIR}/data"
mkdir -p "${LOG_DIR}" "${CONF_DIR}" "${DATA_DIR}"

make_csv_ids() {
  local prefix="$1"
  local count="$2"
  local out=""
  local i
  for ((i=0; i<count; i++)); do
    local id="${prefix}${i}"
    if [[ -z "${out}" ]]; then
      out="${id}"
    else
      out="${out},${id}"
    fi
  done
  echo "${out}"
}

create_real_disk_dirs() {
  local base="$1"
  local count="$2"
  local i
  for ((i=0; i<count; i++)); do
    mkdir -p "${base}/disk${i}"
  done
}

wait_port() {
  local host="$1"
  local port="$2"
  local timeout_s="${3:-20}"
  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= timeout_s )); then
      return 1
    fi
    sleep 0.2
  done
}

REAL_DATA="${DATA_DIR}/real_node_01"
OPTICAL_ARCHIVE_ROOT="${DATA_DIR}/optical_archive"
OPTICAL_CACHE_ROOT="${DATA_DIR}/optical_cache"
MDS_DB_PATH="${DATA_DIR}/mds_rocks"
mkdir -p "${OPTICAL_ARCHIVE_ROOT}"
mkdir -p "${OPTICAL_CACHE_ROOT}"
create_real_disk_dirs "${REAL_DATA}" "${REAL_DISK_COUNT}"

REAL_DISKS_CSV="$(make_csv_ids "disk" "${REAL_DISK_COUNT}")"
OPTICAL_DISKS_CSV="$(make_csv_ids "odisk" "${OPTICAL_DISK_COUNT}")"

cat > "${CONF_DIR}/scheduler.conf" <<EOF
SUSPECT_TIMEOUT_MS=6000
DEAD_TIMEOUT_MS=15000
TICK_INTERVAL_MS=1000
EOF

cat > "${CONF_DIR}/real_node_01.conf" <<EOF
DISK_BASE_DIR=${REAL_DATA}
DISK_COUNT=${REAL_DISK_COUNT}
DISK_CAPACITY_BYTES=${REAL_DISK_CAPACITY_BYTES}
NODE_ID=node-01
NODE_ADDRESS=127.0.0.1:${REAL_PORT}
GROUP_ID=rg-node-01
NODE_ROLE=PRIMARY
REPLICATION_ENABLED=false
NODE_WEIGHT=1
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
MDS_ADDR=127.0.0.1:${MDS_PORT}
HEARTBEAT_INTERVAL_MS=${HEARTBEAT_MS}
ARCHIVE_REPORT_INTERVAL_MS=3000
ARCHIVE_REPORT_TOPK=256
ARCHIVE_REPORT_MIN_AGE_MS=30000
ARCHIVE_TRACK_MAX_CHUNKS=500000
ARCHIVE_META_DIR=${REAL_DATA}/.archive_meta
ARCHIVE_META_SNAPSHOT_INTERVAL_OPS=20000
EOF

cat > "${CONF_DIR}/virtual_node.conf" <<EOF
NODE_ID=vpool
NODE_ADDRESS=127.0.0.1:${VIRTUAL_PORT}
GROUP_ID=vpool-rg
NODE_ROLE=PRIMARY
REPLICATION_ENABLED=false
NODE_WEIGHT=8
VIRTUAL_NODE_COUNT=${VIRTUAL_NODE_COUNT}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
MDS_ADDR=127.0.0.1:${MDS_PORT}
HEARTBEAT_INTERVAL_MS=${HEARTBEAT_MS}
ARCHIVE_REPORT_INTERVAL_MS=3000
ARCHIVE_REPORT_TOPK=256
ARCHIVE_REPORT_MIN_AGE_MS=30000
ARCHIVE_TRACK_MAX_CHUNKS=500000
ARCHIVE_META_DIR=${DATA_DIR}/virtual_node_meta
ARCHIVE_META_SNAPSHOT_INTERVAL_OPS=20000
ALLOW_DYNAMIC_DISKS=true
DISKS=${REAL_DISKS_CSV}
READ_MBPS=800
WRITE_MBPS=600
READ_BASE_LATENCY_MS=2
WRITE_BASE_LATENCY_MS=3
JITTER_MS=1
DISK_CAPACITY_BYTES=${VIRTUAL_DISK_CAPACITY_BYTES}
MOUNT_POINT_PREFIX=/virtual
EOF

cat > "${CONF_DIR}/optical_node.conf" <<EOF
NODE_ID=optical-01
NODE_ADDRESS=127.0.0.1:${OPTICAL_PORT}
GROUP_ID=og-01
NODE_ROLE=PRIMARY
REPLICATION_ENABLED=false
NODE_WEIGHT=1
VIRTUAL_NODE_COUNT=${OPTICAL_NODE_COUNT}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
HEARTBEAT_INTERVAL_MS=${HEARTBEAT_MS}
DISKS=${OPTICAL_DISKS_CSV}
ARCHIVE_ROOT=${OPTICAL_ARCHIVE_ROOT}
CACHE_ROOT=${OPTICAL_CACHE_ROOT}
SIMULATE_IO=${OPTICAL_SIMULATE_IO}
OPTICAL_READ_BYTES_PER_SEC=${OPTICAL_READ_BYTES_PER_SEC}
OPTICAL_WRITE_BYTES_PER_SEC=${OPTICAL_WRITE_BYTES_PER_SEC}
CACHE_READ_BYTES_PER_SEC=${CACHE_READ_BYTES_PER_SEC}
CACHE_DISC_SLOTS=${OPTICAL_CACHE_DISC_SLOTS}
MAX_IMAGE_SIZE_BYTES=${OPTICAL_MAX_IMAGE_SIZE_BYTES}
DISK_CAPACITY_BYTES=${OPTICAL_DISK_CAPACITY_BYTES}
MOUNT_POINT_PREFIX=/optical
STARTUP_SCAN_MODE=${OPTICAL_STARTUP_SCAN_MODE}
EOF

cat > "${CONF_DIR}/mds.conf" <<EOF
MDS_DB_PATH=${MDS_DB_PATH}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
SCHEDULER_REFRESH_MS=2000
CHUNK_SIZE=4194304
REPLICA=1
ENABLE_OPTICAL_ARCHIVE=true
ARCHIVE_TRIGGER_BYTES=1048576
ARCHIVE_TARGET_BYTES=524288
COLD_FILE_TTL_SEC=30
ARCHIVE_SCAN_INTERVAL_MS=2000
ARCHIVE_MAX_CHUNKS_PER_ROUND=256
ARCHIVE_CANDIDATE_QUEUE_SIZE=200000
ARCHIVE_LEASE_DEFAULT_MS=30000
ARCHIVE_LEASE_MIN_MS=1000
ARCHIVE_LEASE_MAX_MS=300000
ARCHIVE_DISC_SIZE_BYTES=1048576
ARCHIVE_STRICT_FULL_DISC=true
ARCHIVE_STAGING_DIR=${DATA_DIR}/mds_archive_staging
ARCHIVE_BATCH_MAX_AGE_MS=0
NODES=node-01@127.0.0.1:${REAL_PORT},type=REAL,weight=1;vpool@127.0.0.1:${VIRTUAL_PORT},type=VIRTUAL,weight=8,virtual_node_count=${VIRTUAL_NODE_COUNT};optical-01@127.0.0.1:${OPTICAL_PORT},type=OPTICAL,weight=1,virtual_node_count=${OPTICAL_NODE_COUNT}
DISKS=node-01:${REAL_DISKS_CSV};vpool:${REAL_DISKS_CSV};optical-01:${OPTICAL_DISKS_CSV}
EOF

PID_FILE="${RUN_DIR}/pids.txt"
touch "${PID_FILE}"

start_service() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  nohup "$@" >"${log_file}" 2>&1 &
  local pid=$!
  echo "${name}:${pid}" >> "${PID_FILE}"
  echo "started ${name} pid=${pid} log=${log_file}"
}

start_service scheduler_server "${BUILD_DIR}/scheduler_server" --config="${CONF_DIR}/scheduler.conf" --port="${SCHEDULER_PORT}"
wait_port 127.0.0.1 "${SCHEDULER_PORT}" 20 || { echo "scheduler not ready"; exit 1; }

start_service real_node_01 "${BUILD_DIR}/real_node_server" --config="${CONF_DIR}/real_node_01.conf" --port="${REAL_PORT}"
wait_port 127.0.0.1 "${REAL_PORT}" 20 || { echo "real_node_01 not ready"; exit 1; }

start_service virtual_node "${BUILD_DIR}/virtual_node_server" --config="${CONF_DIR}/virtual_node.conf" --port="${VIRTUAL_PORT}"
wait_port 127.0.0.1 "${VIRTUAL_PORT}" 20 || { echo "virtual_node not ready"; exit 1; }

start_service optical_node "${BUILD_DIR}/optical_node_server" --config="${CONF_DIR}/optical_node.conf" --port="${OPTICAL_PORT}"
wait_port 127.0.0.1 "${OPTICAL_PORT}" 20 || { echo "optical_node not ready"; exit 1; }

start_service mds_server "${BUILD_DIR}/mds_server" --config="${CONF_DIR}/mds.conf" --port="${MDS_PORT}"
wait_port 127.0.0.1 "${MDS_PORT}" 20 || { echo "mds_server not ready"; exit 1; }

cat > "${RUN_DIR}/cluster_env.sh" <<EOF
#!/usr/bin/env bash
export ZB_RUN_DIR="${RUN_DIR}"
export ZB_CONF_DIR="${CONF_DIR}"
export ZB_LOG_DIR="${LOG_DIR}"
export ZB_SCHEDULER_ADDR="127.0.0.1:${SCHEDULER_PORT}"
export ZB_MDS_ADDR="127.0.0.1:${MDS_PORT}"
export ZB_REAL_SERVERS="127.0.0.1:${REAL_PORT}"
export ZB_REAL_DISKS="${REAL_DISKS_CSV}"
export ZB_REAL_CONFIGS="${CONF_DIR}/real_node_01.conf"
export ZB_VIRTUAL_SERVER="127.0.0.1:${VIRTUAL_PORT}"
export ZB_OPTICAL_SERVER="127.0.0.1:${OPTICAL_PORT}"
export ZB_REAL_DISK_COUNT="${REAL_DISK_COUNT}"
export ZB_VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT}"
export ZB_OPTICAL_NODE_COUNT="${OPTICAL_NODE_COUNT}"
export ZB_OPTICAL_DISK_COUNT="${OPTICAL_DISK_COUNT}"
export ZB_OPTICAL_SIMULATE_IO="${OPTICAL_SIMULATE_IO}"
export ZB_OPTICAL_READ_BYTES_PER_SEC="${OPTICAL_READ_BYTES_PER_SEC}"
export ZB_OPTICAL_WRITE_BYTES_PER_SEC="${OPTICAL_WRITE_BYTES_PER_SEC}"
export ZB_CACHE_READ_BYTES_PER_SEC="${CACHE_READ_BYTES_PER_SEC}"
export ZB_OPTICAL_CACHE_DISC_SLOTS="${OPTICAL_CACHE_DISC_SLOTS}"
export ZB_OPTICAL_STARTUP_SCAN_MODE="${OPTICAL_STARTUP_SCAN_MODE}"
EOF
chmod +x "${RUN_DIR}/cluster_env.sh"

cat > "${RUN_DIR}/stop_cluster.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${THIS_DIR}/pids.txt"
if [[ ! -f "${PID_FILE}" ]]; then
  echo "no pid file: ${PID_FILE}"
  exit 0
fi
while IFS=: read -r name pid; do
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "stopping ${name} pid=${pid}"
    kill "${pid}" 2>/dev/null || true
  fi
done < "${PID_FILE}"
EOF
chmod +x "${RUN_DIR}/stop_cluster.sh"

echo
echo "Cluster started successfully."
echo "RUN_DIR=${RUN_DIR}"
echo "Load env: source ${RUN_DIR}/cluster_env.sh"
echo "Stop all:  ${RUN_DIR}/stop_cluster.sh"
