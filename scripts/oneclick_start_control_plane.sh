#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
RUN_DIR="${RUN_DIR:-${ROOT_DIR}/runtime/oneclick}"
CONF_DIR="${RUN_DIR}/config"
LOG_DIR="${RUN_DIR}/logs"
DATA_DIR="${RUN_DIR}/data"
ENV_FILE="${RUN_DIR}/cluster_env.sh"
PID_FILE="${RUN_DIR}/pids_control_plane.txt"

SCHEDULER_PORT="${SCHEDULER_PORT:-9100}"
MDS_PORT="${MDS_PORT:-9000}"
REAL_PORT="${REAL_PORT:-19080}"
VIRTUAL_PORT="${VIRTUAL_PORT:-29080}"
OPTICAL_PORT="${OPTICAL_PORT:-39080}"

REAL_DISK_COUNT="${REAL_DISK_COUNT:-24}"
VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT:-100}"
OPTICAL_NODE_COUNT="${OPTICAL_NODE_COUNT:-128}"
OPTICAL_DISK_COUNT="${OPTICAL_DISK_COUNT:-128}"
HEARTBEAT_MS="${HEARTBEAT_MS:-2000}"
ENABLE_OPTICAL_ARCHIVE="${ENABLE_OPTICAL_ARCHIVE:-false}"

ARCHIVE_TRIGGER_BYTES="${ARCHIVE_TRIGGER_BYTES:-10737418240}"
ARCHIVE_TARGET_BYTES="${ARCHIVE_TARGET_BYTES:-8589934592}"
COLD_FILE_TTL_SEC="${COLD_FILE_TTL_SEC:-3600}"
ARCHIVE_SCAN_INTERVAL_MS="${ARCHIVE_SCAN_INTERVAL_MS:-5000}"
ARCHIVE_MAX_CHUNKS_PER_ROUND="${ARCHIVE_MAX_CHUNKS_PER_ROUND:-64}"
ARCHIVE_DISC_SIZE_BYTES="${ARCHIVE_DISC_SIZE_BYTES:-1099511627776}"

required_bins=(scheduler_server mds_server)
for bin in "${required_bins[@]}"; do
  if [[ ! -x "${BUILD_DIR}/${bin}" ]]; then
    echo "Missing binary: ${BUILD_DIR}/${bin}" >&2
    echo "Build first: cmake -S . -B build && cmake --build build -j" >&2
    exit 1
  fi
done

mkdir -p "${CONF_DIR}" "${LOG_DIR}" "${DATA_DIR}"

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

stop_previous() {
  if [[ ! -f "${PID_FILE}" ]]; then
    return
  fi
  while IFS=: read -r name pid; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      echo "stopping previous ${name} pid=${pid}"
      kill "${pid}" 2>/dev/null || true
    fi
  done < "${PID_FILE}"
  rm -f "${PID_FILE}"
}

start_service() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  nohup "$@" >"${log_file}" 2>&1 &
  local pid=$!
  echo "${name}:${pid}" >> "${PID_FILE}"
  echo "started ${name} pid=${pid} log=${log_file}"
}

REAL_DISKS_CSV="$(make_csv_ids "disk" "${REAL_DISK_COUNT}")"
OPTICAL_DISKS_CSV="$(make_csv_ids "odisk" "${OPTICAL_DISK_COUNT}")"
MDS_DB_PATH="${DATA_DIR}/mds_rocks"
mkdir -p "${MDS_DB_PATH}"

MDS_NODES="node-01@127.0.0.1:${REAL_PORT},type=REAL,weight=1;vpool@127.0.0.1:${VIRTUAL_PORT},type=VIRTUAL,weight=8,virtual_node_count=${VIRTUAL_NODE_COUNT}"
MDS_DISKS="node-01:${REAL_DISKS_CSV};vpool:${REAL_DISKS_CSV}"
if [[ "${ENABLE_OPTICAL_ARCHIVE}" == "true" ]]; then
  MDS_NODES="${MDS_NODES};optical-01@127.0.0.1:${OPTICAL_PORT},type=OPTICAL,weight=1,virtual_node_count=${OPTICAL_NODE_COUNT}"
  MDS_DISKS="${MDS_DISKS};optical-01:${OPTICAL_DISKS_CSV}"
fi

cat > "${CONF_DIR}/scheduler.conf" <<EOF
SUSPECT_TIMEOUT_MS=6000
DEAD_TIMEOUT_MS=15000
TICK_INTERVAL_MS=1000
EOF

cat > "${CONF_DIR}/mds.conf" <<EOF
MDS_DB_PATH=${MDS_DB_PATH}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
SCHEDULER_REFRESH_MS=2000
CHUNK_SIZE=4194304
REPLICA=1
ENABLE_OPTICAL_ARCHIVE=${ENABLE_OPTICAL_ARCHIVE}
ARCHIVE_TRIGGER_BYTES=${ARCHIVE_TRIGGER_BYTES}
ARCHIVE_TARGET_BYTES=${ARCHIVE_TARGET_BYTES}
COLD_FILE_TTL_SEC=${COLD_FILE_TTL_SEC}
ARCHIVE_SCAN_INTERVAL_MS=${ARCHIVE_SCAN_INTERVAL_MS}
ARCHIVE_MAX_CHUNKS_PER_ROUND=${ARCHIVE_MAX_CHUNKS_PER_ROUND}
ARCHIVE_CANDIDATE_QUEUE_SIZE=200000
ARCHIVE_LEASE_DEFAULT_MS=30000
ARCHIVE_LEASE_MIN_MS=1000
ARCHIVE_LEASE_MAX_MS=300000
ARCHIVE_DISC_SIZE_BYTES=${ARCHIVE_DISC_SIZE_BYTES}
ARCHIVE_STRICT_FULL_DISC=true
ARCHIVE_STAGING_DIR=${DATA_DIR}/mds_archive_staging
ARCHIVE_BATCH_MAX_AGE_MS=0
NODES=${MDS_NODES}
DISKS=${MDS_DISKS}
EOF

stop_previous
touch "${PID_FILE}"

start_service scheduler_server "${BUILD_DIR}/scheduler_server" --config="${CONF_DIR}/scheduler.conf" --port="${SCHEDULER_PORT}"
wait_port 127.0.0.1 "${SCHEDULER_PORT}" 20 || { echo "scheduler not ready"; exit 1; }

start_service mds_server "${BUILD_DIR}/mds_server" --config="${CONF_DIR}/mds.conf" --port="${MDS_PORT}"
wait_port 127.0.0.1 "${MDS_PORT}" 20 || { echo "mds_server not ready"; exit 1; }

cat > "${ENV_FILE}" <<EOF
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
export ZB_ENABLE_OPTICAL_ARCHIVE="${ENABLE_OPTICAL_ARCHIVE}"
export ZB_HEARTBEAT_MS="${HEARTBEAT_MS}"
EOF
chmod +x "${ENV_FILE}"

echo
echo "Control plane started."
echo "RUN_DIR=${RUN_DIR}"
echo "PIDs: ${PID_FILE}"
echo "Env:  ${ENV_FILE}"
