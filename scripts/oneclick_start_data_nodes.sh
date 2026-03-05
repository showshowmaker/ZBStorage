#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
RUN_DIR="${RUN_DIR:-${ROOT_DIR}/runtime/oneclick}"
CONF_DIR="${RUN_DIR}/config"
LOG_DIR="${RUN_DIR}/logs"
DATA_DIR="${RUN_DIR}/data"
ENV_FILE="${RUN_DIR}/cluster_env.sh"
PID_FILE="${RUN_DIR}/pids_data_nodes.txt"

SCHEDULER_PORT="${SCHEDULER_PORT:-9100}"
MDS_PORT="${MDS_PORT:-9000}"
REAL_PORT="${REAL_PORT:-19080}"
VIRTUAL_PORT="${VIRTUAL_PORT:-29080}"

REAL_DISK_COUNT="${REAL_DISK_COUNT:-24}"
REAL_DISK_CAPACITY_BYTES="${REAL_DISK_CAPACITY_BYTES:-2199023255552}"      # 2TB
VIRTUAL_DISK_CAPACITY_BYTES="${VIRTUAL_DISK_CAPACITY_BYTES:-2199023255552}"  # 2TB
VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT:-100}"
HEARTBEAT_MS="${HEARTBEAT_MS:-2000}"

required_bins=(real_node_server virtual_node_server)
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

REAL_DATA="${DATA_DIR}/real_node_01"
mkdir -p "${REAL_DATA}"
for ((i=0; i<REAL_DISK_COUNT; i++)); do
  mkdir -p "${REAL_DATA}/disk${i}"
done

REAL_DISKS_CSV="$(make_csv_ids "disk" "${REAL_DISK_COUNT}")"

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
DISKS=${REAL_DISKS_CSV}
READ_MBPS=800
WRITE_MBPS=600
READ_BASE_LATENCY_MS=2
WRITE_BASE_LATENCY_MS=3
JITTER_MS=1
DISK_CAPACITY_BYTES=${VIRTUAL_DISK_CAPACITY_BYTES}
MOUNT_POINT_PREFIX=/virtual
EOF

stop_previous
touch "${PID_FILE}"

start_service real_node_01 "${BUILD_DIR}/real_node_server" --config="${CONF_DIR}/real_node_01.conf" --port="${REAL_PORT}"
wait_port 127.0.0.1 "${REAL_PORT}" 20 || { echo "real_node_01 not ready"; exit 1; }

start_service virtual_node "${BUILD_DIR}/virtual_node_server" --config="${CONF_DIR}/virtual_node.conf" --port="${VIRTUAL_PORT}"
wait_port 127.0.0.1 "${VIRTUAL_PORT}" 20 || { echo "virtual_node not ready"; exit 1; }

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
export ZB_REAL_DISK_COUNT="${REAL_DISK_COUNT}"
export ZB_VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT}"
EOF
chmod +x "${ENV_FILE}"

echo
echo "Data nodes started."
echo "RUN_DIR=${RUN_DIR}"
echo "PIDs: ${PID_FILE}"
echo "Env:  ${ENV_FILE}"
