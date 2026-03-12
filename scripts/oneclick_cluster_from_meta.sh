#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/meta_out}"
RUN_DIR="${RUN_DIR:-${OUT_DIR}/run}"

MODE="${1:-start}"

SCHEDULER_PORT="${SCHEDULER_PORT:-9100}"
MDS_PORT="${MDS_PORT:-9000}"
REAL_PORT_BASE="${REAL_PORT_BASE:-19080}"
VIRTUAL_PORT_BASE="${VIRTUAL_PORT_BASE:-29080}"
OPTICAL_PORT_BASE="${OPTICAL_PORT_BASE:-39080}"

REAL_NODE_COUNT="${REAL_NODE_COUNT:-1}"
VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT:-1}"
OPTICAL_NODE_COUNT="${OPTICAL_NODE_COUNT:-1}"

REAL_DISKS_PER_NODE="${REAL_DISKS_PER_NODE:-24}"
VIRTUAL_DISKS_PER_NODE="${VIRTUAL_DISKS_PER_NODE:-24}"
OPTICAL_DISCS_PER_NODE="${OPTICAL_DISCS_PER_NODE:-256}"

REAL_DISK_CAPACITY_BYTES="${REAL_DISK_CAPACITY_BYTES:-2199023255552}"     # 2TB
VIRTUAL_DISK_CAPACITY_BYTES="${VIRTUAL_DISK_CAPACITY_BYTES:-2199023255552}" # 2TB
OPTICAL_DISK_CAPACITY_BYTES="${OPTICAL_DISK_CAPACITY_BYTES:-1099511627776}" # 1TB

VIRTUAL_NODE_WEIGHT="${VIRTUAL_NODE_WEIGHT:-8}"
VIRTUAL_NODE_SIM_COUNT="${VIRTUAL_NODE_SIM_COUNT:-100}"
OPTICAL_NODE_WEIGHT="${OPTICAL_NODE_WEIGHT:-1}"
OPTICAL_NODE_SIM_COUNT="${OPTICAL_NODE_SIM_COUNT:-10000}"

ENABLE_OPTICAL_NODE="${ENABLE_OPTICAL_NODE:-1}"
ENABLE_FUSE="${ENABLE_FUSE:-0}"
FUSE_MOUNT_POINT="${FUSE_MOUNT_POINT:-${RUN_DIR}/mnt/zbfs}"

INGEST_MDS_SST="${INGEST_MDS_SST:-1}"
CLEAR_MDS_DB_BEFORE_INGEST="${CLEAR_MDS_DB_BEFORE_INGEST:-0}"
MDS_DB_PATH="${MDS_DB_PATH:-${RUN_DIR}/mds/db}"
MDS_OBJECT_UNIT_SIZE="${MDS_OBJECT_UNIT_SIZE:-4194304}"
MDS_REPLICA="${MDS_REPLICA:-1}"
MDS_ENABLE_OPTICAL_ARCHIVE="${MDS_ENABLE_OPTICAL_ARCHIVE:-0}"

LOG_DIR="${RUN_DIR}/logs"
PID_DIR="${RUN_DIR}/pids"
CFG_DIR="${RUN_DIR}/config"

mkdir -p "${LOG_DIR}" "${PID_DIR}" "${CFG_DIR}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

require_bin() {
  local p="$1"
  if [[ ! -x "${p}" ]]; then
    log "[ERROR] missing executable: ${p}"
    exit 1
  fi
}

join_disk_ids() {
  local prefix="$1"
  local count="$2"
  local out=""
  local i
  for ((i=0; i<count; ++i)); do
    if [[ -n "${out}" ]]; then
      out+=","
    fi
    out+="${prefix}${i}"
  done
  echo "${out}"
}

wait_port() {
  local host="$1"
  local port="$2"
  local timeout_sec="${3:-20}"
  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if command -v nc >/dev/null 2>&1; then
      if nc -z "${host}" "${port}" >/dev/null 2>&1; then
        return 0
      fi
    else
      if (exec 3<>"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
        exec 3<&-
        exec 3>&-
        return 0
      fi
    fi
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      return 1
    fi
    sleep 1
  done
}

write_pid() {
  local name="$1"
  local pid="$2"
  echo "${pid}" > "${PID_DIR}/${name}.pid"
}

read_pid() {
  local name="$1"
  local f="${PID_DIR}/${name}.pid"
  if [[ -f "${f}" ]]; then
    cat "${f}"
  fi
}

is_pid_alive() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

stop_by_name() {
  local name="$1"
  local pid
  pid="$(read_pid "${name}" || true)"
  if [[ -z "${pid}" ]]; then
    return 0
  fi
  if is_pid_alive "${pid}"; then
    kill "${pid}" >/dev/null 2>&1 || true
    sleep 1
    if is_pid_alive "${pid}"; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
    log "[STOP] ${name} pid=${pid}"
  fi
  rm -f "${PID_DIR}/${name}.pid"
}

render_scheduler_config() {
  cat > "${CFG_DIR}/scheduler.conf" <<EOF
SUSPECT_TIMEOUT_MS=6000
DEAD_TIMEOUT_MS=15000
TICK_INTERVAL_MS=1000
EOF
}

render_real_config() {
  local idx="$1"
  local node_id="real-${idx}"
  local port=$((REAL_PORT_BASE + idx - 1))
  local disk_base_dir="${RUN_DIR}/real_nodes/${node_id}/disks"
  local archive_meta_dir="${RUN_DIR}/real_nodes/${node_id}/archive_meta"
  mkdir -p "${disk_base_dir}" "${archive_meta_dir}"
  cat > "${CFG_DIR}/${node_id}.conf" <<EOF
DISK_BASE_DIR=${disk_base_dir}
DISK_COUNT=${REAL_DISKS_PER_NODE}
DISK_CAPACITY_BYTES=${REAL_DISK_CAPACITY_BYTES}
NODE_ID=${node_id}
NODE_ADDRESS=127.0.0.1:${port}
GROUP_ID=${node_id}
NODE_ROLE=PRIMARY
REPLICATION_ENABLED=false
NODE_WEIGHT=1
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
MDS_ADDR=127.0.0.1:${MDS_PORT}
HEARTBEAT_INTERVAL_MS=2000
ARCHIVE_META_DIR=${archive_meta_dir}
ARCHIVE_META_SNAPSHOT_INTERVAL_OPS=20000
ARCHIVE_META_WAL_FSYNC=false
EOF

  local src_meta="${OUT_DIR}/disk_meta/real_nodes/${node_id}/file_meta.tsv"
  local dst_meta="${archive_meta_dir}/file_meta/file_meta.tsv"
  if [[ -f "${src_meta}" ]]; then
    mkdir -p "$(dirname "${dst_meta}")"
    cp "${src_meta}" "${dst_meta}"
    log "[SEED] copied ${src_meta} -> ${dst_meta}"
  fi
}

render_virtual_config() {
  local idx="$1"
  local node_id="virtual-${idx}"
  local port=$((VIRTUAL_PORT_BASE + idx - 1))
  local archive_meta_dir="${RUN_DIR}/virtual_nodes/${node_id}/archive_meta"
  local disks
  disks="$(join_disk_ids "disk" "${VIRTUAL_DISKS_PER_NODE}")"
  mkdir -p "${archive_meta_dir}"
  cat > "${CFG_DIR}/${node_id}.conf" <<EOF
NODE_ID=${node_id}
NODE_ADDRESS=127.0.0.1:${port}
GROUP_ID=${node_id}
NODE_ROLE=PRIMARY
REPLICATION_ENABLED=false
NODE_WEIGHT=${VIRTUAL_NODE_WEIGHT}
VIRTUAL_NODE_COUNT=${VIRTUAL_NODE_SIM_COUNT}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
MDS_ADDR=127.0.0.1:${MDS_PORT}
HEARTBEAT_INTERVAL_MS=2000
ARCHIVE_META_DIR=${archive_meta_dir}
ARCHIVE_META_SNAPSHOT_INTERVAL_OPS=20000
ARCHIVE_META_WAL_FSYNC=false
ALLOW_DYNAMIC_DISKS=true
DISKS=${disks}
READ_MBPS=800
WRITE_MBPS=600
READ_BASE_LATENCY_MS=2
WRITE_BASE_LATENCY_MS=3
JITTER_MS=1
DISK_CAPACITY_BYTES=${VIRTUAL_DISK_CAPACITY_BYTES}
MOUNT_POINT_PREFIX=${RUN_DIR}/virtual_nodes/${node_id}/mount
EOF

  local src_meta="${OUT_DIR}/disk_meta/virtual_nodes/${node_id}/file_meta.tsv"
  local dst_meta="${archive_meta_dir}/file_meta/file_meta.tsv"
  if [[ -f "${src_meta}" ]]; then
    mkdir -p "$(dirname "${dst_meta}")"
    cp "${src_meta}" "${dst_meta}"
    log "[SEED] copied ${src_meta} -> ${dst_meta}"
  fi
}

render_optical_config() {
  local idx="$1"
  local node_id="optical-${idx}"
  local port=$((OPTICAL_PORT_BASE + idx - 1))
  local archive_root="${RUN_DIR}/optical_nodes/${node_id}/archive"
  local cache_root="${RUN_DIR}/optical_nodes/${node_id}/cache"
  local disks
  disks="$(join_disk_ids "odisk" "${OPTICAL_DISCS_PER_NODE}")"
  mkdir -p "${archive_root}" "${cache_root}"
  cat > "${CFG_DIR}/${node_id}.conf" <<EOF
NODE_ID=${node_id}
NODE_ADDRESS=127.0.0.1:${port}
GROUP_ID=${node_id}
NODE_ROLE=PRIMARY
REPLICATION_ENABLED=false
NODE_WEIGHT=${OPTICAL_NODE_WEIGHT}
VIRTUAL_NODE_COUNT=${OPTICAL_NODE_SIM_COUNT}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
HEARTBEAT_INTERVAL_MS=2000
DISKS=${disks}
ARCHIVE_ROOT=${archive_root}
CACHE_ROOT=${cache_root}
SIMULATE_IO=true
OPTICAL_READ_BYTES_PER_SEC=104857600
OPTICAL_WRITE_BYTES_PER_SEC=52428800
CACHE_READ_BYTES_PER_SEC=419430400
CACHE_DISC_SLOTS=4
MAX_IMAGE_SIZE_BYTES=1099511627776
DISK_CAPACITY_BYTES=${OPTICAL_DISK_CAPACITY_BYTES}
MOUNT_POINT_PREFIX=${RUN_DIR}/optical_nodes/${node_id}/mount
STARTUP_SCAN_MODE=fast
EOF
}

render_mds_config() {
  local nodes=""
  local disks=""
  local i

  for ((i=1; i<=REAL_NODE_COUNT; ++i)); do
    local node_id="real-${i}"
    local port=$((REAL_PORT_BASE + i - 1))
    local d
    d="$(join_disk_ids "disk" "${REAL_DISKS_PER_NODE}")"
    nodes+="${node_id}@127.0.0.1:${port},type=REAL,weight=1;"
    disks+="${node_id}:${d};"
  done
  for ((i=1; i<=VIRTUAL_NODE_COUNT; ++i)); do
    local node_id="virtual-${i}"
    local port=$((VIRTUAL_PORT_BASE + i - 1))
    local d
    d="$(join_disk_ids "disk" "${VIRTUAL_DISKS_PER_NODE}")"
    nodes+="${node_id}@127.0.0.1:${port},type=VIRTUAL,weight=${VIRTUAL_NODE_WEIGHT},virtual_node_count=${VIRTUAL_NODE_SIM_COUNT};"
    disks+="${node_id}:${d};"
  done
  if [[ "${ENABLE_OPTICAL_NODE}" == "1" ]]; then
    for ((i=1; i<=OPTICAL_NODE_COUNT; ++i)); do
      local node_id="optical-${i}"
      local port=$((OPTICAL_PORT_BASE + i - 1))
      local d
      d="$(join_disk_ids "odisk" "${OPTICAL_DISCS_PER_NODE}")"
      nodes+="${node_id}@127.0.0.1:${port},type=OPTICAL,weight=${OPTICAL_NODE_WEIGHT},virtual_node_count=${OPTICAL_NODE_SIM_COUNT};"
      disks+="${node_id}:${d};"
    done
  fi
  nodes="${nodes%;}"
  disks="${disks%;}"

  cat > "${CFG_DIR}/mds.conf" <<EOF
MDS_DB_PATH=${MDS_DB_PATH}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
SCHEDULER_REFRESH_MS=2000
OBJECT_UNIT_SIZE=${MDS_OBJECT_UNIT_SIZE}
REPLICA=${MDS_REPLICA}
ENABLE_OPTICAL_ARCHIVE=$( [[ "${MDS_ENABLE_OPTICAL_ARCHIVE}" == "1" ]] && echo "true" || echo "false" )
ARCHIVE_TRIGGER_BYTES=10737418240
ARCHIVE_TARGET_BYTES=8589934592
COLD_FILE_TTL_SEC=3600
ARCHIVE_SCAN_INTERVAL_MS=5000
ARCHIVE_MAX_OBJECTS_PER_ROUND=64
NODES=${nodes}
DISKS=${disks}
EOF
}

ingest_mds_sst_if_needed() {
  if [[ "${INGEST_MDS_SST}" != "1" ]]; then
    log "[INFO] skip MDS SST ingest (INGEST_MDS_SST=${INGEST_MDS_SST})"
    return 0
  fi
  local manifest="${OUT_DIR}/mds_sst/manifest.txt"
  if [[ ! -f "${manifest}" ]]; then
    log "[ERROR] missing manifest: ${manifest}"
    exit 1
  fi
  mkdir -p "$(dirname "${MDS_DB_PATH}")"
  if [[ "${CLEAR_MDS_DB_BEFORE_INGEST}" == "1" ]]; then
    rm -rf "${MDS_DB_PATH}"
  fi
  if [[ -d "${MDS_DB_PATH}" ]] && [[ -n "$(find "${MDS_DB_PATH}" -mindepth 1 -maxdepth 1 2>/dev/null || true)" ]]; then
    log "[INFO] MDS DB already exists and non-empty, skip ingest: ${MDS_DB_PATH}"
    return 0
  fi
  local log_file="${LOG_DIR}/mds_sst_ingest.log"
  log "[STEP] ingest mds sst -> ${MDS_DB_PATH}"
  "${BUILD_DIR}/mds_sst_ingest_tool" --db_path="${MDS_DB_PATH}" --manifest="${manifest}" >"${log_file}" 2>&1
  log "[OK] ingest completed, log=${log_file}"
}

start_scheduler() {
  local log_file="${LOG_DIR}/scheduler.log"
  "${BUILD_DIR}/scheduler_server" --config="${CFG_DIR}/scheduler.conf" --port="${SCHEDULER_PORT}" >"${log_file}" 2>&1 &
  write_pid "scheduler" "$!"
  wait_port "127.0.0.1" "${SCHEDULER_PORT}" 20 || {
    log "[ERROR] scheduler port not ready: ${SCHEDULER_PORT}"
    exit 1
  }
  log "[OK] scheduler started"
}

start_real_nodes() {
  local i
  for ((i=1; i<=REAL_NODE_COUNT; ++i)); do
    local node_id="real-${i}"
    local port=$((REAL_PORT_BASE + i - 1))
    local log_file="${LOG_DIR}/${node_id}.log"
    "${BUILD_DIR}/real_node_server" --config="${CFG_DIR}/${node_id}.conf" --port="${port}" >"${log_file}" 2>&1 &
    write_pid "${node_id}" "$!"
    wait_port "127.0.0.1" "${port}" 20 || {
      log "[ERROR] ${node_id} port not ready: ${port}"
      exit 1
    }
    log "[OK] ${node_id} started"
  done
}

start_virtual_nodes() {
  local i
  for ((i=1; i<=VIRTUAL_NODE_COUNT; ++i)); do
    local node_id="virtual-${i}"
    local port=$((VIRTUAL_PORT_BASE + i - 1))
    local log_file="${LOG_DIR}/${node_id}.log"
    "${BUILD_DIR}/virtual_node_server" --config="${CFG_DIR}/${node_id}.conf" --port="${port}" >"${log_file}" 2>&1 &
    write_pid "${node_id}" "$!"
    wait_port "127.0.0.1" "${port}" 20 || {
      log "[ERROR] ${node_id} port not ready: ${port}"
      exit 1
    }
    log "[OK] ${node_id} started"
  done
}

start_optical_nodes() {
  if [[ "${ENABLE_OPTICAL_NODE}" != "1" ]]; then
    return 0
  fi
  local i
  for ((i=1; i<=OPTICAL_NODE_COUNT; ++i)); do
    local node_id="optical-${i}"
    local port=$((OPTICAL_PORT_BASE + i - 1))
    local log_file="${LOG_DIR}/${node_id}.log"
    "${BUILD_DIR}/optical_node_server" --config="${CFG_DIR}/${node_id}.conf" --port="${port}" >"${log_file}" 2>&1 &
    write_pid "${node_id}" "$!"
    wait_port "127.0.0.1" "${port}" 20 || {
      log "[ERROR] ${node_id} port not ready: ${port}"
      exit 1
    }
    log "[OK] ${node_id} started"
  done
}

start_mds() {
  local log_file="${LOG_DIR}/mds.log"
  "${BUILD_DIR}/mds_server" --config="${CFG_DIR}/mds.conf" --port="${MDS_PORT}" >"${log_file}" 2>&1 &
  write_pid "mds" "$!"
  wait_port "127.0.0.1" "${MDS_PORT}" 20 || {
    log "[ERROR] mds port not ready: ${MDS_PORT}"
    exit 1
  }
  log "[OK] mds started"
}

start_fuse_if_enabled() {
  if [[ "${ENABLE_FUSE}" != "1" ]]; then
    return 0
  fi
  if [[ ! -x "${BUILD_DIR}/zb_fuse_client" ]]; then
    log "[WARN] zb_fuse_client not found, skip fuse mount"
    return 0
  fi
  mkdir -p "${FUSE_MOUNT_POINT}"
  local log_file="${LOG_DIR}/fuse.log"
  "${BUILD_DIR}/zb_fuse_client" \
    --mds="127.0.0.1:${MDS_PORT}" \
    --scheduler="127.0.0.1:${SCHEDULER_PORT}" \
    -- "${FUSE_MOUNT_POINT}" -f >"${log_file}" 2>&1 &
  write_pid "fuse" "$!"
  sleep 2
  log "[OK] fuse started (mount=${FUSE_MOUNT_POINT})"
}

start_all() {
  require_bin "${BUILD_DIR}/scheduler_server"
  require_bin "${BUILD_DIR}/real_node_server"
  require_bin "${BUILD_DIR}/virtual_node_server"
  require_bin "${BUILD_DIR}/mds_server"
  require_bin "${BUILD_DIR}/mds_sst_ingest_tool"
  if [[ "${ENABLE_OPTICAL_NODE}" == "1" ]]; then
    require_bin "${BUILD_DIR}/optical_node_server"
  fi

  render_scheduler_config
  local i
  for ((i=1; i<=REAL_NODE_COUNT; ++i)); do
    render_real_config "${i}"
  done
  for ((i=1; i<=VIRTUAL_NODE_COUNT; ++i)); do
    render_virtual_config "${i}"
  done
  if [[ "${ENABLE_OPTICAL_NODE}" == "1" ]]; then
    for ((i=1; i<=OPTICAL_NODE_COUNT; ++i)); do
      render_optical_config "${i}"
    done
  fi
  render_mds_config
  ingest_mds_sst_if_needed

  start_scheduler
  start_real_nodes
  start_virtual_nodes
  start_optical_nodes
  start_mds
  start_fuse_if_enabled

  log "=== cluster started ==="
  log "OUT_DIR=${OUT_DIR}"
  log "RUN_DIR=${RUN_DIR}"
  log "logs=${LOG_DIR}"
  log "pids=${PID_DIR}"
}

stop_all() {
  # Prefer fixed stop order first.
  stop_by_name "fuse"
  stop_by_name "mds"
  stop_by_name "scheduler"
  # Then clean up any remaining pids from previous runs.
  if [[ -d "${PID_DIR}" ]]; then
    local pid_file
    for pid_file in "${PID_DIR}"/*.pid; do
      [[ -e "${pid_file}" ]] || break
      stop_by_name "$(basename "${pid_file}" .pid)"
    done
  fi
  log "=== cluster stopped ==="
}

status_all() {
  local names=("scheduler" "mds")
  local i
  for ((i=1; i<=REAL_NODE_COUNT; ++i)); do
    names+=("real-${i}")
  done
  for ((i=1; i<=VIRTUAL_NODE_COUNT; ++i)); do
    names+=("virtual-${i}")
  done
  if [[ "${ENABLE_OPTICAL_NODE}" == "1" ]]; then
    for ((i=1; i<=OPTICAL_NODE_COUNT; ++i)); do
      names+=("optical-${i}")
    done
  fi
  if [[ "${ENABLE_FUSE}" == "1" ]]; then
    names+=("fuse")
  fi
  local name pid
  for name in "${names[@]}"; do
    pid="$(read_pid "${name}" || true)"
    if [[ -n "${pid}" ]] && is_pid_alive "${pid}"; then
      echo "${name}: RUNNING pid=${pid}"
    else
      echo "${name}: STOPPED"
    fi
  done
}

case "${MODE}" in
  start)
    start_all
    ;;
  stop)
    stop_all
    ;;
  restart)
    stop_all
    start_all
    ;;
  status)
    status_all
    ;;
  *)
    echo "Usage: $0 [start|stop|restart|status]"
    exit 1
    ;;
esac
