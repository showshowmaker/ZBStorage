#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/meta_out}"
LOG_DIR="${LOG_DIR:-${OUT_DIR}/logs}"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/meta_gen_pipeline_${RUN_TS}.log"
ABORT_FLAG="${OUT_DIR}/.meta_gen_space_abort"

TOTAL_FILES="${TOTAL_FILES:-100000000000}"
NAMESPACE_COUNT="${NAMESPACE_COUNT:-1000}"
DISK_REPLICA_RATIO="${DISK_REPLICA_RATIO:-0.5}"

OPTICAL_NODE_COUNT="${OPTICAL_NODE_COUNT:-10000}"
DISCS_PER_OPTICAL_NODE="${DISCS_PER_OPTICAL_NODE:-10000}"
TARGET_OPTICAL_EB="${TARGET_OPTICAL_EB:-1000}"

VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT:-100}"
VIRTUAL_DISKS_PER_NODE="${VIRTUAL_DISKS_PER_NODE:-16}"
VIRTUAL_DISK_SIZE_TB="${VIRTUAL_DISK_SIZE_TB:-2}"
REAL_NODE_COUNT="${REAL_NODE_COUNT:-1}"
REAL_DISKS_PER_NODE="${REAL_DISKS_PER_NODE:-24}"
REAL_DISK_SIZE_TB="${REAL_DISK_SIZE_TB:-2}"

BRANCH_FACTOR="${BRANCH_FACTOR:-64}"
MAX_DEPTH="${MAX_DEPTH:-4}"
MAX_FILES_PER_LEAF="${MAX_FILES_PER_LEAF:-2048}"
SIZE_SEED="${SIZE_SEED:-1145141919810}"

SST_MAX_KV="${SST_MAX_KV:-1000000}"

MIN_FREE_SPACE_TB="${MIN_FREE_SPACE_TB:-10}"
SPACE_CHECK_INTERVAL_SEC="${SPACE_CHECK_INTERVAL_SEC:-30}"
ENABLE_SPACE_GUARD="${ENABLE_SPACE_GUARD:-1}"

# Rough size estimation knobs.
EST_MDS_BYTES_PER_FILE="${EST_MDS_BYTES_PER_FILE:-230}"
EST_OPTICAL_MANIFEST_BYTES_PER_FILE="${EST_OPTICAL_MANIFEST_BYTES_PER_FILE:-150}"
EST_DISK_META_LINE_BYTES="${EST_DISK_META_LINE_BYTES:-96}"
EST_AVG_FILE_SIZE_BYTES="${EST_AVG_FILE_SIZE_BYTES:-1000000000}"

META_PLAN_BIN="${BUILD_DIR}/meta_plan_tool"
MDS_SST_GEN_BIN="${BUILD_DIR}/mds_sst_gen_tool"
DISK_META_BIN="${BUILD_DIR}/disk_meta_gen_tool"
OPTICAL_META_BIN="${BUILD_DIR}/optical_meta_gen_tool"

mkdir -p "${OUT_DIR}" "${LOG_DIR}"
touch "${LOG_FILE}"

log() {
  local msg="$1"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${msg}" | tee -a "${LOG_FILE}"
}

human_bytes() {
  local bytes="$1"
  awk -v b="${bytes}" 'BEGIN{
    split("B KB MB GB TB PB EB", u, " ");
    i=1;
    while (b>=1000 && i<7) { b=b/1000; i++; }
    printf "%.2f %s", b, u[i];
  }'
}

require_bc() {
  if ! command -v bc >/dev/null 2>&1; then
    log "[ERROR] bc is required to compute statistics"
    exit 1
  fi
}

free_bytes_under_out_dir() {
  df -PB1 "${OUT_DIR}" | awk 'NR==2 {print $4}'
}

monitor_space_for_pid() {
  local target_pid="$1"
  local threshold_bytes="$2"
  local interval_sec="$3"
  while kill -0 "${target_pid}" >/dev/null 2>&1; do
    local free_bytes
    free_bytes="$(free_bytes_under_out_dir)"
    if [[ -n "${free_bytes}" && "${free_bytes}" =~ ^[0-9]+$ ]] && (( free_bytes < threshold_bytes )); then
      local msg="free space ${free_bytes} bytes < threshold ${threshold_bytes} bytes; aborting current step"
      echo "${msg}" > "${ABORT_FLAG}"
      log "[ABORT] ${msg}"
      kill -TERM "${target_pid}" >/dev/null 2>&1 || true
      sleep 2
      kill -KILL "${target_pid}" >/dev/null 2>&1 || true
      return
    fi
    sleep "${interval_sec}"
  done
}

run_step() {
  local step_name="$1"
  shift
  log "=== [${step_name}] ==="
  log "CMD: $*"
  rm -f "${ABORT_FLAG}"

  set +e
  "$@" 2>&1 | tee -a "${LOG_FILE}" &
  local cmd_pid=$!
  local mon_pid=""
  if [[ "${ENABLE_SPACE_GUARD}" == "1" ]]; then
    local threshold_bytes
    threshold_bytes="$(echo "${MIN_FREE_SPACE_TB}*1000000000000" | bc)"
    monitor_space_for_pid "${cmd_pid}" "${threshold_bytes}" "${SPACE_CHECK_INTERVAL_SEC}" &
    mon_pid=$!
  fi

  wait "${cmd_pid}"
  local rc=$?
  if [[ -n "${mon_pid}" ]]; then
    kill "${mon_pid}" >/dev/null 2>&1 || true
    wait "${mon_pid}" >/dev/null 2>&1 || true
  fi
  set -e

  if [[ -f "${ABORT_FLAG}" ]]; then
    log "[FAIL] ${step_name} aborted by free-space guard: $(cat "${ABORT_FLAG}")"
    exit 11
  fi
  if [[ ${rc} -ne 0 ]]; then
    log "[FAIL] ${step_name} exited with ${rc}"
    exit ${rc}
  fi
  log "[OK] ${step_name}"
}

estimate_meta_size() {
  local total_files="$1"
  local mds_bytes_per_file="$2"
  local optical_bytes_per_file="$3"
  local disk_line_bytes="$4"
  local avg_file_size_bytes="$5"
  local disk_total_bytes="$6"
  local disk_ratio="$7"
  local optical_disc_count="$8"

  local est_mds_bytes
  local est_optical_manifest_bytes
  local est_disk_used_bytes
  local est_disk_file_count
  local est_disk_meta_bytes
  local est_optical_catalog_bytes
  local est_total_bytes

  est_mds_bytes="$(echo "${total_files}*${mds_bytes_per_file}" | bc)"
  est_optical_manifest_bytes="$(echo "${total_files}*${optical_bytes_per_file}" | bc)"
  est_disk_used_bytes="$(echo "${disk_total_bytes}*${disk_ratio}" | bc)"
  est_disk_file_count="$(echo "${est_disk_used_bytes}/${avg_file_size_bytes}" | bc)"
  est_disk_meta_bytes="$(echo "${est_disk_file_count}*${disk_line_bytes}" | bc)"
  # approx 40 bytes per line: node_id\tdisc_id\tcapacity\tstate\n
  est_optical_catalog_bytes="$(echo "${optical_disc_count}*40" | bc)"
  est_total_bytes="$(echo "${est_mds_bytes}+${est_optical_manifest_bytes}+${est_disk_meta_bytes}+${est_optical_catalog_bytes}" | bc)"

  log "Estimated MDS SST logical size: $(human_bytes "${est_mds_bytes}") (${est_mds_bytes} bytes)"
  log "Estimated optical manifest size: $(human_bytes "${est_optical_manifest_bytes}") (${est_optical_manifest_bytes} bytes)"
  log "Estimated disk meta TSV size: $(human_bytes "${est_disk_meta_bytes}") (${est_disk_meta_bytes} bytes)"
  log "Estimated optical catalog size: $(human_bytes "${est_optical_catalog_bytes}") (${est_optical_catalog_bytes} bytes)"
  log "Estimated total generated metadata: $(human_bytes "${est_total_bytes}") (${est_total_bytes} bytes)"
  log "Note: RocksDB SST/LSM overhead may increase MDS actual disk usage to ~1.3x-2.0x."
}

for bin in "${META_PLAN_BIN}" "${MDS_SST_GEN_BIN}" "${DISK_META_BIN}" "${OPTICAL_META_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    log "[ERROR] binary not found or not executable: ${bin}"
    log "Build first: cmake --build ${BUILD_DIR} --target meta_plan_tool mds_sst_gen_tool disk_meta_gen_tool optical_meta_gen_tool -j"
    exit 1
  fi
done

mkdir -p "${OUT_DIR}/plan" "${OUT_DIR}/mds_sst" "${OUT_DIR}/disk_meta" "${OUT_DIR}/optical_meta"
require_bc

disk_total_bytes_est="$(echo "${REAL_NODE_COUNT}*${REAL_DISKS_PER_NODE}*${REAL_DISK_SIZE_TB}*1000000000000 + ${VIRTUAL_NODE_COUNT}*${VIRTUAL_DISKS_PER_NODE}*${VIRTUAL_DISK_SIZE_TB}*1000000000000" | bc)"
optical_disc_count_est="$((OPTICAL_NODE_COUNT * DISCS_PER_OPTICAL_NODE))"
log "Pipeline log file: ${LOG_FILE}"
log "OUT_DIR=${OUT_DIR}, BUILD_DIR=${BUILD_DIR}"
log "Free space guard: ENABLE=${ENABLE_SPACE_GUARD}, threshold=${MIN_FREE_SPACE_TB}TB, interval=${SPACE_CHECK_INTERVAL_SEC}s"
estimate_meta_size "${TOTAL_FILES}" \
                   "${EST_MDS_BYTES_PER_FILE}" \
                   "${EST_OPTICAL_MANIFEST_BYTES_PER_FILE}" \
                   "${EST_DISK_META_LINE_BYTES}" \
                   "${EST_AVG_FILE_SIZE_BYTES}" \
                   "${disk_total_bytes_est}" \
                   "${DISK_REPLICA_RATIO}" \
                   "${optical_disc_count_est}"

run_step "1/4 planning" "${META_PLAN_BIN}" \
  --output="${OUT_DIR}/plan/meta_plan_report.md" \
  --total_files="${TOTAL_FILES}" \
  --namespace_count="${NAMESPACE_COUNT}" \
  --disk_replica_ratio="${DISK_REPLICA_RATIO}" \
  --optical_node_count="${OPTICAL_NODE_COUNT}" \
  --discs_per_optical_node="${DISCS_PER_OPTICAL_NODE}" \
  --virtual_node_count="${VIRTUAL_NODE_COUNT}" \
  --virtual_disks_per_node="${VIRTUAL_DISKS_PER_NODE}" \
  --real_node_count="${REAL_NODE_COUNT}" \
  --real_disks_per_node="${REAL_DISKS_PER_NODE}" \
  --target_optical_eb="${TARGET_OPTICAL_EB}" \
  --branch_factor="${BRANCH_FACTOR}" \
  --max_depth="${MAX_DEPTH}" \
  --max_files_per_leaf="${MAX_FILES_PER_LEAF}" \
  --seed="${SIZE_SEED}"

run_step "2/4 mds_sst_generation" "${MDS_SST_GEN_BIN}" \
  --output_dir="${OUT_DIR}/mds_sst" \
  --total_files="${TOTAL_FILES}" \
  --namespace_count="${NAMESPACE_COUNT}" \
  --disk_replica_ratio="${DISK_REPLICA_RATIO}" \
  --optical_node_count="${OPTICAL_NODE_COUNT}" \
  --discs_per_optical_node="${DISCS_PER_OPTICAL_NODE}" \
  --virtual_node_count="${VIRTUAL_NODE_COUNT}" \
  --virtual_disks_per_node="${VIRTUAL_DISKS_PER_NODE}" \
  --real_node_count="${REAL_NODE_COUNT}" \
  --real_disks_per_node="${REAL_DISKS_PER_NODE}" \
  --branch_factor="${BRANCH_FACTOR}" \
  --max_depth="${MAX_DEPTH}" \
  --max_files_per_leaf="${MAX_FILES_PER_LEAF}" \
  --seed="${SIZE_SEED}" \
  --max_kv_per_sst="${SST_MAX_KV}"

run_step "3/4 disk_meta_generation" "${DISK_META_BIN}" \
  --output_dir="${OUT_DIR}/disk_meta" \
  --total_files="${TOTAL_FILES}" \
  --namespace_count="${NAMESPACE_COUNT}" \
  --disk_replica_ratio="${DISK_REPLICA_RATIO}" \
  --virtual_node_count="${VIRTUAL_NODE_COUNT}" \
  --virtual_disks_per_node="${VIRTUAL_DISKS_PER_NODE}" \
  --real_node_count="${REAL_NODE_COUNT}" \
  --real_disks_per_node="${REAL_DISKS_PER_NODE}" \
  --branch_factor="${BRANCH_FACTOR}" \
  --max_depth="${MAX_DEPTH}" \
  --max_files_per_leaf="${MAX_FILES_PER_LEAF}" \
  --seed="${SIZE_SEED}"

run_step "4/4 optical_meta_generation" "${OPTICAL_META_BIN}" \
  --output_dir="${OUT_DIR}/optical_meta" \
  --total_files="${TOTAL_FILES}" \
  --namespace_count="${NAMESPACE_COUNT}" \
  --optical_node_count="${OPTICAL_NODE_COUNT}" \
  --discs_per_optical_node="${DISCS_PER_OPTICAL_NODE}" \
  --target_optical_eb="${TARGET_OPTICAL_EB}" \
  --branch_factor="${BRANCH_FACTOR}" \
  --max_depth="${MAX_DEPTH}" \
  --max_files_per_leaf="${MAX_FILES_PER_LEAF}" \
  --seed="${SIZE_SEED}"

log "=== [stats] writing generation summary ==="

MDS_MANIFEST="${OUT_DIR}/mds_sst/manifest.txt"
DISK_MANIFEST="${OUT_DIR}/disk_meta/disk_meta_manifest.txt"
OPTICAL_MANIFEST="${OUT_DIR}/optical_meta/optical_meta_manifest.txt"
SUMMARY_CONF="${OUT_DIR}/generation_stats.conf"

read_manifest_kv() {
  local file="$1"
  local key="$2"
  local line
  line="$(grep -E "^${key}=" "${file}" | tail -n 1 || true)"
  if [[ -z "${line}" ]]; then
    echo ""
  else
    echo "${line#*=}"
  fi
}

total_files="$(read_manifest_kv "${MDS_MANIFEST}" "total_files")"
total_file_size_bytes="$(read_manifest_kv "${MDS_MANIFEST}" "sampled_total_bytes")"
avg_file_size_bytes="$(read_manifest_kv "${MDS_MANIFEST}" "avg_file_size_bytes")"
disk_used_space_bytes="$(read_manifest_kv "${DISK_MANIFEST}" "disk_used_bytes")"
count_100gb="$(read_manifest_kv "${OPTICAL_MANIFEST}" "count_100gb")"
count_1tb="$(read_manifest_kv "${OPTICAL_MANIFEST}" "count_1tb")"
count_10tb="$(read_manifest_kv "${OPTICAL_MANIFEST}" "count_10tb")"

if [[ -z "${total_files}" ]]; then
  total_files="${TOTAL_FILES}"
fi
if [[ -z "${total_file_size_bytes}" ]]; then
  total_file_size_bytes="0"
fi
if [[ -z "${avg_file_size_bytes}" ]]; then
  if [[ "${total_files}" == "0" ]]; then
    avg_file_size_bytes="0"
  else
    avg_file_size_bytes="$(echo "scale=0; ${total_file_size_bytes}/${total_files}" | bc)"
  fi
fi
if [[ -z "${disk_used_space_bytes}" ]]; then
  disk_used_space_bytes="0"
fi
if [[ -z "${count_100gb}" ]]; then
  count_100gb="0"
fi
if [[ -z "${count_1tb}" ]]; then
  count_1tb="0"
fi
if [[ -z "${count_10tb}" ]]; then
  count_10tb="0"
fi

disk_node_count=$((REAL_NODE_COUNT + VIRTUAL_NODE_COUNT))
disk_count=$((REAL_NODE_COUNT * REAL_DISKS_PER_NODE + VIRTUAL_NODE_COUNT * VIRTUAL_DISKS_PER_NODE))
optical_node_count="${OPTICAL_NODE_COUNT}"
optical_disc_count=$((OPTICAL_NODE_COUNT * DISCS_PER_OPTICAL_NODE))
optical_data_count="${optical_disc_count}"

total_disk_space_bytes="$(echo "${REAL_NODE_COUNT}*${REAL_DISKS_PER_NODE}*${REAL_DISK_SIZE_TB}*1000000000000 + ${VIRTUAL_NODE_COUNT}*${VIRTUAL_DISKS_PER_NODE}*${VIRTUAL_DISK_SIZE_TB}*1000000000000" | bc)"
total_optical_space_bytes="$(echo "${count_100gb}*100000000000 + ${count_1tb}*1000000000000 + ${count_10tb}*10000000000000" | bc)"
used_optical_space_bytes="${total_file_size_bytes}"

cat > "${SUMMARY_CONF}" <<EOF
total_files=${total_files}
avg_file_size_bytes=${avg_file_size_bytes}
total_file_size_bytes=${total_file_size_bytes}
disk_node_count=${disk_node_count}
disk_count=${disk_count}
total_disk_space_bytes=${total_disk_space_bytes}
used_disk_space_bytes=${disk_used_space_bytes}
optical_node_count=${optical_node_count}
optical_disc_count=${optical_disc_count}
optical_data_count=${optical_data_count}
total_optical_space_bytes=${total_optical_space_bytes}
used_optical_space_bytes=${used_optical_space_bytes}
EOF

actual_mds_size="$(du -sb "${OUT_DIR}/mds_sst" 2>/dev/null | awk '{print $1}')"
actual_disk_meta_size="$(du -sb "${OUT_DIR}/disk_meta" 2>/dev/null | awk '{print $1}')"
actual_optical_meta_size="$(du -sb "${OUT_DIR}/optical_meta" 2>/dev/null | awk '{print $1}')"
if [[ -n "${actual_mds_size}" ]]; then
  log "Actual mds_sst directory size: $(human_bytes "${actual_mds_size}") (${actual_mds_size} bytes)"
fi
if [[ -n "${actual_disk_meta_size}" ]]; then
  log "Actual disk_meta directory size: $(human_bytes "${actual_disk_meta_size}") (${actual_disk_meta_size} bytes)"
fi
if [[ -n "${actual_optical_meta_size}" ]]; then
  log "Actual optical_meta directory size: $(human_bytes "${actual_optical_meta_size}") (${actual_optical_meta_size} bytes)"
fi

log "=== done ==="
log "Output root: ${OUT_DIR}"
log "- Plan: ${OUT_DIR}/plan/meta_plan_report.md"
log "- MDS SST: ${OUT_DIR}/mds_sst"
log "- Disk meta: ${OUT_DIR}/disk_meta"
log "- Optical meta: ${OUT_DIR}/optical_meta"
log "- Summary: ${SUMMARY_CONF}"
log "- Detailed log: ${LOG_FILE}"
