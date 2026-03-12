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
STEP_PROGRESS_INTERVAL_SEC="${STEP_PROGRESS_INTERVAL_SEC:-30}"

MDS_PROGRESS_INTERVAL_FILES="${MDS_PROGRESS_INTERVAL_FILES:-1000000}"
MDS_PROGRESS_INTERVAL_SEC="${MDS_PROGRESS_INTERVAL_SEC:-30}"
DISK_PROGRESS_INTERVAL_FILES="${DISK_PROGRESS_INTERVAL_FILES:-1000000}"
DISK_PROGRESS_INTERVAL_SEC="${DISK_PROGRESS_INTERVAL_SEC:-30}"
OPTICAL_PROGRESS_INTERVAL_FILES="${OPTICAL_PROGRESS_INTERVAL_FILES:-1000000}"
OPTICAL_PROGRESS_INTERVAL_SEC="${OPTICAL_PROGRESS_INTERVAL_SEC:-30}"

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

STEP_KEYS=("planning" "mds_sst_generation" "disk_meta_generation" "optical_meta_generation")
declare -A STEP_STATUS
for __k in "${STEP_KEYS[@]}"; do
  STEP_STATUS["${__k}"]="PENDING"
done

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

format_duration() {
  local total_sec="$1"
  if [[ -z "${total_sec}" || ! "${total_sec}" =~ ^[0-9]+$ ]]; then
    echo "0s"
    return
  fi
  local h=$((total_sec / 3600))
  local m=$(((total_sec % 3600) / 60))
  local s=$((total_sec % 60))
  if (( h > 0 )); then
    echo "${h}h${m}m${s}s"
  elif (( m > 0 )); then
    echo "${m}m${s}s"
  else
    echo "${s}s"
  fi
}

print_stage_board() {
  local board="Pipeline stages:"
  local k
  for k in "${STEP_KEYS[@]}"; do
    board="${board} ${k}=${STEP_STATUS[${k}]}"
  done
  log "${board}"
}

count_remaining_steps() {
  local key="$1"
  local found=0
  local count=0
  local k
  for k in "${STEP_KEYS[@]}"; do
    if [[ "${k}" == "${key}" ]]; then
      found=1
      continue
    fi
    if (( found == 1 )) && [[ "${STEP_STATUS[${k}]}" == "PENDING" ]]; then
      count=$((count + 1))
    fi
  done
  echo "${count}"
}

step_progress_snapshot() {
  local step_key="$1"
  local elapsed_sec="$2"
  local free_bytes
  free_bytes="$(free_bytes_under_out_dir)"
  case "${step_key}" in
    planning)
      local report_file="${OUT_DIR}/plan/meta_plan_report.md"
      if [[ -f "${report_file}" ]]; then
        local report_size
        report_size="$(wc -c < "${report_file}" 2>/dev/null || echo 0)"
        log "[PROGRESS][planning] elapsed=$(format_duration "${elapsed_sec}") report_size=$(human_bytes "${report_size}") free_space=$(human_bytes "${free_bytes}")"
      else
        log "[PROGRESS][planning] elapsed=$(format_duration "${elapsed_sec}") report=not_created free_space=$(human_bytes "${free_bytes}")"
      fi
      ;;
    mds_sst_generation)
      local sst_count
      local mds_size
      sst_count="$(find "${OUT_DIR}/mds_sst" -maxdepth 1 -name '*.sst' 2>/dev/null | wc -l)"
      mds_size="$(du -sb "${OUT_DIR}/mds_sst" 2>/dev/null | awk '{print $1}')"
      if [[ -z "${mds_size}" ]]; then
        mds_size=0
      fi
      log "[PROGRESS][mds_sst_generation] elapsed=$(format_duration "${elapsed_sec}") sst_count=${sst_count} mds_sst_size=$(human_bytes "${mds_size}") free_space=$(human_bytes "${free_bytes}")"
      ;;
    disk_meta_generation)
      local disk_size
      local node_files
      disk_size="$(du -sb "${OUT_DIR}/disk_meta" 2>/dev/null | awk '{print $1}')"
      if [[ -z "${disk_size}" ]]; then
        disk_size=0
      fi
      node_files="$(find "${OUT_DIR}/disk_meta" -name 'file_meta.tsv' 2>/dev/null | wc -l)"
      log "[PROGRESS][disk_meta_generation] elapsed=$(format_duration "${elapsed_sec}") file_meta_files=${node_files} disk_meta_size=$(human_bytes "${disk_size}") free_space=$(human_bytes "${free_bytes}")"
      ;;
    optical_meta_generation)
      local optical_size
      local manifest_size
      optical_size="$(du -sb "${OUT_DIR}/optical_meta" 2>/dev/null | awk '{print $1}')"
      manifest_size="$(wc -c < "${OUT_DIR}/optical_meta/optical_manifest.tsv" 2>/dev/null || echo 0)"
      if [[ -z "${optical_size}" ]]; then
        optical_size=0
      fi
      log "[PROGRESS][optical_meta_generation] elapsed=$(format_duration "${elapsed_sec}") optical_manifest_size=$(human_bytes "${manifest_size}") optical_meta_size=$(human_bytes "${optical_size}") free_space=$(human_bytes "${free_bytes}")"
      ;;
    *)
      log "[PROGRESS][${step_key}] elapsed=$(format_duration "${elapsed_sec}") free_space=$(human_bytes "${free_bytes}")"
      ;;
  esac
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

monitor_step_progress_for_pid() {
  local target_pid="$1"
  local step_key="$2"
  local interval_sec="$3"
  local start_ts
  start_ts="$(date +%s)"
  while kill -0 "${target_pid}" >/dev/null 2>&1; do
    local now_ts elapsed_sec
    now_ts="$(date +%s)"
    elapsed_sec=$((now_ts - start_ts))
    step_progress_snapshot "${step_key}" "${elapsed_sec}"
    sleep "${interval_sec}"
  done
}

run_step() {
  local step_key="$1"
  local step_name="$2"
  shift
  shift
  STEP_STATUS["${step_key}"]="RUNNING"
  print_stage_board
  local remaining
  remaining="$(count_remaining_steps "${step_key}")"
  log "Current step=${step_name}, remaining_steps=${remaining}"
  log "=== [${step_name}] ==="
  log "CMD: $*"
  rm -f "${ABORT_FLAG}"

  set +e
  "$@" 2>&1 | tee -a "${LOG_FILE}" &
  local cmd_pid=$!
  local mon_pid=""
  local progress_pid=""
  if [[ "${ENABLE_SPACE_GUARD}" == "1" ]]; then
    local threshold_bytes
    threshold_bytes="$(echo "${MIN_FREE_SPACE_TB}*1000000000000" | bc)"
    monitor_space_for_pid "${cmd_pid}" "${threshold_bytes}" "${SPACE_CHECK_INTERVAL_SEC}" &
    mon_pid=$!
  fi
  if [[ "${STEP_PROGRESS_INTERVAL_SEC}" =~ ^[0-9]+$ ]] && (( STEP_PROGRESS_INTERVAL_SEC > 0 )); then
    monitor_step_progress_for_pid "${cmd_pid}" "${step_key}" "${STEP_PROGRESS_INTERVAL_SEC}" &
    progress_pid=$!
  fi

  wait "${cmd_pid}"
  local rc=$?
  if [[ -n "${mon_pid}" ]]; then
    kill "${mon_pid}" >/dev/null 2>&1 || true
    wait "${mon_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${progress_pid}" ]]; then
    kill "${progress_pid}" >/dev/null 2>&1 || true
    wait "${progress_pid}" >/dev/null 2>&1 || true
  fi
  set -e

  if [[ -f "${ABORT_FLAG}" ]]; then
    STEP_STATUS["${step_key}"]="FAILED"
    print_stage_board
    log "[FAIL] ${step_name} aborted by free-space guard: $(cat "${ABORT_FLAG}")"
    exit 11
  fi
  if [[ ${rc} -ne 0 ]]; then
    STEP_STATUS["${step_key}"]="FAILED"
    print_stage_board
    log "[FAIL] ${step_name} exited with ${rc}"
    exit ${rc}
  fi
  STEP_STATUS["${step_key}"]="DONE"
  print_stage_board
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
log "Step progress monitor interval: ${STEP_PROGRESS_INTERVAL_SEC}s"
log "Tool progress intervals: mds(files=${MDS_PROGRESS_INTERVAL_FILES},sec=${MDS_PROGRESS_INTERVAL_SEC}) disk(files=${DISK_PROGRESS_INTERVAL_FILES},sec=${DISK_PROGRESS_INTERVAL_SEC}) optical(files=${OPTICAL_PROGRESS_INTERVAL_FILES},sec=${OPTICAL_PROGRESS_INTERVAL_SEC})"
print_stage_board
estimate_meta_size "${TOTAL_FILES}" \
                   "${EST_MDS_BYTES_PER_FILE}" \
                   "${EST_OPTICAL_MANIFEST_BYTES_PER_FILE}" \
                   "${EST_DISK_META_LINE_BYTES}" \
                   "${EST_AVG_FILE_SIZE_BYTES}" \
                   "${disk_total_bytes_est}" \
                   "${DISK_REPLICA_RATIO}" \
                   "${optical_disc_count_est}"

run_step "planning" "1/4 planning" "${META_PLAN_BIN}" \
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

run_step "mds_sst_generation" "2/4 mds_sst_generation" "${MDS_SST_GEN_BIN}" \
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
  --max_kv_per_sst="${SST_MAX_KV}" \
  --progress_interval_files="${MDS_PROGRESS_INTERVAL_FILES}" \
  --progress_interval_sec="${MDS_PROGRESS_INTERVAL_SEC}"

run_step "disk_meta_generation" "3/4 disk_meta_generation" "${DISK_META_BIN}" \
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
  --seed="${SIZE_SEED}" \
  --progress_interval_files="${DISK_PROGRESS_INTERVAL_FILES}" \
  --progress_interval_sec="${DISK_PROGRESS_INTERVAL_SEC}"

run_step "optical_meta_generation" "4/4 optical_meta_generation" "${OPTICAL_META_BIN}" \
  --output_dir="${OUT_DIR}/optical_meta" \
  --total_files="${TOTAL_FILES}" \
  --namespace_count="${NAMESPACE_COUNT}" \
  --optical_node_count="${OPTICAL_NODE_COUNT}" \
  --discs_per_optical_node="${DISCS_PER_OPTICAL_NODE}" \
  --target_optical_eb="${TARGET_OPTICAL_EB}" \
  --branch_factor="${BRANCH_FACTOR}" \
  --max_depth="${MAX_DEPTH}" \
  --max_files_per_leaf="${MAX_FILES_PER_LEAF}" \
  --seed="${SIZE_SEED}" \
  --progress_interval_files="${OPTICAL_PROGRESS_INTERVAL_FILES}" \
  --progress_interval_sec="${OPTICAL_PROGRESS_INTERVAL_SEC}"

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
