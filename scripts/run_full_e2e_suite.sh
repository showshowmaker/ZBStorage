#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
SCRIPTS_DIR="${ROOT_DIR}/scripts"
TS="$(date +%Y%m%d_%H%M%S)"

RUN_DIR="${RUN_DIR:-${ROOT_DIR}/runtime/e2e_${TS}}"
LOG_DIR="${RUN_DIR}/logs"
TEST_LOG_DIR="${LOG_DIR}/tests"
STARTUP_LOG_DIR="${LOG_DIR}/startup"
REPORT_DIR="${RUN_DIR}/report"
REPORT_FILE="${REPORT_DIR}/e2e_report_${TS}.md"
ENV_FILE="${RUN_DIR}/cluster_env.sh"
MOUNT_DIR="${MOUNT_DIR:-${RUN_DIR}/mnt/zbfs}"

SKIP_MOUNT="${SKIP_MOUNT:-false}"
ENABLE_SCHED_CONTROL_TEST="${ENABLE_SCHED_CONTROL_TEST:-false}"
TIMEOUT_SEC="${TIMEOUT_SEC:-120}"

PASS_COUNT=0
FAIL_COUNT=0
TOTAL_COUNT=0
declare -a TEST_NAMES=()
declare -a TEST_RESULTS=()
declare -a TEST_DURS=()
declare -a TEST_LOGS=()
declare -a TEST_CMDS=()

cleanup() {
  set +e

  if [[ "${SKIP_MOUNT}" != "true" ]] && command -v mountpoint >/dev/null 2>&1; then
    if mountpoint -q "${MOUNT_DIR}" >/dev/null 2>&1; then
      if command -v fusermount3 >/dev/null 2>&1; then
        fusermount3 -u "${MOUNT_DIR}" >/dev/null 2>&1 || true
      fi
      if mountpoint -q "${MOUNT_DIR}" >/dev/null 2>&1; then
        umount "${MOUNT_DIR}" >/dev/null 2>&1 || true
      fi
    fi
  fi

  stop_from_pidfile "${RUN_DIR}/pids_client.txt"
  stop_from_pidfile "${RUN_DIR}/pids_data_nodes.txt"
  stop_from_pidfile "${RUN_DIR}/pids_control_plane.txt"
}

stop_from_pidfile() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 0

  while IFS=: read -r name pid; do
    [[ -n "${pid:-}" ]] || continue
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done < "${pid_file}"

  sleep 1
  while IFS=: read -r name pid; do
    [[ -n "${pid:-}" ]] || continue
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  done < "${pid_file}"

  rm -f "${pid_file}"
}

trap cleanup EXIT INT TERM

require_file() {
  local path="$1"
  if [[ ! -f "${path}" ]]; then
    echo "Missing file: ${path}" >&2
    exit 1
  fi
}

require_bin() {
  local path="$1"
  if [[ ! -x "${path}" ]]; then
    echo "Missing binary: ${path}" >&2
    exit 1
  fi
}

run_startup_step() {
  local name="$1"
  local cmd="$2"
  local log_file="${STARTUP_LOG_DIR}/${name}.log"
  echo "=== [startup:${name}] ==="
  echo "${cmd}"
  if bash -lc "${cmd}" >"${log_file}" 2>&1; then
    echo "[OK] ${name}"
    return 0
  fi
  echo "[FAIL] ${name}. See ${log_file}" >&2
  tail -n 50 "${log_file}" >&2 || true
  exit 1
}

run_case() {
  local name="$1"
  local cmd="$2"
  local log_file="${TEST_LOG_DIR}/${name}.log"
  local start_ts end_ts dur

  TOTAL_COUNT=$((TOTAL_COUNT + 1))
  TEST_NAMES+=("${name}")
  TEST_CMDS+=("${cmd}")
  TEST_LOGS+=("${log_file}")

  echo "=== [test:${name}] ==="
  echo "${cmd}"

  start_ts="$(date +%s)"
  if command -v timeout >/dev/null 2>&1; then
    if timeout --preserve-status "${TIMEOUT_SEC}" bash -lc "${cmd}" >"${log_file}" 2>&1; then
      end_ts="$(date +%s)"
      dur=$((end_ts - start_ts))
      TEST_RESULTS+=("PASS")
      TEST_DURS+=("${dur}")
      PASS_COUNT=$((PASS_COUNT + 1))
      echo "[PASS] ${name} (${dur}s)"
      return
    fi
  else
    if bash -lc "${cmd}" >"${log_file}" 2>&1; then
      end_ts="$(date +%s)"
      dur=$((end_ts - start_ts))
      TEST_RESULTS+=("PASS")
      TEST_DURS+=("${dur}")
      PASS_COUNT=$((PASS_COUNT + 1))
      echo "[PASS] ${name} (${dur}s)"
      return
    fi
  fi

  end_ts="$(date +%s)"
  dur=$((end_ts - start_ts))
  TEST_RESULTS+=("FAIL")
  TEST_DURS+=("${dur}")
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "[FAIL] ${name} (${dur}s), see ${log_file}" >&2
}

generate_report() {
  mkdir -p "${REPORT_DIR}"
  {
    echo "# ZBStorage E2E Test Report"
    echo
    echo "- generated_at: $(date '+%Y-%m-%d %H:%M:%S %z')"
    echo "- run_dir: ${RUN_DIR}"
    echo "- report_file: ${REPORT_FILE}"
    echo "- skip_mount: ${SKIP_MOUNT}"
    echo "- timeout_sec: ${TIMEOUT_SEC}"
    if [[ -f "${ENV_FILE}" ]]; then
      echo "- scheduler: ${ZB_SCHEDULER_ADDR:-N/A}"
      echo "- mds: ${ZB_MDS_ADDR:-N/A}"
      echo "- real_servers: ${ZB_REAL_SERVERS:-N/A}"
      echo "- virtual_server: ${ZB_VIRTUAL_SERVER:-N/A}"
    fi
    echo
    echo "## Summary"
    echo
    echo "- total: ${TOTAL_COUNT}"
    echo "- pass: ${PASS_COUNT}"
    echo "- fail: ${FAIL_COUNT}"
    if [[ "${FAIL_COUNT}" -eq 0 ]]; then
      echo "- result: PASS"
    else
      echo "- result: FAIL"
    fi
    echo
    echo "## Cases"
    echo
    echo "| case | result | duration_s | log |"
    echo "|---|---|---:|---|"
    for i in "${!TEST_NAMES[@]}"; do
      echo "| ${TEST_NAMES[$i]} | ${TEST_RESULTS[$i]} | ${TEST_DURS[$i]} | ${TEST_LOGS[$i]} |"
    done
    echo
    echo "## Startup Logs"
    echo
    echo "- control_plane: ${STARTUP_LOG_DIR}/start_control_plane.log"
    echo "- data_nodes: ${STARTUP_LOG_DIR}/start_data_nodes.log"
    if [[ "${SKIP_MOUNT}" != "true" ]]; then
      echo "- mount_client: ${STARTUP_LOG_DIR}/mount_client.log"
    fi
    echo
    echo "## Case Tail (last 20 lines)"
    echo
    for i in "${!TEST_NAMES[@]}"; do
      echo "### ${TEST_NAMES[$i]} (${TEST_RESULTS[$i]})"
      echo
      echo '```bash'
      echo "${TEST_CMDS[$i]}"
      echo '```'
      echo
      echo '```text'
      tail -n 20 "${TEST_LOGS[$i]}" || true
      echo '```'
      echo
    done
  } > "${REPORT_FILE}"
}

mkdir -p "${RUN_DIR}" "${TEST_LOG_DIR}" "${STARTUP_LOG_DIR}" "${REPORT_DIR}"

require_file "${SCRIPTS_DIR}/oneclick_start_control_plane.sh"
require_file "${SCRIPTS_DIR}/oneclick_start_data_nodes.sh"
require_file "${SCRIPTS_DIR}/oneclick_mount_client.sh"

require_bin "${BUILD_DIR}/real_node_multi_test"
require_bin "${BUILD_DIR}/real_node_client"

if [[ "${ENABLE_SCHED_CONTROL_TEST}" == "true" ]]; then
  require_bin "${BUILD_DIR}/scheduler_control_test"
fi

if [[ "${SKIP_MOUNT}" != "true" ]]; then
  require_bin "${BUILD_DIR}/zb_fuse_client"
  if ! command -v mountpoint >/dev/null 2>&1; then
    echo "mountpoint command not found, cannot run metadata ops through FUSE." >&2
    exit 1
  fi
fi

run_startup_step \
  "start_control_plane" \
  "RUN_DIR='${RUN_DIR}' bash '${SCRIPTS_DIR}/oneclick_start_control_plane.sh'"

run_startup_step \
  "start_data_nodes" \
  "RUN_DIR='${RUN_DIR}' bash '${SCRIPTS_DIR}/oneclick_start_data_nodes.sh'"

if [[ "${SKIP_MOUNT}" != "true" ]]; then
  run_startup_step \
    "mount_client" \
    "RUN_DIR='${RUN_DIR}' bash '${SCRIPTS_DIR}/oneclick_mount_client.sh' '${MOUNT_DIR}'"
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file after startup: ${ENV_FILE}" >&2
  exit 1
fi
source "${ENV_FILE}"

REAL_FIRST_SERVER="${ZB_REAL_SERVERS%%,*}"
REAL_FIRST_DISK="${ZB_REAL_DISKS%%,*}"
VIRTUAL_TEST_DISK="${VIRTUAL_TEST_DISK:-${REAL_FIRST_DISK}}"
UNIQ="e2e_${TS}"

REAL_PAYLOAD="real_payload_${UNIQ}"
VIRTUAL_PAYLOAD="virtual_payload_${UNIQ}"
REAL_CHUNK_ID="real_chunk_${UNIQ}"
VIRTUAL_CHUNK_ID="virtual_chunk_${UNIQ}"

run_case \
  "real_node_multi_test" \
  "\"${BUILD_DIR}/real_node_multi_test\" --servers=\"${ZB_REAL_SERVERS}\" --disks=\"${ZB_REAL_DISKS}\" --verify_fs=true --config_files=\"${ZB_REAL_CONFIGS}\""

run_case \
  "real_node_rw_via_client" \
  "set -euo pipefail; \
   \"${BUILD_DIR}/real_node_client\" --server=\"${REAL_FIRST_SERVER}\" --disk_id=\"${REAL_FIRST_DISK}\" --chunk_id=\"${REAL_CHUNK_ID}\" --write_data=\"${REAL_PAYLOAD}\" --read_size=${#REAL_PAYLOAD} --mode=both; \
   \"${BUILD_DIR}/real_node_client\" --server=\"${REAL_FIRST_SERVER}\" --disk_id=\"${REAL_FIRST_DISK}\" --chunk_id=\"${REAL_CHUNK_ID}\" --read_size=${#REAL_PAYLOAD} --mode=read | grep -q \"data=${REAL_PAYLOAD}\""

run_case \
  "virtual_node_rw_via_client" \
  "set -euo pipefail; \
   \"${BUILD_DIR}/real_node_client\" --server=\"${ZB_VIRTUAL_SERVER}\" --disk_id=\"${VIRTUAL_TEST_DISK}\" --chunk_id=\"${VIRTUAL_CHUNK_ID}\" --write_data=\"${VIRTUAL_PAYLOAD}\" --read_size=${#VIRTUAL_PAYLOAD} --mode=both; \
   \"${BUILD_DIR}/real_node_client\" --server=\"${ZB_VIRTUAL_SERVER}\" --disk_id=\"${VIRTUAL_TEST_DISK}\" --chunk_id=\"${VIRTUAL_CHUNK_ID}\" --read_size=${#VIRTUAL_PAYLOAD} --mode=read | grep -q \"data=${VIRTUAL_PAYLOAD}\""

if [[ "${SKIP_MOUNT}" != "true" ]]; then
  run_case \
    "mds_metadata_ops_via_fuse" \
    "set -euo pipefail; \
     base='${MOUNT_DIR}/e2e_meta_${UNIQ}'; \
     mkdir -p \"${MOUNT_DIR}\"; \
     test -d \"${MOUNT_DIR}\"; \
     mkdir -p \"${base}/dir1\"; \
     echo 'meta-hello-${UNIQ}' > \"${base}/dir1/file1.txt\"; \
     test -f \"${base}/dir1/file1.txt\"; \
     mv \"${base}/dir1/file1.txt\" \"${base}/dir1/file2.txt\"; \
     test -f \"${base}/dir1/file2.txt\"; \
     stat \"${base}/dir1/file2.txt\" >/dev/null; \
     truncate -s 1024 \"${base}/dir1/file2.txt\"; \
     test \"\$(stat -c%s \"${base}/dir1/file2.txt\")\" -eq 1024; \
     ls -la \"${base}/dir1\" >/dev/null; \
     rm -f \"${base}/dir1/file2.txt\"; \
     rmdir \"${base}/dir1\"; \
     rmdir \"${base}\""
else
  echo "[SKIP] mds_metadata_ops_via_fuse (SKIP_MOUNT=true)"
fi

if [[ "${ENABLE_SCHED_CONTROL_TEST}" == "true" ]]; then
  run_case \
    "scheduler_control_test" \
    "\"${BUILD_DIR}/scheduler_control_test\" --scheduler=\"${ZB_SCHEDULER_ADDR}\" --node_id=node-01 --do_reboot=true"
fi

generate_report
echo "Report generated: ${REPORT_FILE}"

if [[ "${FAIL_COUNT}" -ne 0 ]]; then
  exit 1
fi
