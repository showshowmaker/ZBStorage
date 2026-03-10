#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
SCRIPTS_DIR="${ROOT_DIR}/scripts"
TS="$(date +%Y%m%d_%H%M%S)"

RUN_DIR="${RUN_DIR:-${ROOT_DIR}/runtime/module_io_smoke_${TS}}"
LOG_DIR="${RUN_DIR}/logs"
STARTUP_LOG_DIR="${LOG_DIR}/startup"
TEST_LOG_DIR="${LOG_DIR}/tests"
REPORT_DIR="${RUN_DIR}/report"
REPORT_FILE="${REPORT_DIR}/module_io_smoke_report_${TS}.md"
ENV_FILE="${RUN_DIR}/cluster_env.sh"

ENABLE_OPTICAL_ARCHIVE="${ENABLE_OPTICAL_ARCHIVE:-true}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
if [[ -z "${BUILD_JOBS:-}" ]]; then
  if command -v nproc >/dev/null 2>&1; then
    BUILD_JOBS="$(nproc)"
  else
    BUILD_JOBS="4"
  fi
fi

TEST_BIN="${BUILD_DIR}/module_io_smoke_test"
RESULT="FAIL"
TEST_EXIT_CODE=1

stop_from_pidfile() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 0
  while IFS=: read -r _name pid; do
    [[ -n "${pid:-}" ]] || continue
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done < "${pid_file}"
  sleep 1
  while IFS=: read -r _name pid; do
    [[ -n "${pid:-}" ]] || continue
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  done < "${pid_file}"
  rm -f "${pid_file}"
}

cleanup() {
  set +e
  stop_from_pidfile "${RUN_DIR}/pids_client.txt"
  stop_from_pidfile "${RUN_DIR}/pids_data_nodes.txt"
  stop_from_pidfile "${RUN_DIR}/pids_control_plane.txt"
}

generate_report() {
  mkdir -p "${REPORT_DIR}"
  {
    echo "# Module IO Smoke Report"
    echo
    echo "- generated_at: $(date '+%Y-%m-%d %H:%M:%S %z')"
    echo "- run_dir: ${RUN_DIR}"
    echo "- result: ${RESULT}"
    echo "- exit_code: ${TEST_EXIT_CODE}"
    echo "- enable_optical_archive: ${ENABLE_OPTICAL_ARCHIVE}"
    echo
    echo "## Artifacts"
    echo
    echo "- startup_control_plane_log: ${STARTUP_LOG_DIR}/start_control_plane.log"
    echo "- startup_data_nodes_log: ${STARTUP_LOG_DIR}/start_data_nodes.log"
    echo "- build_log: ${LOG_DIR}/build.log"
    echo "- test_log: ${TEST_LOG_DIR}/module_io_smoke_test.log"
    echo
    echo "## Test Log Tail"
    echo
    echo '```text'
    tail -n 80 "${TEST_LOG_DIR}/module_io_smoke_test.log" 2>/dev/null || true
    echo '```'
  } > "${REPORT_FILE}"
  echo "Report generated: ${REPORT_FILE}"
}

trap 'cleanup; generate_report' EXIT INT TERM

mkdir -p "${RUN_DIR}" "${LOG_DIR}" "${STARTUP_LOG_DIR}" "${TEST_LOG_DIR}"

echo "=== [build:configure] ==="
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" > "${LOG_DIR}/build.log" 2>&1

echo "=== [build:targets] ==="
cmake --build "${BUILD_DIR}" -j "${BUILD_JOBS}" --target \
  scheduler_server \
  mds_server \
  real_node_server \
  virtual_node_server \
  optical_node_server \
  module_io_smoke_test >> "${LOG_DIR}/build.log" 2>&1

if [[ ! -x "${TEST_BIN}" ]]; then
  echo "Missing test binary: ${TEST_BIN}" >&2
  exit 1
fi

echo "=== [startup:control_plane] ==="
RUN_DIR="${RUN_DIR}" ENABLE_OPTICAL_ARCHIVE="${ENABLE_OPTICAL_ARCHIVE}" \
  bash "${SCRIPTS_DIR}/oneclick_start_control_plane.sh" \
  > "${STARTUP_LOG_DIR}/start_control_plane.log" 2>&1

echo "=== [startup:data_nodes] ==="
RUN_DIR="${RUN_DIR}" ENABLE_OPTICAL_ARCHIVE="${ENABLE_OPTICAL_ARCHIVE}" \
  bash "${SCRIPTS_DIR}/oneclick_start_data_nodes.sh" \
  > "${STARTUP_LOG_DIR}/start_data_nodes.log" 2>&1

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  exit 1
fi
source "${ENV_FILE}"
sleep 1

REAL_FIRST_SERVER="${ZB_REAL_SERVERS%%,*}"
REAL_FIRST_DISK="${ZB_REAL_DISKS%%,*}"
OPTICAL_DISC_ID="${OPTICAL_DISC_ID:-odisk0}"
EXPECT_OPTICAL="false"
if [[ "${ENABLE_OPTICAL_ARCHIVE}" == "true" ]]; then
  EXPECT_OPTICAL="true"
fi

TEST_CMD=(
  "${TEST_BIN}"
  "--scheduler=${ZB_SCHEDULER_ADDR}"
  "--mds=${ZB_MDS_ADDR}"
  "--real=${REAL_FIRST_SERVER}"
  "--virtual_node=${ZB_VIRTUAL_SERVER}"
  "--optical=${ZB_OPTICAL_SERVER}"
  "--real_disk=${REAL_FIRST_DISK}"
  "--virtual_disk=${REAL_FIRST_DISK}"
  "--optical_disc=${OPTICAL_DISC_ID}"
  "--expect_optical=${EXPECT_OPTICAL}"
)

echo "=== [test:module_io_smoke_test] ==="
printf '%q ' "${TEST_CMD[@]}"
echo
set +e
"${TEST_CMD[@]}" > "${TEST_LOG_DIR}/module_io_smoke_test.log" 2>&1
TEST_EXIT_CODE=$?
set -e

if [[ "${TEST_EXIT_CODE}" -eq 0 ]]; then
  RESULT="PASS"
  echo "[PASS] module_io_smoke_test"
else
  RESULT="FAIL"
  echo "[FAIL] module_io_smoke_test, see ${TEST_LOG_DIR}/module_io_smoke_test.log" >&2
  tail -n 100 "${TEST_LOG_DIR}/module_io_smoke_test.log" >&2 || true
  exit "${TEST_EXIT_CODE}"
fi

echo
