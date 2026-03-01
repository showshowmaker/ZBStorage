#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
RUNTIME_ROOT="${ROOT_DIR}/runtime"

RUN_DIR_ARG="${1:-}"
if [[ -n "${RUN_DIR_ARG}" ]]; then
  if [[ -d "${RUN_DIR_ARG}" ]]; then
    RUN_DIR="${RUN_DIR_ARG}"
  else
    RUN_DIR="${RUNTIME_ROOT}/${RUN_DIR_ARG}"
  fi
else
  LATEST="$(ls -1dt "${RUNTIME_ROOT}"/interaction_cluster_* 2>/dev/null | head -n 1 || true)"
  if [[ -z "${LATEST}" ]]; then
    echo "No runtime cluster found. Start one first with scripts/start_interaction_cluster.sh" >&2
    exit 1
  fi
  RUN_DIR="${LATEST}"
fi

if [[ ! -d "${RUN_DIR}" ]]; then
  echo "Run dir not found: ${RUN_DIR}" >&2
  exit 1
fi

ENV_FILE="${RUN_DIR}/cluster_env.sh"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}" >&2
  exit 1
fi
source "${ENV_FILE}"

REQUIRED_BINS=(
  real_node_multi_test
  virtual_node_test
  scheduler_control_test
  optical_archive_stress_test
)

for bin in "${REQUIRED_BINS[@]}"; do
  if [[ ! -x "${BUILD_DIR}/${bin}" ]]; then
    echo "Missing binary: ${BUILD_DIR}/${bin}" >&2
    exit 1
  fi
done

REPORT_DIR="${RUN_DIR}/report"
TEST_LOG_DIR="${REPORT_DIR}/logs"
mkdir -p "${TEST_LOG_DIR}"

TS="$(date +%Y%m%d_%H%M%S)"
REPORT_FILE="${REPORT_DIR}/integration_report_${TS}.md"

PASS_COUNT=0
FAIL_COUNT=0
TOTAL_COUNT=0

declare -a TEST_NAMES=()
declare -a TEST_RESULTS=()
declare -a TEST_LOGS=()
declare -a TEST_CMDS=()

run_case() {
  local name="$1"
  shift
  local cmd="$*"
  local log_file="${TEST_LOG_DIR}/${name}.log"

  TOTAL_COUNT=$((TOTAL_COUNT + 1))
  TEST_NAMES+=("${name}")
  TEST_CMDS+=("${cmd}")
  TEST_LOGS+=("${log_file}")

  echo "=== [${name}] ==="
  echo "${cmd}"
  if bash -lc "${cmd}" >"${log_file}" 2>&1; then
    echo "[PASS] ${name}"
    TEST_RESULTS+=("PASS")
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] ${name} (see ${log_file})"
    TEST_RESULTS+=("FAIL")
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

run_case "real_node_multi_test" \
  "\"${BUILD_DIR}/real_node_multi_test\" --servers=\"${ZB_REAL_SERVERS}\" --disks=\"${ZB_REAL_DISKS}\" --verify_fs=true --config_files=\"${ZB_REAL_CONFIGS}\""

run_case "virtual_node_test" \
  "\"${BUILD_DIR}/virtual_node_test\" --server=\"${ZB_VIRTUAL_SERVER}\" --disk=disk-01 --write_size=4096 --read_size=4096"

run_case "scheduler_control_test" \
  "\"${BUILD_DIR}/scheduler_control_test\" --scheduler=\"${ZB_SCHEDULER_ADDR}\" --node_id=node-01 --do_reboot=true"

run_case "optical_archive_stress_test" \
  "\"${BUILD_DIR}/optical_archive_stress_test\" --mds=\"${ZB_MDS_ADDR}\" --duration_sec=90 --file_count=64 --write_size=1048576 --report_interval_sec=10 --sample_files=16 --require_optical=true --cooldown_sec=30 --require_optical_only_after_cooldown=true"

{
  echo "# ZBStorage 模块交互读写测试报告"
  echo
  echo "- 生成时间: $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "- 运行目录: ${RUN_DIR}"
  echo "- Scheduler: ${ZB_SCHEDULER_ADDR}"
  echo "- MDS: ${ZB_MDS_ADDR}"
  echo "- Real 节点: ${ZB_REAL_SERVERS}"
  echo "- Virtual 节点数: ${ZB_VIRTUAL_NODE_COUNT}"
  echo "- Optical 光盘数: ${ZB_OPTICAL_DISK_COUNT}"
  echo
  echo "## 总结"
  echo
  echo "- 总用例数: ${TOTAL_COUNT}"
  echo "- 通过: ${PASS_COUNT}"
  echo "- 失败: ${FAIL_COUNT}"
  if [[ "${FAIL_COUNT}" -eq 0 ]]; then
    echo "- 结论: PASS"
  else
    echo "- 结论: FAIL"
  fi
  echo
  echo "## 详细结果"
  echo
  echo "| 用例 | 结果 | 日志 |"
  echo "|---|---|---|"
  for i in "${!TEST_NAMES[@]}"; do
    echo "| ${TEST_NAMES[$i]} | ${TEST_RESULTS[$i]} | ${TEST_LOGS[$i]} |"
  done
  echo
  echo "## 关键输出（每项最后20行）"
  echo
  for i in "${!TEST_NAMES[@]}"; do
    echo "### ${TEST_NAMES[$i]} (${TEST_RESULTS[$i]})"
    echo
    echo "命令:"
    echo
    echo '```bash'
    echo "${TEST_CMDS[$i]}"
    echo '```'
    echo
    echo "日志尾部:"
    echo
    echo '```text'
    tail -n 20 "${TEST_LOGS[$i]}" || true
    echo '```'
    echo
  done
} > "${REPORT_FILE}"

echo
echo "Report generated: ${REPORT_FILE}"

if [[ "${FAIL_COUNT}" -ne 0 ]]; then
  exit 1
fi

