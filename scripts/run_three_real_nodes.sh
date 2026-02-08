#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BUILD_DIR="${ROOT_DIR}/build"

BIN="${BUILD_DIR}/real_node_server"
if [[ ! -x "${BIN}" ]]; then
  echo "real_node_server not found at ${BIN}. Build first:" >&2
  echo "  cmake -S ${ROOT_DIR} -B ${BUILD_DIR} && cmake --build ${BUILD_DIR} -j" >&2
  exit 1
fi

CONFIG_MP1="${ROOT_DIR}/config/real_node_mp1.conf"
CONFIG_MP2="${ROOT_DIR}/config/real_node_mp2.conf"
CONFIG_MP3="${ROOT_DIR}/config/real_node_mp3.conf"

for cfg in "${CONFIG_MP1}" "${CONFIG_MP2}" "${CONFIG_MP3}"; do
  if [[ ! -f "${cfg}" ]]; then
    echo "Missing config file: ${cfg}" >&2
    exit 1
  fi
  if ! grep -q "^ZB_DISKS=" "${cfg}"; then
    echo "Config missing ZB_DISKS: ${cfg}" >&2
    exit 1
  fi
  # Basic path existence check (best-effort)
  zb_line=$(grep -E '^ZB_DISKS=' "${cfg}" | head -n1 || true)
  zb_value=${zb_line#ZB_DISKS=}
  paths=$(echo "${zb_value}" | tr ';' '\n' | cut -d: -f2-)
  for p in ${paths}; do
    if [[ ! -d "${p}" ]]; then
      echo "Warning: path does not exist: ${p}" >&2
    fi
  done
 done

PORT1=19080
PORT2=19081
PORT3=19082

LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

"${BIN}" --config="${CONFIG_MP1}" --port=${PORT1} >"${LOG_DIR}/real_node_mp1.log" 2>&1 &
PID1=$!
"${BIN}" --config="${CONFIG_MP2}" --port=${PORT2} >"${LOG_DIR}/real_node_mp2.log" 2>&1 &
PID2=$!
"${BIN}" --config="${CONFIG_MP3}" --port=${PORT3} >"${LOG_DIR}/real_node_mp3.log" 2>&1 &
PID3=$!

trap 'echo "Stopping..."; kill ${PID1} ${PID2} ${PID3} 2>/dev/null || true' INT TERM

echo "Started real nodes:"
echo "  mp1 -> port ${PORT1}, log ${LOG_DIR}/real_node_mp1.log"
echo "  mp2 -> port ${PORT2}, log ${LOG_DIR}/real_node_mp2.log"
echo "  mp3 -> port ${PORT3}, log ${LOG_DIR}/real_node_mp3.log"

wait ${PID1} ${PID2} ${PID3}
