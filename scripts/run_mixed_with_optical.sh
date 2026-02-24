#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

start_bg() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  nohup "$@" >"${log_file}" 2>&1 &
  local pid=$!
  echo "${name} started pid=${pid} log=${log_file}"
}

start_bg scheduler_server "${BUILD_DIR}/scheduler_server" --config="${ROOT_DIR}/config/scheduler.conf.example" --port=9100
start_bg real_node_1 "${BUILD_DIR}/real_node_server" --config="${ROOT_DIR}/config/real_node_mp1.conf" --port=19080
start_bg real_node_2 "${BUILD_DIR}/real_node_server" --config="${ROOT_DIR}/config/real_node_mp2.conf" --port=19081
start_bg real_node_3 "${BUILD_DIR}/real_node_server" --config="${ROOT_DIR}/config/real_node_mp3.conf" --port=19082
start_bg virtual_node "${BUILD_DIR}/virtual_node_server" --config="${ROOT_DIR}/config/virtual_node.conf.example" --port=29080
start_bg optical_node "${BUILD_DIR}/optical_node_server" --config="${ROOT_DIR}/config/optical_node.conf.example" --port=39080
start_bg mds_server "${BUILD_DIR}/mds_server" --config="${ROOT_DIR}/config/mds.conf.optical" --port=9000

echo "All mixed services (with optical) started."
