#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
RUN_DIR="${RUN_DIR:-${ROOT_DIR}/.demo_run}"
MDS_ADDR="${MDS_ADDR:-127.0.0.1:9000}"
SCHEDULER_ADDR="${SCHEDULER_ADDR:-127.0.0.1:9100}"
MOUNT_POINT="${MOUNT_POINT:-${RUN_DIR}/mnt}"
REAL_DIR_NAME="${REAL_DIR_NAME:-real}"
VIRTUAL_DIR_NAME="${VIRTUAL_DIR_NAME:-virtual}"

if [[ ! -x "${BUILD_DIR}/system_demo_tool" ]]; then
  echo "Missing executable: ${BUILD_DIR}/system_demo_tool"
  exit 1
fi

exec "${BUILD_DIR}/system_demo_tool" \
  --mds="${MDS_ADDR}" \
  --scheduler="${SCHEDULER_ADDR}" \
  --mount_point="${MOUNT_POINT}" \
  --real_dir="${REAL_DIR_NAME}" \
  --virtual_dir="${VIRTUAL_DIR_NAME}" \
  --scenario=interactive \
  "$@"
