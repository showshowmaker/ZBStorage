#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
MDS_ADDR="${MDS_ADDR:-127.0.0.1:9000}"
SCHEDULER_ADDR="${SCHEDULER_ADDR:-127.0.0.1:9100}"
MOUNT_POINT="${MOUNT_POINT:-${ROOT_DIR}/.demo_run/mnt}"

FILE_COUNT="${1:-}"
NAMESPACE_ID="${2:-demo-ns-$(date +%Y%m%d_%H%M%S)}"

if [[ -z "${FILE_COUNT}" ]]; then
  echo "Usage: $0 <file_count> [namespace_id]"
  exit 1
fi

if [[ ! -x "${BUILD_DIR}/system_demo_tool" ]]; then
  echo "Missing executable: ${BUILD_DIR}/system_demo_tool"
  exit 1
fi

exec "${BUILD_DIR}/system_demo_tool" \
  --mds="${MDS_ADDR}" \
  --scheduler="${SCHEDULER_ADDR}" \
  --mount_point="${MOUNT_POINT}" \
  --scenario=masstree_import \
  --masstree_namespace_id="${NAMESPACE_ID}" \
  --masstree_file_count="${FILE_COUNT}"
