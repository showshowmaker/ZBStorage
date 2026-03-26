#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
MDS_ADDR="${MDS_ADDR:-127.0.0.1:9000}"
SCHEDULER_ADDR="${SCHEDULER_ADDR:-127.0.0.1:9100}"
MOUNT_POINT="${MOUNT_POINT:-${ROOT_DIR}/.demo_run/mnt}"
NAMESPACE_PREFIX="${NAMESPACE_PREFIX:-demo-ns}"
FILES_PER_NAMESPACE=100000000
PAGE_SIZE_BYTES="${MASSTREE_PAGE_SIZE_BYTES:-4194304}"
VERIFY_INODE_SAMPLES="${MASSTREE_VERIFY_INODE_SAMPLES:-32}"
VERIFY_DENTRY_SAMPLES="${MASSTREE_VERIFY_DENTRY_SAMPLES:-32}"

NAMESPACE_COUNT="${1:-}"

if [[ -z "${NAMESPACE_COUNT}" ]]; then
  echo "Usage: $0 <namespace_count>"
  exit 1
fi

if [[ ! -x "${BUILD_DIR}/system_demo_tool" ]]; then
  echo "Missing executable: ${BUILD_DIR}/system_demo_tool"
  exit 1
fi

if ! [[ "${NAMESPACE_COUNT}" =~ ^[0-9]+$ ]] || [[ "${NAMESPACE_COUNT}" -le 0 ]]; then
  echo "namespace_count must be a positive integer"
  exit 1
fi

for ((i=1; i<=NAMESPACE_COUNT; ++i)); do
  NAMESPACE_ID="$(printf "%s-%06d" "${NAMESPACE_PREFIX}" "${i}")"
  echo "==== importing namespace ${i}/${NAMESPACE_COUNT}: ${NAMESPACE_ID} (${FILES_PER_NAMESPACE} files) ===="

  "${BUILD_DIR}/system_demo_tool" \
    --mds="${MDS_ADDR}" \
    --scheduler="${SCHEDULER_ADDR}" \
    --mount_point="${MOUNT_POINT}" \
    --scenario=masstree_import \
    --masstree_namespace_id="${NAMESPACE_ID}" \
    --masstree_file_count="${FILES_PER_NAMESPACE}" \
    --masstree_page_size_bytes="${PAGE_SIZE_BYTES}" \
    --masstree_verify_inode_samples="${VERIFY_INODE_SAMPLES}" \
    --masstree_verify_dentry_samples="${VERIFY_DENTRY_SAMPLES}"
done
