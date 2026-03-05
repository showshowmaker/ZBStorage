#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
RUN_DIR="${RUN_DIR:-${ROOT_DIR}/runtime/oneclick}"
LOG_DIR="${RUN_DIR}/logs"
ENV_FILE="${RUN_DIR}/cluster_env.sh"
PID_FILE="${RUN_DIR}/pids_client.txt"
MOUNT_DIR="${1:-${RUN_DIR}/mnt/zbfs}"

CLIENT_BIN="${BUILD_DIR}/zb_fuse_client"
if [[ ! -x "${CLIENT_BIN}" ]]; then
  echo "Missing binary: ${CLIENT_BIN}" >&2
  echo "Build first: cmake -S . -B build && cmake --build build -j" >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Run control-plane and data-node startup scripts first." >&2
  exit 1
fi
source "${ENV_FILE}"

mkdir -p "${LOG_DIR}" "${MOUNT_DIR}"

if ! command -v mountpoint >/dev/null 2>&1; then
  echo "mountpoint command not found; please install util-linux." >&2
  exit 1
fi

if mountpoint -q "${MOUNT_DIR}"; then
  echo "Already mounted: ${MOUNT_DIR}"
  exit 0
fi

if [[ -f "${PID_FILE}" ]]; then
  while IFS=: read -r name pid; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      echo "stopping previous ${name} pid=${pid}"
      kill "${pid}" 2>/dev/null || true
    fi
  done < "${PID_FILE}"
  rm -f "${PID_FILE}"
fi

LOG_FILE="${LOG_DIR}/zb_fuse_client.log"
nohup "${CLIENT_BIN}" --mds="${ZB_MDS_ADDR}" -- "${MOUNT_DIR}" -f >"${LOG_FILE}" 2>&1 &
PID=$!
echo "zb_fuse_client:${PID}" > "${PID_FILE}"

sleep 1
if ! mountpoint -q "${MOUNT_DIR}"; then
  echo "Mount failed. Check log: ${LOG_FILE}" >&2
  exit 1
fi

echo "Client mounted."
echo "Mount: ${MOUNT_DIR}"
echo "PID:   ${PID}"
echo "Log:   ${LOG_FILE}"
echo "Umount: fusermount3 -u ${MOUNT_DIR}  (or umount ${MOUNT_DIR})"
