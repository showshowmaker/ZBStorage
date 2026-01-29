#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BUILD_DIR="${ROOT_DIR}/build"

if [[ ! -x "${BUILD_DIR}/real_node_server" ]]; then
  echo "real_node_server not found at ${BUILD_DIR}. Build first." >&2
  exit 1
fi
if [[ ! -x "${BUILD_DIR}/real_node_client" ]]; then
  echo "real_node_client not found at ${BUILD_DIR}. Build first." >&2
  exit 1
fi

TMP_DIR=$(mktemp -d)
DISK_DIR="${TMP_DIR}/disk1"
mkdir -p "${DISK_DIR}"
echo "disk-01" > "${DISK_DIR}/.disk_id"

CONFIG_FILE="${TMP_DIR}/real_node.conf"
cat > "${CONFIG_FILE}" <<EOF
ZB_DISKS=disk-01:${DISK_DIR}
EOF

PORT=8000
"${BUILD_DIR}/real_node_server" --config="${CONFIG_FILE}" --port=${PORT} &
SERVER_PID=$!

sleep 1

"${BUILD_DIR}/real_node_client" \
  --server=127.0.0.1:${PORT} \
  --disk_id=disk-01 \
  --chunk_id=550e8400-e29b-41d4-a716-446655440000 \
  --write_data=hello \
  --read_size=5 \
  --mode=both

kill ${SERVER_PID}
wait ${SERVER_PID} 2>/dev/null || true

rm -rf "${TMP_DIR}"
