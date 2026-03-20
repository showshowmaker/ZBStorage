#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
CMAKE_GENERATOR="${CMAKE_GENERATOR:-}"
JOBS="${JOBS:-}"
MODE="${1:-build}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

pick_generator() {
  if [[ -n "${CMAKE_GENERATOR}" ]]; then
    echo "${CMAKE_GENERATOR}"
    return
  fi
  if command -v ninja >/dev/null 2>&1; then
    echo "Ninja"
    return
  fi
  echo ""
}

configure_project() {
  local generator
  generator="$(pick_generator)"

  mkdir -p "${BUILD_DIR}"

  local cmd=(cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}")
  if [[ -n "${generator}" ]]; then
    cmd+=(-G "${generator}")
  fi

  log "[CONFIGURE] ${cmd[*]}"
  "${cmd[@]}"
}

build_project() {
  local cmd=(cmake --build "${BUILD_DIR}" --config "${BUILD_TYPE}")
  if [[ -n "${JOBS}" ]]; then
    cmd+=(--parallel "${JOBS}")
  else
    cmd+=(--parallel)
  fi

  log "[BUILD] ${cmd[*]}"
  "${cmd[@]}"
}

clean_build_dir() {
  if [[ -d "${BUILD_DIR}" ]]; then
    log "[CLEAN] removing ${BUILD_DIR}"
    rm -rf "${BUILD_DIR}"
  fi
}

print_usage() {
  cat <<EOF
Usage: $0 [build|configure|reconfigure|clean]

Environment variables:
  BUILD_DIR         Build directory, default: ${ROOT_DIR}/build
  BUILD_TYPE        CMake build type, default: Release
  CMAKE_GENERATOR   Optional CMake generator, auto-picks Ninja when available
  JOBS              Parallel job count for cmake --build
EOF
}

case "${MODE}" in
  build)
    configure_project
    build_project
    ;;
  configure)
    configure_project
    ;;
  reconfigure)
    clean_build_dir
    configure_project
    build_project
    ;;
  clean)
    clean_build_dir
    ;;
  -h|--help|help)
    print_usage
    ;;
  *)
    print_usage
    exit 1
    ;;
esac
