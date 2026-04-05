#!/usr/bin/env bash
set -euo pipefail

resolve_demo_root() {
  local root_dir="$1"
  local config_file="${CONFIG_BASE_FILE:-${root_dir}/config/base.conf}"
  local configured_root=""
  local raw_line=""

  if [[ -f "${config_file}" ]]; then
    while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
      local line="${raw_line%%#*}"
      line="$(printf '%s' "${line}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
      [[ -z "${line}" ]] && continue
      if [[ "${line}" == ROOT_PATH=* ]]; then
        configured_root="${line#ROOT_PATH=}"
        configured_root="$(printf '%s' "${configured_root}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
        break
      fi
    done < "${config_file}"
  fi

  if [[ -n "${configured_root}" ]]; then
    if [[ "${configured_root}" != /* ]]; then
      configured_root="${root_dir}/${configured_root}"
    fi
    printf '%s\n' "${configured_root}"
  else
    printf '%s\n' "${root_dir}/.demo_run"
  fi
}
