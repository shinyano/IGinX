#!/usr/bin/env bash
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)"
CONFIG_FILE="${CONFIG_FILE:-${ROOT_DIR}/conf/config.properties}"
RUNNER_CLASS="cn.edu.tsinghua.iginx.tools.benchmark.PythonUdfThroughputBenchmarkRunner"
JAVA_CMD="${JAVA_CMD:-java}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6888}"
USER_NAME="${USER_NAME:-root}"
PASSWORD="${PASSWORD:-root}"
PORTS_TO_CLEAN="${PORTS_TO_CLEAN:-${PORT} 7888}"
CONDA_BASE="${CONDA_BASE:-}"
GIL_CONDA_ENV="${GIL_CONDA_ENV:-py313_std}"
FT_CONDA_ENV="${FT_CONDA_ENV:-py313_ft}"
MODES="${MODES:-gil ft}"
THREADS="${THREADS:-1 2 4 8}"
ROUNDS="${ROUNDS:-3}"
ROWS="${ROWS:-10000}"
COLS="${COLS:-10}"
LOOPS="${LOOPS:-20}"
INVOCATIONS_PER_THREAD="${INVOCATIONS_PER_THREAD:-10}"
WARMUP="${WARMUP:-1}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-90}"
SHUTDOWN_TIMEOUT_SECONDS="${SHUTDOWN_TIMEOUT_SECONDS:-30}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-2}"
PORT_KILL_TIMEOUT_SECONDS="${PORT_KILL_TIMEOUT_SECONDS:-10}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/benchmark/results}"
UDF_DIR="${UDF_DIR:-${ROOT_DIR}/benchmark/udf}"
START_CMD="${START_CMD:-}"
STOP_CMD="${STOP_CMD:-}"
PID_FILE="${OUT_DIR}/iginx.pid"
RAW_CSV="${OUT_DIR}/raw-results.csv"
CONFIG_BACKUP="${OUT_DIR}/config.properties.bak"
ORIGINAL_LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"

mkdir -p "${OUT_DIR}" "${OUT_DIR}/logs"
cp "${CONFIG_FILE}" "${CONFIG_BACKUP}"

if [[ -n "${START_CMD}" && -z "${STOP_CMD}" ]]; then
  echo "STOP_CMD is required when START_CMD is provided" >&2
  exit 1
fi

restore_config() {
  if [[ -f "${CONFIG_BACKUP}" ]]; then
    cp "${CONFIG_BACKUP}" "${CONFIG_FILE}"
  fi
}

cleanup() {
  restore_config
}

trap cleanup EXIT

resolve_conda_base() {
  if [[ -n "${CONDA_BASE}" ]]; then
    printf '%s' "${CONDA_BASE}"
    return 0
  fi

  if [[ -n "${CONDA_EXE:-}" ]]; then
    printf '%s' "$(cd -- "$(dirname -- "${CONDA_EXE}")"/.. && pwd)"
    return 0
  fi

  if command -v conda >/dev/null 2>&1; then
    conda info --base
    return 0
  fi

  echo "Unable to resolve CONDA_BASE. Set CONDA_BASE or GIL_PYTHON/FT_PYTHON explicitly." >&2
  return 1
}

resolve_conda_env_python() {
  local env_name="$1"
  local conda_base
  conda_base="$(resolve_conda_base)"
  local python_exec="${conda_base}/envs/${env_name}/bin/python"
  if [[ ! -x "${python_exec}" ]]; then
    echo "Conda env python not found: ${python_exec}" >&2
    return 1
  fi
  printf '%s' "${python_exec}"
}

escape_sed_value() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//&/\\&}"
  value="${value//|/\\|}"
  printf '%s' "${value}"
}

update_property() {
  local key="$1"
  local value="$2"
  local tmp_file
  tmp_file="$(mktemp)"
  awk -v key="${key}" -v value="${value}" '
    BEGIN {
      replaced = 0
      pattern = "^[[:space:]]*#?[[:space:]]*" key "[[:space:]]*="
    }
    $0 ~ pattern {
      if (!replaced) {
        print key "=" value
        replaced = 1
      }
      next
    }
    {
      print
    }
    END {
      if (!replaced) {
        print key "=" value
      }
    }
  ' "${CONFIG_FILE}" > "${tmp_file}"
  chmod --reference="${CONFIG_FILE}" "${tmp_file}"
  mv "${tmp_file}" "${CONFIG_FILE}"
}

wait_for_port_state() {
  local expected="$1"
  local deadline=$((SECONDS + $2))
  while (( SECONDS < deadline )); do
    if (echo >"/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1; then
      if [[ "${expected}" == "open" ]]; then
        return 0
      fi
    else
      if [[ "${expected}" == "closed" ]]; then
        return 0
      fi
    fi
    sleep 1
  done
  echo "port ${HOST}:${PORT} did not become ${expected} in time" >&2
  return 1
}

list_pids_for_port() {
  local target_port="$1"
  if ! command -v ss >/dev/null 2>&1; then
    return 0
  fi
  ss -ltnp 2>/dev/null \
    | awk -v suffix=":${target_port}" '$4 ~ suffix "$" {print}' \
    | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' \
    | sort -u
}

is_port_listened() {
  local target_port="$1"
  if ! command -v ss >/dev/null 2>&1; then
    return 1
  fi
  ss -ltnp 2>/dev/null | awk -v suffix=":${target_port}" '$4 ~ suffix "$" {found=1} END {exit found ? 0 : 1}'
}

kill_pids_gracefully() {
  local pids="$1"
  local signal_name="$2"
  if [[ -z "${pids}" ]]; then
    return 0
  fi
  while IFS= read -r pid; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "-${signal_name}" "${pid}" >/dev/null 2>&1 || true
    fi
  done <<< "${pids}"
}

cleanup_stale_ports() {
  local ports=(${PORTS_TO_CLEAN})
  local port
  for port in "${ports[@]}"; do
    local pids
    pids="$(list_pids_for_port "${port}")"
    if [[ -z "${pids}" ]]; then
      continue
    fi
    echo "[cleanup] killing stale listeners on port ${port}: ${pids}"
    kill_pids_gracefully "${pids}" TERM

    local deadline=$((SECONDS + PORT_KILL_TIMEOUT_SECONDS))
    while (( SECONDS < deadline )); do
      if ! is_port_listened "${port}"; then
        break
      fi
      sleep 1
    done

    if is_port_listened "${port}"; then
      local force_pids
      force_pids="$(list_pids_for_port "${port}")"
      if [[ -n "${force_pids}" ]]; then
        echo "[cleanup] force killing listeners on port ${port}: ${force_pids}"
        kill_pids_gracefully "${force_pids}" KILL
      fi
    fi
  done
}

build_runtime_ld_library_path() {
  local python_exec="$1"
  local python_bin_dir
  python_bin_dir="$(cd -- "$(dirname -- "${python_exec}")" && pwd)"
  local env_root
  env_root="$(cd -- "${python_bin_dir}"/.. && pwd)"
  local env_lib_dir="${env_root}/lib"
  if [[ -d "${env_lib_dir}" ]]; then
    if [[ -n "${ORIGINAL_LD_LIBRARY_PATH}" ]]; then
      printf '%s:%s' "${env_lib_dir}" "${ORIGINAL_LD_LIBRARY_PATH}"
    else
      printf '%s' "${env_lib_dir}"
    fi
  else
    printf '%s' "${ORIGINAL_LD_LIBRARY_PATH}"
  fi
}

start_iginx() {
  local log_file="$1"
  local python_exec="$2"
  local runtime_ld_library_path
  update_property "pythonCMD" "${python_exec}"
  echo "[debug] config_file=${CONFIG_FILE}"
  echo "[debug] pythonCMD target=${python_exec}"
  sed -n '/^[[:space:]]*pythonCMD[[:space:]]*=/p' "${CONFIG_FILE}"
  runtime_ld_library_path="$(build_runtime_ld_library_path "${python_exec}")"
  cleanup_stale_ports
  rm -f "${PID_FILE}"
  if [[ -n "${START_CMD}" ]]; then
    LD_LIBRARY_PATH="${runtime_ld_library_path}" bash -lc "${START_CMD}" >"${log_file}" 2>&1 &
  else
    LD_LIBRARY_PATH="${runtime_ld_library_path}" bash "${ROOT_DIR}/sbin/start_iginx.sh" >"${log_file}" 2>&1 &
  fi
  echo $! > "${PID_FILE}"
  wait_for_port_state open "${STARTUP_TIMEOUT_SECONDS}"
}

stop_iginx() {
  if [[ -n "${STOP_CMD}" ]]; then
    bash -lc "${STOP_CMD}" || true
  elif [[ -f "${PID_FILE}" ]]; then
    kill "$(cat "${PID_FILE}")" >/dev/null 2>&1 || true
  fi
  wait_for_port_state closed "${SHUTDOWN_TIMEOUT_SECONDS}" || true
  cleanup_stale_ports
  rm -f "${PID_FILE}"
}

select_python() {
  local mode="$1"
  case "${mode}" in
    gil)
      if [[ -n "${GIL_PYTHON:-}" ]]; then
        printf '%s' "${GIL_PYTHON}"
      else
        resolve_conda_env_python "${GIL_CONDA_ENV}"
      fi
      ;;
    ft)
      if [[ -n "${FT_PYTHON:-}" ]]; then
        printf '%s' "${FT_PYTHON}"
      else
        resolve_conda_env_python "${FT_CONDA_ENV}"
      fi
      ;;
    *)
      echo "Unsupported mode: ${mode}" >&2
      exit 1
      ;;
  esac
}

verify_python_runtime() {
  local python_exec="$1"
  local runtime_ld_library_path
  runtime_ld_library_path="$(build_runtime_ld_library_path "${python_exec}")"
  LD_LIBRARY_PATH="${runtime_ld_library_path}" "${python_exec}" - <<'PY'
import glob
import os
import sys
import sysconfig
import traceback

errors = []

for package in ("numpy", "pandas", "iginx_udf"):
    try:
        __import__(package)
    except Exception:
        errors.append(
            f"cannot import {package}:\n"
            + "".join(traceback.format_exception(*sys.exc_info())).rstrip()
        )

purelib = sysconfig.get_paths().get("purelib", "")
platlib = sysconfig.get_paths().get("platlib", "")
search_roots = [path for path in (purelib, platlib) if path]

legacy_candidates = []
split_candidates = []
for root in search_roots:
    legacy_candidates.extend(glob.glob(os.path.join(root, "_pemja*.so")))
    split_candidates.extend(glob.glob(os.path.join(root, "pemja_utils*.so")))
    split_candidates.extend(glob.glob(os.path.join(root, "pemja_core*.so")))

if legacy_candidates:
    try:
        import _pemja as pemja_module
        extension_file = getattr(pemja_module, "__file__", "")
        if not extension_file:
            errors.append("_pemja imported but __file__ is empty")
        elif not os.path.exists(extension_file):
            errors.append(f"_pemja extension file does not exist: {extension_file}")
    except Exception as exc:
        errors.append(f"cannot import native _pemja extension: {exc}")
elif split_candidates:
    has_utils = any(os.path.basename(path).startswith("pemja_utils") for path in split_candidates)
    has_core = any(os.path.basename(path).startswith("pemja_core") for path in split_candidates)
    if not has_utils:
        errors.append("pemja_utils native library is missing")
    if not has_core:
        errors.append("pemja_core native library is missing")
else:
    errors.append(
        "no native pemja library found under purelib/platlib; expected _pemja*.so or pemja_utils/pemja_core*.so"
    )

if errors:
    print("Python runtime validation failed:", file=sys.stderr)
    for item in errors:
        print(f"  - {item}", file=sys.stderr)
    sys.exit(1)

print(f"python_ok={sys.executable}")
print(f"ld_library_path={os.environ.get('LD_LIBRARY_PATH', '')}")
print(f"purelib={purelib}")
print(f"platlib={platlib}")
if legacy_candidates:
    print("native_candidates=" + ",".join(sorted(os.path.basename(path) for path in legacy_candidates)))
if split_candidates:
    print("native_candidates=" + ",".join(sorted(os.path.basename(path) for path in split_candidates)))
PY
}

run_single_case() {
  local mode="$1"
  local round_id="$2"
  local threads="$3"
  local python_exec="$4"
  local log_file="${OUT_DIR}/logs/${mode}-t${threads}-r${round_id}.log"

  update_property "pythonCMD" "${python_exec}"
  update_property "udfPoolEnabled" "true"
  update_property "udfPoolMinThreads" "${threads}"
  update_property "udfPoolMaxThreads" "${threads}"
  update_property "udfPoolInitialThreads" "${threads}"

  echo "[run] mode=${mode} threads=${threads} round=${round_id} python=${python_exec}"
  start_iginx "${log_file}" "${python_exec}"
  "${JAVA_CMD}" -cp "${ROOT_DIR}/lib/*" "${RUNNER_CLASS}" \
    --host "${HOST}" \
    --port "${PORT}" \
    --user "${USER_NAME}" \
    --password "${PASSWORD}" \
    --mode "${mode}" \
    --round "${round_id}" \
    --threads "${threads}" \
    --rows "${ROWS}" \
    --cols "${COLS}" \
    --loops "${LOOPS}" \
    --invocations-per-thread "${INVOCATIONS_PER_THREAD}" \
    --warmup "${WARMUP}" \
    --timeout-seconds "${TIMEOUT_SECONDS}" \
    --python-exec "${python_exec}" \
    --udf-dir "${UDF_DIR}" \
    --output-csv "${RAW_CSV}"
  stop_iginx
  sleep "${COOLDOWN_SECONDS}"
}

for mode in ${MODES}; do
  python_exec="$(select_python "${mode}")"
  if [[ ! -x "${python_exec}" ]]; then
    echo "Python executable is not runnable: ${python_exec}" >&2
    exit 1
  fi
  verify_python_runtime "${python_exec}"
  for threads in ${THREADS}; do
    for round_id in $(seq 1 "${ROUNDS}"); do
      run_single_case "${mode}" "${round_id}" "${threads}" "${python_exec}"
    done
  done
done

echo "[done] raw results written to ${RAW_CSV}"
