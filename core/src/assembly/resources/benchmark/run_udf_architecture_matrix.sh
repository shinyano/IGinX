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
RUNNER_CLASS="cn.edu.tsinghua.iginx.tools.benchmark.AdaptiveUdfArchitectureBenchmarkRunner"
JAVA_CMD="${JAVA_CMD:-java}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6888}"
USER_NAME="${USER_NAME:-root}"
PASSWORD="${PASSWORD:-root}"
PORTS_TO_CLEAN="${PORTS_TO_CLEAN:-${PORT} 7888}"

CONDA_BASE="${CONDA_BASE:-}"
PYTHON_CONDA_ENV="${PYTHON_CONDA_ENV:-py313_std}"
PYTHON_EXEC="${PYTHON_EXEC:-}"

ARCHITECTURES="${ARCHITECTURES:-legacy adaptive}"
CONCURRENCIES="${CONCURRENCIES:-4 8 16 32 64}"
ROUNDS="${ROUNDS:-3}"

ROWS="${ROWS:-10000}"
COLS="${COLS:-10}"
LOOPS="${LOOPS:-20}"
WARMUP="${WARMUP:-1}"
STEADY_INVOCATIONS_PER_THREAD="${STEADY_INVOCATIONS_PER_THREAD:-10}"
MIXED_DURATION_SECONDS="${MIXED_DURATION_SECONDS:-30}"
LIGHT_QUERY_CONCURRENCY="${LIGHT_QUERY_CONCURRENCY:-2}"
LIGHT_QUERY_LIMIT="${LIGHT_QUERY_LIMIT:-256}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"

ADAPTIVE_INITIAL_THREADS="${ADAPTIVE_INITIAL_THREADS:-4}"
ADAPTIVE_MIN_THREADS="${ADAPTIVE_MIN_THREADS:-4}"
ADAPTIVE_MAX_THREADS="${ADAPTIVE_MAX_THREADS:-16}"

STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-90}"
SHUTDOWN_TIMEOUT_SECONDS="${SHUTDOWN_TIMEOUT_SECONDS:-30}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-2}"
PORT_KILL_TIMEOUT_SECONDS="${PORT_KILL_TIMEOUT_SECONDS:-10}"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/benchmark/results/architecture}"
UDF_DIR="${UDF_DIR:-${ROOT_DIR}/benchmark/udf}"
START_CMD="${START_CMD:-}"
STOP_CMD="${STOP_CMD:-}"
PID_FILE="${OUT_DIR}/iginx.pid"
THROUGHPUT_CSV="${OUT_DIR}/raw-throughput.csv"
LATENCY_CSV="${OUT_DIR}/raw-latency-samples.csv"
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

  echo "Unable to resolve CONDA_BASE. Set CONDA_BASE or PYTHON_EXEC explicitly." >&2
  return 1
}

resolve_python_exec() {
  if [[ -n "${PYTHON_EXEC}" ]]; then
    printf '%s' "${PYTHON_EXEC}"
    return 0
  fi

  local conda_base
  conda_base="$(resolve_conda_base)"
  local python_exec="${conda_base}/envs/${PYTHON_CONDA_ENV}/bin/python"
  if [[ ! -x "${python_exec}" ]]; then
    echo "Conda env python not found: ${python_exec}" >&2
    return 1
  fi
  printf '%s' "${python_exec}"
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

verify_python_runtime() {
  local python_exec="$1"
  local runtime_ld_library_path
  runtime_ld_library_path="$(build_runtime_ld_library_path "${python_exec}")"
  LD_LIBRARY_PATH="${runtime_ld_library_path}" "${python_exec}" - <<'PY'
import sys
import traceback

errors = []
for package in ("pandas", "iginx_udf"):
    try:
        __import__(package)
    except Exception:
        errors.append(
            f"cannot import {package}:\n"
            + "".join(traceback.format_exception(*sys.exc_info())).rstrip()
        )

if errors:
    print("Python runtime validation failed:", file=sys.stderr)
    for item in errors:
        print(f"  - {item}", file=sys.stderr)
    sys.exit(1)

print(f"python_ok={sys.executable}")
print(f"gil_enabled={int(getattr(sys, '_is_gil_enabled', lambda: True)())}")
PY
}

start_iginx() {
  local log_file="$1"
  local python_exec="$2"
  local runtime_ld_library_path
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

apply_architecture_config() {
  local architecture="$1"
  update_property "pythonCMD" "${PYTHON_EXECUTABLE}"
  case "${architecture}" in
    legacy)
      update_property "udfPoolEnabled" "false"
      ;;
    adaptive)
      update_property "udfPoolEnabled" "true"
      update_property "udfPoolInitialThreads" "${ADAPTIVE_INITIAL_THREADS}"
      update_property "udfPoolMinThreads" "${ADAPTIVE_MIN_THREADS}"
      update_property "udfPoolMaxThreads" "${ADAPTIVE_MAX_THREADS}"
      ;;
    *)
      echo "Unsupported architecture: ${architecture}" >&2
      exit 1
      ;;
  esac
}

run_single_case() {
  local architecture="$1"
  local round_id="$2"
  local concurrency="$3"
  local log_file="${OUT_DIR}/logs/${architecture}-c${concurrency}-r${round_id}.log"

  apply_architecture_config "${architecture}"

  echo "[run] architecture=${architecture} concurrency=${concurrency} round=${round_id}"
  start_iginx "${log_file}" "${PYTHON_EXECUTABLE}"
  "${JAVA_CMD}" -cp "${ROOT_DIR}/lib/*" "${RUNNER_CLASS}" \
    --host "${HOST}" \
    --port "${PORT}" \
    --user "${USER_NAME}" \
    --password "${PASSWORD}" \
    --architecture "${architecture}" \
    --round "${round_id}" \
    --concurrency "${concurrency}" \
    --rows "${ROWS}" \
    --cols "${COLS}" \
    --loops "${LOOPS}" \
    --initial-threads "${ADAPTIVE_INITIAL_THREADS}" \
    --min-threads "${ADAPTIVE_MIN_THREADS}" \
    --max-threads "${ADAPTIVE_MAX_THREADS}" \
    --steady-invocations-per-thread "${STEADY_INVOCATIONS_PER_THREAD}" \
    --warmup "${WARMUP}" \
    --mixed-duration-seconds "${MIXED_DURATION_SECONDS}" \
    --light-query-concurrency "${LIGHT_QUERY_CONCURRENCY}" \
    --light-query-limit "${LIGHT_QUERY_LIMIT}" \
    --timeout-seconds "${TIMEOUT_SECONDS}" \
    --python-exec "${PYTHON_EXECUTABLE}" \
    --udf-dir "${UDF_DIR}" \
    --throughput-csv "${THROUGHPUT_CSV}" \
    --latency-csv "${LATENCY_CSV}"
  stop_iginx
  sleep "${COOLDOWN_SECONDS}"
}

PYTHON_EXECUTABLE="$(resolve_python_exec)"
if [[ ! -x "${PYTHON_EXECUTABLE}" ]]; then
  echo "Python executable is not runnable: ${PYTHON_EXECUTABLE}" >&2
  exit 1
fi

verify_python_runtime "${PYTHON_EXECUTABLE}"

for architecture in ${ARCHITECTURES}; do
  for concurrency in ${CONCURRENCIES}; do
    for round_id in $(seq 1 "${ROUNDS}"); do
      run_single_case "${architecture}" "${round_id}" "${concurrency}"
    done
  done
done

echo "[done] throughput csv written to ${THROUGHPUT_CSV}"
echo "[done] latency csv written to ${LATENCY_CSV}"
