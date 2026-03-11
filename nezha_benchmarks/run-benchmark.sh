#!/usr/bin/env bash

set -euo pipefail

print_usage() {
  cat <<EOF
Usage: $0 [--run] [--log-level <level>|--info|--debug] <benchmark-folder>

Run docker compose for a benchmark folder under nezha_benchmarks.

Positional arguments:
  <benchmark-folder>   Name of the folder (e.g. test_clock_skew, low_quality)

Options:
  --run                Skip docker image build step and only run docker compose up
  --log-level <level>  Set RUST_LOG for all services (e.g. info, debug, trace)
  --info               Shortcut for --log-level info
  --debug              Shortcut for --log-level debug
                       (default log level is debug)
  -h, --help           Show this help message and exit
EOF
}

BENCHMARK_FOLDER=""
SKIP_BUILD=0
LOG_LEVEL="debug"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run)
      SKIP_BUILD=1
      shift
      ;;
    --log-level)
      LOG_LEVEL="${2:-}"
      if [[ -z "${LOG_LEVEL}" ]]; then
        echo "Error: --log-level requires a value (e.g. info, debug)."
        echo
        print_usage
        exit 1
      fi
      shift 2
      ;;
    --info)
      LOG_LEVEL="info"
      shift
      ;;
    --debug)
      LOG_LEVEL="debug"
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      if [[ -z "$BENCHMARK_FOLDER" ]]; then
        BENCHMARK_FOLDER="$1"
      else
        echo "Unknown argument: $1"
        echo
        print_usage
        exit 1
      fi
      shift
      ;;
  esac
done

if [[ -z "${BENCHMARK_FOLDER}" ]]; then
  echo "Error: benchmark folder name is required."
  echo
  print_usage
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${SCRIPT_DIR}/${BENCHMARK_FOLDER}"

if [[ ! -d "${TARGET_DIR}" ]]; then
  echo "Error: benchmark folder '${BENCHMARK_FOLDER}' does not exist under nezha_benchmarks."
  exit 1
fi

COMPOSE_FILE=""
if [[ -f "${TARGET_DIR}/docker-compose.yml" ]]; then
  COMPOSE_FILE="${TARGET_DIR}/docker-compose.yml"
elif [[ -f "${TARGET_DIR}/docker-compose.yaml" ]]; then
  COMPOSE_FILE="${TARGET_DIR}/docker-compose.yaml"
else
  echo "Error: no docker-compose.yml or docker-compose.yaml found in '${BENCHMARK_FOLDER}'."
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE_CMD=(docker compose -f "${COMPOSE_FILE}")
elif docker-compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE_CMD=(docker-compose -f "${COMPOSE_FILE}")
else
  echo "Error: neither 'docker compose' nor 'docker-compose' is available."
  exit 1
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo "Building images for benchmark '${BENCHMARK_FOLDER}'..."
  "${DOCKER_COMPOSE_CMD[@]}" build
fi

echo "Running docker compose for benchmark '${BENCHMARK_FOLDER}'..."
echo "Using log level: ${LOG_LEVEL}"
RUST_LOG="${LOG_LEVEL}" "${DOCKER_COMPOSE_CMD[@]}" up

