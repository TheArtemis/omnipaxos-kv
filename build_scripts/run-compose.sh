#!/usr/bin/env bash

set -euo pipefail

print_usage() {
  cat <<EOF
Usage: $0 [--run] [--log-level <level>|--info|--debug]

Build and/or run the local docker-compose setup in build_scripts.

Options:
  --run                Skip docker image build step and only run docker compose up
  --log-level <level>  Set RUST_LOG for all services (e.g. info, debug, trace)
  --info               Shortcut for --log-level info
  --debug              Shortcut for --log-level debug (default)
  -h, --help           Show this help message and exit
EOF
}

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
      echo "Unknown argument: $1"
      echo
      print_usage
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "Error: docker-compose.yml not found in build_scripts."
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
  echo "Building images for local cluster..."
  "${DOCKER_COMPOSE_CMD[@]}" build
fi

echo "Running docker compose for local cluster..."
echo "Using log level: ${LOG_LEVEL}"
RUST_LOG="${LOG_LEVEL}" "${DOCKER_COMPOSE_CMD[@]}" up

