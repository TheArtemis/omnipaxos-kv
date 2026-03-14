#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VERIFIER_DIR="${REPO_ROOT}/verifier"

print_usage() {
  cat <<EOF
Usage: $0 [--run] [--log-level <level>|--info|--debug] <benchmark-folder>

Run docker compose for a benchmark folder, then start the linearizability verifier
with --serve on history-local-1.json and history-local-2.json from that run's logs.

Positional arguments:
  <benchmark-folder>   Name of the folder (e.g. test_clock_skew, test_node_failure, low_quality)

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
ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run)
      ARGS+=("$1")
      shift
      ;;
    --log-level)
      ARGS+=("$1" "${2:-}")
      if [[ -z "${2:-}" ]]; then
        echo "Error: --log-level requires a value (e.g. info, debug)."
        echo
        print_usage
        exit 1
      fi
      shift 2
      ;;
    --info)
      ARGS+=("$1")
      shift
      ;;
    --debug)
      ARGS+=("$1")
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      if [[ -z "${BENCHMARK_FOLDER}" ]]; then
        BENCHMARK_FOLDER="$1"
        ARGS+=("$1")
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

LOGS_DIR="${SCRIPT_DIR}/${BENCHMARK_FOLDER}/logs"
H1="${LOGS_DIR}/history-local-1.json"
H2="${LOGS_DIR}/history-local-2.json"

# Run the benchmark (reuse run-benchmark.sh)
echo "=== Running benchmark: ${BENCHMARK_FOLDER} ==="
"${SCRIPT_DIR}/run-benchmark.sh" "${ARGS[@]}"
BENCHMARK_EXIT=$?

if [[ ${BENCHMARK_EXIT} -ne 0 ]]; then
  echo "Benchmark exited with code ${BENCHMARK_EXIT}; skipping verifier."
  exit "${BENCHMARK_EXIT}"
fi

if [[ ! -f "${H1}" ]] || [[ ! -f "${H2}" ]]; then
  echo "Error: history files not found. Expected: ${H1} and ${H2}"
  exit 1
fi

# Fix permissions on all nezha_benchmarks log folders so the verifier can write merged-history.json (Docker may create them as root)
for log_dir in "${SCRIPT_DIR}"/*/logs; do
  if [[ -d "${log_dir}" ]] && ! [[ -w "${log_dir}" ]]; then
    echo "Fixing permissions on ${log_dir} (may prompt for sudo)..."
    sudo chown -R "$(whoami):" "${log_dir}" 2>/dev/null || true
  fi
done
if [[ -d "${LOGS_DIR}" ]]; then
  chmod -R u+rwX "${LOGS_DIR}" 2>/dev/null || true
fi

echo ""
echo "=== Starting linearizability verifier (--serve) ==="
echo "History files: ${H1}, ${H2}"
echo "Open http://localhost:8080 in your browser."
echo ""

cd "${VERIFIER_DIR}"
go run ./cmd/verifier --serve "${H1}" "${H2}"
