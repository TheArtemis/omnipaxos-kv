#!/usr/bin/env python3
"""
Refresh benchmark result artifacts for the report.

Copies benchmark_results.png (and optionally metrics.json) from
../nezha_benchmarks/<config>/logs/ into documents/data/<config>/.

Usage:
    python refresh_data.py              # refresh all known configs
    python refresh_data.py low_quality  # refresh specific configs
    python refresh_data.py adaptive_deadline high_quality
"""

import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = REPO_ROOT / "documents"
DATA_DIR = DOCS_DIR / "data"
BENCH_ROOT = REPO_ROOT / "nezha_benchmarks"

KNOWN_CONFIGS = [
    "adaptive_deadline",
    "high_quality",
    "medium_quality",
    "low_quality",
    "test_clock_skew",
    "test_node_failure",
]


def refresh_config(config: str) -> None:
    if config not in KNOWN_CONFIGS:
        print(f"[skip] {config}: not in known configs {KNOWN_CONFIGS}")
        return

    src_logs = BENCH_ROOT / config / "logs"
    dst_dir = DATA_DIR / config
    dst_dir.mkdir(parents=True, exist_ok=True)

    src_png = src_logs / "benchmark_results.png"
    if src_png.is_file():
        dst_png = dst_dir / "benchmark_results.png"
        shutil.copy2(src_png, dst_png)
        print(f"[ok]   {config}: copied benchmark_results.png")
    else:
        print(f"[warn] {config}: benchmark_results.png not found at {src_png}")

    # Optionally copy metrics.json if present
    src_metrics = src_logs / "metrics.json"
    if src_metrics.is_file():
        dst_metrics = dst_dir / "metrics.json"
        shutil.copy2(src_metrics, dst_metrics)
        print(f"[ok]   {config}: copied metrics.json")
    else:
        print(f"[info] {config}: no metrics.json found (skipping)")


def main(argv: list[str]) -> None:
    if len(argv) > 1:
        configs = argv[1:]
    else:
        configs = KNOWN_CONFIGS

    print(f"Repository root: {REPO_ROOT}")
    print(f"Benchmark root:  {BENCH_ROOT}")
    print(f"Data dir:        {DATA_DIR}")
    print()

    for cfg in configs:
        refresh_config(cfg)


if __name__ == "__main__":
    main(sys.argv)

