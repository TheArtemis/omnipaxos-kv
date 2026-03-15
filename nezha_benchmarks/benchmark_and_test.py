#!/usr/bin/env python3
"""
Linearizability tester for nezha_benchmarks.

Usage
-----
    python benchmark_and_test.py [options] <benchmark_folder | all>

Positional argument
    benchmark_folder   Subdirectory name under nezha_benchmarks/
                       (e.g. adaptive_deadline, low_quality, all)
    all                Run every benchmark folder that contains a docker-compose.yml

Options
    --check-only       Skip docker compose; only analyse existing logs/
    --run-only         Run docker compose but skip the analysis step
    --timeout N        Seconds to wait for client containers to finish (default 60)
    --log-level LEVEL  RUST_LOG level passed to docker containers (default: info)
    --no-plots         Skip matplotlib plots even if matplotlib is available
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()

# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class Operation:
    client_id: int
    op_type: str
    key: str
    write_val: Optional[str]
    call_ns: int
    return_ns: int
    result_val: Optional[str]

    def latency_ms(self) -> float:
        return (self.return_ns - self.call_ns) / 1e6

# ── Helpers ────────────────────────────────────────────────────────────────────

def detect_compose_cmd() -> list[str]:
    for candidate in (["docker", "compose"], ["docker-compose"]):
        try:
            subprocess.run(candidate + ["version"], capture_output=True, check=True, timeout=5)
            return candidate
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            pass
    raise RuntimeError("Neither 'docker compose' nor 'docker-compose' is available on PATH.")


def compose_cmd(compose_file: pathlib.Path) -> list[str]:
    return detect_compose_cmd() + ["-f", str(compose_file), "--project-directory", str(compose_file.parent)]

# ── Docker compose runner ──────────────────────────────────────────────────────

def run_compose(compose_file: pathlib.Path, log_level: str, timeout: int) -> bool:
    """
    Start docker compose in detached mode, wait for client containers to exit,
    then bring everything down. Returns True on clean completion.
    """
    compose_dir = compose_file.parent
    logs_dir = compose_dir / "logs"
    logs_dir.mkdir(exist_ok=True)

    base = compose_cmd(compose_file)
    env = {**os.environ, "RUST_LOG": log_level}

    # Build images (only s1 has build: context, others share the image)
    print("  Building images...")
    result = subprocess.run(base + ["build"], env=env, cwd=str(compose_dir))
    if result.returncode != 0:
        print(f"  ⚠  docker compose build failed (exit {result.returncode})")
        return False

    # Start detached
    print("  Starting containers (detached)...")
    subprocess.run(base + ["up", "-d"], env=env, cwd=str(compose_dir), check=True)

    # Discover client container names from compose ps
    client_containers = _find_client_containers(base, compose_dir)
    if not client_containers:
        print("  ⚠  Could not identify client containers; falling back to names 'c1','c2'")
        client_containers = ["c1", "c2"]

    # Poll until all clients have exited
    print(f"  Waiting for clients {client_containers} (timeout={timeout}s)...")
    deadline = time.monotonic() + timeout
    all_done = _wait_for_containers(client_containers, deadline)
    if not all_done:
        print(f"  ⚠  Timeout reached; some clients may not have finished.")

    # Bring everything down gracefully
    print("  Stopping remaining containers...")
    subprocess.run(base + ["down"], cwd=str(compose_dir), timeout=60, check=False)
    return all_done


def _find_client_containers(base: list[str], cwd: pathlib.Path) -> list[str]:
    """Return the names of containers whose service starts with 'c' (clients)."""
    try:
        result = subprocess.run(
            base + ["ps", "--format", "json"],
            capture_output=True, text=True, timeout=10, cwd=str(cwd)
        )
        names = []
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                svc = obj.get("Service", obj.get("Name", ""))
                name = obj.get("Name", obj.get("ID", svc))
                if svc.startswith("c") and svc[1:].isdigit():
                    names.append(name)
            except json.JSONDecodeError:
                pass
        return names
    except Exception:
        return []


def _wait_for_containers(names: list[str], deadline: float) -> bool:
    """Poll until all named containers report 'exited' status (or timeout)."""
    while time.monotonic() < deadline:
        time.sleep(3)
        if all(_container_exited(n) for n in names):
            return True
    return all(_container_exited(n) for n in names)


def _container_exited(name: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", name],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() == "exited"
    except Exception:
        return False

# ── History loading ────────────────────────────────────────────────────────────

def load_history(logs_dir: pathlib.Path) -> list[Operation]:
    import re as _re
    ops: list[Operation] = []
    # Only load canonical per-client files: history-1.json, history-2.json, …
    # Ignore files with non-numeric suffixes (e.g. history-raw-c1.json from
    # older runs) which would mix operations from different experiments.
    _numeric_history = _re.compile(r"^history-\d+\.json$")
    for path in sorted(p for p in logs_dir.glob("history-*.json")
                       if _numeric_history.match(p.name)):
        try:
            with open(path) as f:
                entries = json.load(f)
            for e in entries:
                inp = e["input"]
                out = e.get("output", {})
                ops.append(Operation(
                    client_id=e["client_id"],
                    op_type=inp["type"],
                    key=inp["key"],
                    write_val=inp.get("value"),
                    call_ns=e["call"],
                    return_ns=e["return_time"],
                    result_val=out.get("value"),
                ))
        except Exception as ex:
            print(f"  ⚠  Could not load {path}: {ex}")
    return ops

# ── Linearizability checker ────────────────────────────────────────────────────

def _check_key(key: str, ops: list[Operation]) -> tuple[bool, str]:
    """
    Check linearizability for a single key (single-register model).

    Rules:
    (1) Get → None:  violation if any Put.return_ns ≤ Get.call_ns
                     (a committed write must be visible)
    (2) Get → V:
        (a) There must exist Put(K,V) with return_ns ≤ Get.return_ns.
        (b) If the latest Put(K,anything) that completed before Get.call_ns
            wrote value W ≠ V, then there must also be a Put(K,V) with
            return_ns > W_write.return_ns (V was written after W, so
            the Get could have been linearized after V but before anything
            newer).
    """
    puts = [op for op in ops if op.op_type == "Put"]
    gets = [op for op in ops if op.op_type == "Get"]

    for g in gets:
        rv = g.result_val

        if rv is None:
            # No value returned — must mean the key was absent in this linearization.
            # Violation if a write had fully committed before this get started.
            for p in puts:
                if p.return_ns <= g.call_ns:
                    return False, (
                        f"Key '{key}': Get by client {g.client_id} returned None, "
                        f"but Put({p.write_val!r}) by client {p.client_id} "
                        f"committed at t={p.return_ns:,} before Get started at t={g.call_ns:,}."
                    )
        else:
            # Returned value rv must correspond to a write that could have been
            # linearized before this Get.  Two operations can be ordered A before B
            # iff A.call_ns ≤ B.return_ns (A started before B returned).  Using
            # Put.return_ns ≤ Get.return_ns is too strict: it rejects valid
            # concurrent executions where the Put's ack races the Get's ack but
            # the write was committed (and visible) before the Get was served.
            valid_writes = [p for p in puts if p.write_val == rv and p.call_ns <= g.return_ns]
            if not valid_writes:
                return False, (
                    f"Key '{key}': Get by client {g.client_id} returned {rv!r}, "
                    f"but no Put({rv!r}) started before Get.return_ns={g.return_ns:,}."
                )

            # Check for mandatory overwrite: find the latest Put that finished
            # entirely before this Get was issued.
            pre_gets = [p for p in puts if p.return_ns <= g.call_ns]
            if pre_gets:
                latest_pre = max(pre_gets, key=lambda p: p.return_ns)
                if latest_pre.write_val != rv:
                    # The register must have contained `latest_pre.write_val` when
                    # the Get started — unless rv was written AFTER latest_pre and
                    # before the end of the Get's execution window.
                    later_rv_writes = [
                        p for p in valid_writes
                        if p.return_ns > latest_pre.return_ns
                    ]
                    if not later_rv_writes:
                        return False, (
                            f"Key '{key}': Get by client {g.client_id} returned {rv!r}, "
                            f"but the latest committed write before Get.call_ns={g.call_ns:,} "
                            f"was {latest_pre.write_val!r} (t={latest_pre.return_ns:,}), "
                            f"and no write of {rv!r} occurred after that."
                        )

    return True, "ok"


def check_linearizability(ops: list[Operation]) -> tuple[bool, list[str]]:
    """Check the full history by projecting onto each key independently."""
    by_key: dict[str, list[Operation]] = defaultdict(list)
    for op in ops:
        by_key[op.key].append(op)

    violations: list[str] = []
    for key in sorted(by_key, key=lambda k: (len(k), k)):
        ok, msg = _check_key(key, by_key[key])
        if not ok:
            violations.append(msg)
    return not violations, violations

# ── Metrics ────────────────────────────────────────────────────────────────────

def load_metrics(logs_dir: pathlib.Path) -> Optional[dict]:
    path = logs_dir / "metrics.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

# ── Plotting ───────────────────────────────────────────────────────────────────

def plot_results(
    config_name: str,
    ops: list[Operation],
    metrics: Optional[dict],
    logs_dir: pathlib.Path,
) -> None:
    if not HAS_MATPLOTLIB:
        print("  (matplotlib not available — skipping plots)")
        return
    if not ops and not metrics:
        return

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f"Benchmark: {config_name}", fontsize=14, fontweight="bold")

    # ── Panel 1: Latency CDF ──────────────────────────────────────────────────
    ax = axes[0]
    ax.set_title("End-to-End Latency CDF")
    put_lats = sorted(op.latency_ms() for op in ops if op.op_type == "Put")
    get_lats = sorted(op.latency_ms() for op in ops if op.op_type == "Get")
    for lats, label, color in [
        (put_lats, "Put (write)", "#2196F3"),
        (get_lats, "Get (read)", "#FF5722"),
    ]:
        if not lats:
            continue
        n = len(lats)
        y = [(i + 1) / n for i in range(n)]
        ax.plot(lats, y, label=label, color=color, linewidth=1.5)
        for frac, pname in [(0.50, "p50"), (0.95, "p95"), (0.99, "p99")]:
            v = lats[min(int(frac * n), n - 1)]
            ax.axvline(v, color=color, linestyle="--", alpha=0.4, linewidth=0.8)
            ax.text(v, frac + 0.02, f" {pname}={v:.1f}", fontsize=7, color=color)
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Cumulative fraction")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # ── Panel 2: Latency histogram ────────────────────────────────────────────
    ax = axes[1]
    ax.set_title("Latency Distribution (histogram)")
    all_lats = [op.latency_ms() for op in ops]
    if all_lats:
        bins = min(60, max(20, len(all_lats) // 20))
        ax.hist(put_lats, bins=bins, alpha=0.6, color="#2196F3", label="Put")
        ax.hist(get_lats, bins=bins, alpha=0.6, color="#FF5722", label="Get")
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Count")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # ── Panel 3: Path distribution (from proxy metrics) ───────────────────────
    ax = axes[2]
    if metrics:
        fp = metrics.get("fast_path_committed", 0)
        sp = metrics.get("slow_path_committed", 0)
        aborted = metrics.get("fast_path_aborted", 0)
        avg_fp_ms = metrics.get("avg_fast_path_latency_us", 0) / 1000.0
        avg_sp_ms = metrics.get("avg_slow_path_latency_us", 0) / 1000.0
        tp = metrics.get("avg_throughput_rps", 0.0)
        ax.set_title(f"Proxy metrics  |  throughput ≈ {tp:.0f} rps")
        bars = ax.bar(
            ["Fast-path", "Slow-path", "Aborted"],
            [fp, sp, aborted],
            color=["#4CAF50", "#FF9800", "#F44336"],
        )
        for bar, v in zip(bars, [fp, sp, aborted]):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1,
                str(v),
                ha="center", va="bottom", fontsize=9,
            )
        ax.set_ylabel("Operations committed")
        ax2 = ax.twinx()
        ax2.plot(
            ["Fast-path", "Slow-path"],
            [avg_fp_ms, avg_sp_ms],
            "D--", color="#9C27B0", markersize=8, label="Avg latency (ms)",
        )
        ax2.set_ylabel("Avg latency (ms)", color="#9C27B0")
        ax2.tick_params(axis="y", labelcolor="#9C27B0")
    else:
        ax.set_title("Proxy metrics")
        ax.text(
            0.5, 0.5,
            "No metrics.json\n(proxy telemetry flushed on shutdown;\n"
            "check that the container exited cleanly)",
            ha="center", va="center", transform=ax.transAxes, fontsize=9,
        )

    plt.tight_layout()
    out = logs_dir / "benchmark_results.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot saved → {out}")

# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(
    config_name: str,
    ops: list[Operation],
    metrics: Optional[dict],
    lin_ok: bool,
    violations: list[str],
) -> None:
    puts = [o for o in ops if o.op_type == "Put"]
    gets = [o for o in ops if o.op_type == "Get"]
    lats = sorted(o.latency_ms() for o in ops)
    n = len(lats)

    print(f"\n{'═' * 62}")
    print(f"  Config : {config_name}")
    print(f"  History: {len(ops)} ops  ({len(puts)} Puts / {len(gets)} Gets)")

    if n > 0:
        p50 = lats[int(0.50 * n)]
        p95 = lats[int(0.95 * n)]
        p99 = lats[min(int(0.99 * n), n - 1)]
        print(f"  E2E latency p50/p95/p99 : {p50:.2f} ms / {p95:.2f} ms / {p99:.2f} ms")

    if metrics:
        fp = metrics.get("fast_path_committed", 0)
        sp = metrics.get("slow_path_committed", 0)
        tot = fp + sp or 1
        aborted = metrics.get("fast_path_aborted", 0)
        avg_fp = metrics.get("avg_fast_path_latency_us", 0) / 1000
        avg_sp = metrics.get("avg_slow_path_latency_us", 0) / 1000
        tp = metrics.get("avg_throughput_rps", 0)
        print(
            f"  Fast-path : {fp:6d} ops ({fp / tot * 100:.1f}%)  avg {avg_fp:.2f} ms "
            f"| aborted: {aborted}"
        )
        print(f"  Slow-path : {sp:6d} ops ({sp / tot * 100:.1f}%)  avg {avg_sp:.2f} ms")
        print(f"  Throughput: {tp:.1f} rps")
    else:
        print("  Proxy metrics: not available (metrics.json missing)")

    if not ops:
        print("  Linearizability: SKIPPED  (no history files found)")
    elif lin_ok:
        print("  Linearizability: ✓  PASS")
    else:
        print(f"  Linearizability: ✗  FAIL  ({len(violations)} violation(s))")
        for v in violations[:5]:
            print(f"    • {v}")
        if len(violations) > 5:
            print(f"    … and {len(violations) - 5} more")
    print(f"{'═' * 62}")

# ── Single benchmark runner ────────────────────────────────────────────────────

def run_single(
    config_dir: pathlib.Path,
    do_run: bool,
    do_check: bool,
    timeout: int,
    log_level: str,
    no_plots: bool,
) -> dict:
    config_name = config_dir.name
    compose_file = config_dir / "docker-compose.yml"
    if not compose_file.exists():
        compose_file = config_dir / "docker-compose.yaml"
    if not compose_file.exists():
        print(f"  ✗ No docker-compose file in {config_name}, skipping.")
        return {"config": config_name, "skipped": True}

    logs_dir = config_dir / "logs"

    if do_run:
        print(f"\n  ┌─ Running docker compose for '{config_name}' ─────────────────")
        ok = run_compose(compose_file, log_level=log_level, timeout=timeout)
        if not ok:
            print(f"  ⚠  Benchmark may be incomplete.")

    ops = []
    metrics = None
    lin_ok = True
    violations: list[str] = []

    if do_check:
        if not logs_dir.exists():
            print(f"  ⚠  No logs/ directory found for '{config_name}'.")
        else:
            ops = load_history(logs_dir)
            metrics = load_metrics(logs_dir)

            if not ops:
                print(
                    f"  ⚠  No history-*.json files in {logs_dir}.\n"
                    f"     History is written by the client binary; make sure you ran\n"
                    f"     a docker compose build since the last code change."
                )
            else:
                lin_ok, violations = check_linearizability(ops)

        print_summary(config_name, ops, metrics, lin_ok, violations)

        if not no_plots:
            plot_results(config_name, ops, metrics, logs_dir)

    return {
        "config": config_name,
        "skipped": False,
        "ops": len(ops),
        "lin_ok": lin_ok,
        "violations": len(violations),
        "metrics": metrics,
    }

# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="OmniPaxos-KV benchmark runner and linearizability tester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "target",
        help="Benchmark folder name (e.g. adaptive_deadline) or 'all'",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Skip docker compose; only analyse existing logs/",
    )
    parser.add_argument(
        "--run-only",
        action="store_true",
        help="Run docker compose but skip the analysis step",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Seconds to wait for client containers before forcing shutdown (default: 60)",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        help="RUST_LOG level passed to docker containers (default: info)",
    )
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Suppress matplotlib output",
    )
    args = parser.parse_args()

    if args.check_only and args.run_only:
        parser.error("--check-only and --run-only are mutually exclusive.")

    do_run = not args.check_only
    do_check = not args.run_only

    if args.target == "all":
        configs = sorted(
            d for d in SCRIPT_DIR.iterdir()
            if d.is_dir() and (d / "docker-compose.yml").exists()
        )
    else:
        target_path = SCRIPT_DIR / args.target
        if not target_path.is_dir():
            print(f"Error: no such benchmark directory: {target_path}", file=sys.stderr)
            sys.exit(1)
        configs = [target_path]

    print(f"OmniPaxos-KV Benchmark & Linearizability Test")
    print(f"  Configs : {', '.join(c.name for c in configs)}")
    print(f"  Mode    : {'run+check' if do_run and do_check else 'run-only' if do_run else 'check-only'}")
    print(f"  Timeout : {args.timeout}s")

    results = []
    for cfg in configs:
        print(f"\n{'─' * 62}")
        print(f"▶  {cfg.name}")
        results.append(
            run_single(
                cfg,
                do_run=do_run,
                do_check=do_check,
                timeout=args.timeout,
                log_level=args.log_level,
                no_plots=args.no_plots,
            )
        )

    # ── Cross-config summary ────────────────────────────────────────────────
    real = [r for r in results if not r.get("skipped")]
    if len(real) > 1:
        print(f"\n{'═' * 62}")
        print(f"GLOBAL SUMMARY  ({len(real)} benchmarks)")
        print(f"{'═' * 62}")
        passed = sum(1 for r in real if r.get("lin_ok", True))
        for r in real:
            if r.get("skipped"):
                continue
            lin = "✓ PASS" if r.get("lin_ok", True) else f"✗ FAIL ({r['violations']})"
            tp_str = ""
            if r.get("metrics"):
                tp_str = f"  tp≈{r['metrics'].get('avg_throughput_rps', 0):.0f}rps"
            print(f"  {r['config']:<30s}  {lin}  {r['ops']} ops{tp_str}")
        print(f"\nLinearizability: {passed}/{len(real)} passed")
        if passed < len(real):
            sys.exit(1)


if __name__ == "__main__":
    main()
