# Benchmark Results Summary

This file summarizes the benchmark results used in the report for the Nezha-style, clock-assisted OmniPaxos-KV experiments. All configurations use 3 servers, 2 clients, and 1 proxy.

## adaptive_deadline

- **History**: 364 operations (274 puts / 90 gets)
- **E2E latency (p50 / p95 / p99)**: 4.46 ms / 9.71 ms / 15.52 ms
- **Fast path**: 283 ops (77.7%), avg 3.74 ms, aborted: 0
- **Slow path**: 81 ops (22.3%), avg 5.00 ms
- **Throughput**: 17.1 rps
- **Linearizability**: PASS

## high_quality

- **History**: 362 operations (180 puts / 182 gets)
- **E2E latency (p50 / p95 / p99)**: 4.26 ms / 7.60 ms / 10.07 ms
- **Fast path**: 302 ops (83.4%), avg 3.38 ms, aborted: 0
- **Slow path**: 60 ops (16.6%), avg 3.89 ms
- **Throughput**: 17.9 rps
- **Linearizability**: PASS

## low_quality

- **History**: 363 operations (271 puts / 92 gets)
- **E2E latency (p50 / p95 / p99)**: 5.96 ms / 13.63 ms / 15.31 ms
- **Fast path**: 253 ops (69.7%), avg 4.64 ms, aborted: 0
- **Slow path**: 110 ops (30.3%), avg 7.86 ms
- **Throughput**: 17.3 rps
- **Linearizability**: PASS

## medium_quality

- **History**: 362 operations (274 puts / 88 gets)
- **E2E latency (p50 / p95 / p99)**: 3.88 ms / 8.14 ms / 14.24 ms
- **Fast path**: 288 ops (79.6%), avg 3.13 ms, aborted: 0
- **Slow path**: 74 ops (20.4%), avg 5.12 ms
- **Throughput**: 18.9 rps
- **Linearizability**: PASS

## test_clock_skew

- **History**: 362 operations (264 puts / 98 gets)
- **E2E latency (p50 / p95 / p99)**: 4.60 ms / 6.46 ms / 13.88 ms
- **Fast path**: 186 ops (51.4%), avg 4.00 ms, aborted: 0
- **Slow path**: 176 ops (48.6%), avg 4.69 ms
- **Throughput**: 19.5 rps
- **Linearizability**: PASS

## test_node_failure

- **History**: 402 operations (304 puts / 98 gets)
- **E2E latency (p50 / p95 / p99)**: 9.78 ms / 2014.19 ms / 2814.55 ms
- **Fast path**: 8 ops (2.0%), avg 5.51 ms, aborted: 56
- **Slow path**: 394 ops (98.0%), avg 246.07 ms
- **Throughput**: 16.8 rps
- **Linearizability**: PASS

