## Nezha Benchmarks

This directory contains local Nezha-based benchmark setups, each in its own subfolder
with a `docker-compose.yml` describing the benchmark topology.

### Run a benchmark

Use the `run-benchmark.sh` helper script to build and run a benchmark:

```bash
cd nezha_benchmarks

# Build images and run the benchmark in test_clock_skew
./run-benchmark.sh test_clock_skew

# Same, but for a different benchmark folder
./run-benchmark.sh low_quality
```

By default, the script:

- Builds the Docker images referenced in the benchmark's `docker-compose.yml`.
- Then starts the benchmark using `docker compose up` (or `docker-compose up`).

### Run without rebuilding images

To skip the build step and only run the containers, pass `--run`:

```bash
cd nezha_benchmarks

./run-benchmark.sh --run test_clock_skew
```

### Control log level

The default log level is **debug**. You can override the `RUST_LOG` level used by the services:

```bash
cd nezha_benchmarks

# Explicit level
./run-benchmark.sh --log-level info test_clock_skew
./run-benchmark.sh --log-level debug test_clock_skew

# Shortcuts
./run-benchmark.sh --info test_clock_skew
./run-benchmark.sh --debug test_clock_skew
```

### Help message

Show usage information with:

```bash
cd nezha_benchmarks

./run-benchmark.sh -h
```

