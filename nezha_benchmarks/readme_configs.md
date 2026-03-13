# Nezha Benchmark Config Reference

This file documents the configuration keys used by the benchmark folders (server, proxy and client config files).

## Server configs (`server-*.toml`)
- `location`: Human-readable node name.
- `server_id`: Integer ID of this server.
- `num_clients`: Number of direct clients expected.
- `listen_address`: IP address to bind the server.
- `listen_port`: Port to listen on.
- `proxy_address`: Address of the proxy (host:port).
- `use_proxy`: `true`/`false` — whether the server should expect traffic via proxy.
- `adaptive_deadline`: `true`/`false` — enable adaptive deadline logic.
- `default_deadline`: Default deadline (ms) used when adaptive is disabled.
- `failure_injection`: `true`/`false` — enable injected failures (these benchmarks set this to `false`).
- `failure_probability`: Float `[0.0-1.0]` probability of a failure event.
- `failure_check_interval_ms`: Interval (ms) between failure checks.
- `failure_downtime_ms`: Downtime duration (ms) for injected failures.
- `failure_max_events`: Integer maximum number of injected failure events.
- `output_filepath`: Path where server writes JSON logs.

### `[owd]` (optional)
- `default_value`: Default one-way delay value.
- `percentile`: Percentile used for owd computations.
- `window_size`: Window size for owd calculations.

### `[clock]`
- `drift_rate`: Clock drift rate (units used by benchmark).
- `uncertainty_bound`: Clock uncertainty bound (ms).
- `sync_freq`: Clock sync frequency (ms).

## Proxy configs (`proxy-config.toml`)
- `cluster_name`: Name for this proxy cluster.
- `nodes`: Array of server IDs in the cluster.
- `node_addrs`: Array of `host:port` addresses for servers.
- `proxy_listen_address`: Address the proxy binds to.
- `proxy_listen_port`: Port the proxy listens on.
- `metrics_filepath`: Where proxy writes metrics JSON.
- `telemetry`: `no` / `yes` or telemetry level. if 'yes' telemetry is real time on the json file
- `[clock]`: Same clock keys as server configs.

## Client configs (`client-*-config.toml`)
- `location`: Human-readable client name.
- `server_id`: ID of server to connect to (when not using proxy).
- `server_address`: `host:port` of target server.
- `proxy_address`: `host:port` of proxy.
- `use_proxy`: `true`/`false` — whether client sends via proxy.
- `summary_filepath`: Path for client summary JSON.
- `output_filepath`: Path for client request traces (CSV/JSON).
- `[[requests]]`: Sequence of request phases with keys:
  - `duration_sec`: Phase duration in seconds.
  - `requests_per_sec`: Target request rate for the phase.
  - `read_ratio`: Fraction of requests that are reads (0.0–1.0).


These parameters are used across the benchmark folders (e.g. `low_quality`, `medium_quality`, `high_quality`, `test_clock_skew`, `adaptive_deadline`). The server configs in these folders have been updated to disable failure injection and set failure probability/downtime/max events to zero.
