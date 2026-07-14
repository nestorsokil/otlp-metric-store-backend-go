# OTLP Metric Storage (Go)

A gRPC service that receives OTLP metrics (Gauge and Sum) and stores them in ClickHouse, split
into a **series lookup table** (`otel_series`) plus **skinny datapoint tables**
(`otel_datapoints_gauge`/`otel_datapoints_sum`), so a series' identity is stored once and its
datapoints stay minimal. Built for high-throughput ingest where the same series recurs across
millions of datapoints.

The original task definition is preserved in [ASSIGNMENT.md](ASSIGNMENT.md).

## Usage

Requires a reachable ClickHouse instance (native protocol, default `localhost:9000`), configured
via flags (`-clickhouseAddr`, `-clickhouseDatabase`, `-clickhouseUsername`, `-clickhousePassword`).

```shell
make build            # go build ./...
make run              # go run .
make test             # unit tests
make test-integration # integration tests against a real ClickHouse (testcontainers + Docker)
make test-all
```

Integration tests logs `debug` by default to see `Export` detail (datapoint counts, series emitted vs. deduped). Additionally it's possible to see stdout metrics and traces provided by setting `OTEL_DEBUG=1`.

## Design

Full reasoning — requirements, design (schema, canonical query, alternatives considered),
task breakdown — lives in [specs/001-series-datapoint-split/](specs/001-series-datapoint-split/).
This section captures only the decisions worth knowing up front:

- **SeriesId** is a deterministic `xxHash64` (seed 0) over a **length-prefixed** encoding of the
  series identity — not delimiter-separated, since attribute keys/values are arbitrary
  user-controlled UTF-8 and a naive `k=v,k=v` join collides deterministically when a value
  contains `,` or `=`. See `seriesID` in `metrics_mapper.go`.
- **Write path** collapses same-series datapoints in a batch to one candidate, gates each through
  an in-process dedup cache (`series_cache.go`, `ShouldEmit`/`MarkEmitted` split so a failed
  insert never suppresses the client's retry), and writes series-first with
  `async_insert=1`+`wait_for_async_insert=1` — throughput without sacrificing a durable ack.
- **Read path** (`MetricsQuerier.QueryDatapoints`, internal only — no network exposure) resolves
  `SeriesId`s from `otel_series`, then range-scans datapoints by `SeriesId` + time. Partitioning by
  day plus the primary key guarantees no full table scan; verified in the integration suite via
  `EXPLAIN indexes = 1`.
- **Out of scope**: Histogram/Exponential Histogram/Summary metric types, migration from the
  legacy wide tables (runbook only, in `4-migration.md`), retention/TTL, and dedup-cache
  warm-read on startup.

[specs/roadmap.md](specs/roadmap.md) tracks these and other potential follow-ups as future
features/specs.

## References

- [OpenTelemetry Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [OpenTelemetry Protocol (OTLP)](https://github.com/open-telemetry/opentelemetry-proto)

This feature was implemented using the [sdd](https://github.com/nestorsokil/sdd) Claude Code
skill — a spec-driven development workflow (requirements → design → tasks, reviewed before code).
