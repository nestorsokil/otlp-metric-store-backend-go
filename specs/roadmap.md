# OTLP Metric Storage — Roadmap
> Status: draft

## Vision
Ingest OTLP metrics over gRPC and store them in ClickHouse so that each metric series' identity
is stored once in a series lookup table while its datapoints stay skinny (`value + timestamp +
SeriesId reference`). Optimized for high-throughput telemetry: queries always carry a time-frame
and never require a full table scan.

## Scope & non-goals
- **In scope**: the series/datapoint split for all OTLP metric types; efficient, no-full-scan
  storage and retrieval; operability (signals, validation) for a production deployment.
- **Out of scope**: a product query API surface (reads validated by tests + documented query
  patterns); alerting/dashboards; multi-instance coordination.

## Project constraints
- **PC-1** Compiles with standard Go SDK, compatible with Go 1.26.
- **PC-2** ClickHouse is the storage engine; schema must guarantee no full table scans when
  queried by time-frame (the only mandatory filter).
- **PC-3** High write throughput — series-table writes must not scale with datapoint count.
- **PC-4** No cross-table transactions assumed; SeriesId is content-derived so a series row is
  reconstructable and consistency is eventual.

## Features

| NNN | Feature | Purpose | Depends on | Status |
|-----|---------|---------|------------|--------|
| 001 | series-datapoint-split | Split Gauge+Sum storage into a shared series table + skinny datapoint tables, proven end-to-end | — | specced |
| 002 | other-metric-types | Extend the split to Histogram, Exponential Histogram, Summary | 001 | planned |
| 003 | series-cache-warmup | Pre-populate the dedup cache from ClickHouse on startup to avoid the restart re-insert burst | 001 | planned |
| 004 | metrics-self-observability | Export the app's own OTel metrics via OTLP back into its own ingest path, self-stored + queryable via MetricsQuerier | 001 | planned |

Status: `planned` → `specced` (spec set approved) → `implemented` (all tasks shipped).

## Sequencing notes
- **001 is the foundation** — it establishes the SeriesId hash, the shared series table, the
  dedup cache, and the skinny-table + two-step read pattern. Everything else builds on it.
- **MVP boundary**: 001 alone is a shippable slice (Gauge+Sum, the two most common types).
- 002, 003, 004 depend only on 001 and are independent of each other. 002 is user-facing coverage
  (more metric types); 003 is an operability optimization (startup burst reduction); 004 is
  self-observability (dogfood the app's own metrics through its ingest path).
- The 001 → existing-deployment migration runbook lives inside 001 itself
  (`001-series-datapoint-split/4-migration.md`), not as its own feature — it's the rollout plan for
  001, only relevant on a brownfield (existing wide-table) deployment.

## Per-feature detail

### 002 — other-metric-types
- Scope sketch: `MapHistogramRows`/`MapSummaryRows`/`MapExponentialHistogramRows` → skinny
  per-shape datapoint tables (`otel_metrics_histogram`, `_exp_histogram`, `_summary`) referencing
  the shared `otel_series` table; extend the Export handler and integration suite. Series-level constants
  (temporality, bounds definitions) go on the lookup row, per the 001 pattern.
- Touches: `metrics_mapper.go`, `clickhouse_schema.go`, `clickhouse_client.go`, `metrics_service.go`.

### 003 — series-cache-warmup
- Scope sketch: on startup, `SELECT SeriesId, LastSeen FROM otel_series` to seed the dedup
  cache, avoiding a burst of idempotent series re-inserts after a restart.
- Notable: only worth building if the restart burst is observed to matter at real cardinality.

### 004 — metrics-self-observability
- Scope sketch: add an OTLP metric exporter on the app's `MeterProvider` (`otel.go`) targeting its
  own gRPC ingest endpoint (keep stdout for debug). The app's own metrics then self-store in
  `otel_series` + datapoints and are queryable via `MetricsQuerier`, tagged
  `service.name = otlp-metrics-processor-backend` so they're filterable from real ingest.
- Caveats to design for: **self-referential loop** (exporting metrics is itself an `Export` →
  bounded/steady since export cadence is fixed, not volume-driven); **startup ordering** (start the
  self-exporter after ingest + ClickHouse are ready; early exports retry/drop).
