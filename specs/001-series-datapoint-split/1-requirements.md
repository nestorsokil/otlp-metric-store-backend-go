# Series / Datapoint Split — Requirements
> Status: draft

## Overview
Today every datapoint row carries the full metric identity (resource attributes, scope,
metric name/description/unit, datapoint attributes) repeated on every row, in a wide table
per metric type. This feature splits storage into a **series lookup table** (one row per unique
series identity) and **skinny datapoint tables** (`value + timestamp + SeriesId reference`), so a
series' identity is stored once and its datapoints stay minimal. Target: high-throughput telemetry
ingest where the same series recurs across millions of datapoints.

## Behavior
- **AC-1** Given an OTLP `Export` with Gauge and/or Sum metrics, when received, then each
  datapoint is stored in a skinny datapoint table referencing a `SeriesId`, and the series identity
  is stored once in the shared series lookup table.
- **AC-2** Given the same series appearing in many datapoints/batches, when ingested, then the
  series table holds exactly one active row per series (deduped), not one per datapoint.
- **AC-3** Given a stored series, when queried with a time-frame (and any optional filters), then its
  datapoints are retrievable via a two-step join (resolve SeriesId from the series table →
  range-scan datapoints) **without a full table scan** — demonstrated by asserting partition +
  primary-index pruning (`EXPLAIN indexes = 1`, or `read_rows` ≪ total rows via `system.query_log`).
- **AC-4** Given series-level constants (`AggregationTemporality`, `IsMonotonic`, `MetricType`,
  description, unit), when stored, then they live on the series row, not repeated per datapoint.
- **AC-5** Given a cold start (empty dedup cache), when ingest resumes, then correctness holds
  (no lost/duplicated series) — at most a transient burst of idempotent series re-inserts.
- **AC-6** Given a query through the `MetricsQuerier` interface, when only the time-frame and
  `MetricType` are set and every filter is left unset, then it returns datapoints across **all**
  services/metrics of that type — the time-frame is the only mandatory *filter* (C-2); `MetricType`
  is a table selector, not a filter. Optional filters (service, metric, attributes) narrow the result
  when set; unset filters emit no SQL clause. Callers never write SQL.
- **AC-7** Given an `Export` that is retried by the client (the routine OTLP response to a timeout or
  `UNAVAILABLE`), when the same batch is ingested twice, then values are **not double-counted** —
  repeated `(SeriesId, TimeUnix)` datapoints collapse, and repeated series rows merge.

## Constraints
- **C-1** Compiles with standard Go SDK, compatible with Go 1.26.
- **C-2** All data partitioned/ordered so queries filtered by time-frame never require a full
  table scan; time-frame is the only mandatory query filter.
- **C-3** Must sustain high write throughput — series-table writes must not scale with datapoint
  count (dedup off the hot path).
- **C-4** No cross-table transaction is assumed; consistency is eventual and correctness must
  not depend on atomic dual writes (SeriesId is content-derived, so a series row is reconstructable).
- **C-5** Series cardinality assumed low, but resource/attributes churn over time — the design
  must tolerate new series appearing and old ones going silent.

## Non-goals
- **NG-1** No *external* query API (gRPC/HTTP read endpoint). An internal `MetricsQuerier` interface
  provides programmatic reads (backing tests and any future endpoint), but nothing is exposed over
  the network in this feature.
- **NG-2** Only Gauge and Sum are in scope. Histogram, Exponential Histogram, and Summary ingestion
  are **deferred to separate future feature(s)** — no DDL, mappers, or tasks for them here. The
  shared series table and skinny-table pattern are designed to accommodate them later.
- **NG-3** No horizontal scaling / multi-instance coordination work beyond noting it stays correct.
- **NG-4** No series-cache warm-read on startup. Cold-start correctness is required (AC-5), but the
  warm-read *optimization* (pre-populating the dedup cache from ClickHouse to avoid the restart
  burst) is **deferred to a separate future feature**.
