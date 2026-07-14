# Series / Datapoint Split — Tasks
> Status: draft

## Prerequisites
- `git init` (repo is not yet under version control) and create a feature branch.
- ClickHouse via testcontainers already wired in `integration_test.go` (reuse).
- Add `github.com/cespare/xxhash/v2` dependency for the hasher.

## Tasks

### Walking skeleton (chunks 1+2: write split, proven by read) — the 4h core
- [ ] [medium] **1. Schema: series + skinny datapoint tables** — replace the 5 wide-table DDLs
  with `createSeriesTableSQL` (`otel_series`, AggregatingMergeTree with min FirstSeen / max LastSeen,
  series-level constants as columns,
  bloom indexes) and skinny `createGaugeTableSQL`/`createSumTableSQL` creating
  `otel_metrics_gauge_datapoints`/`otel_metrics_sum_datapoints` (`ORDER BY (SeriesId, TimeUnix)`,
  partition by day; distinct from legacy wide names). Update `CreateTables`. (`clickhouse_schema.go`, `clickhouse_client.go`)
  - Spec: design §Schema §Migration, AC-1 AC-4, C-2
  - Review: —
- [ ] [small] **2. SeriesId hasher + row types** — add `seriesID(...)` over a canonical sorted
  encoding; define `SeriesRow` and skinny `GaugeRow`/`SumRow`; unit-test determinism (same identity
  → same id, different identity → different id, key-order independence). (`metrics_mapper.go`, `clickhouse_client.go`)
  - Spec: design §SeriesId hasher §Glossary, AC-2
  - Review: —
- [ ] [medium] **3. Series dedup cache** — `series_cache.go`: concurrent LRU + TTL, `ShouldEmit`
  with `refreshInterval`. Unit-test: first sight emits, immediate re-sight suppressed, post-interval
  re-emits, eviction bounds size, concurrent access safe. (`series_cache.go`)
  - Spec: design §Series dedup cache, AC-2 AC-5, C-3
  - Review: —
- [ ] [medium] **4. Store: skinny GaugeRow/SumRow + InsertSeries** — make `GaugeRow`/`SumRow` skinny
  (SeriesId + timestamps + value), add `InsertSeries` to the `MetricsStore` interface, keep
  `InsertGauge`/`InsertSum`; batch inserts with `async_insert` on datapoints. Update
  `MapGaugeRows`/`MapSumRows` to emit skinny rows + `SeriesRow` (series-level constants onto the
  series row). **BREAKING: row structs change shape.** (`clickhouse_client.go`, `metrics_mapper.go`)
  - Spec: design §MetricsStore, AC-1 AC-4
  - Review: —
- [ ] [small] **5. Wire Export handler** — compute SeriesIds, gate the series row via the cache,
  insert series-first then datapoints. Add `series_registered_total` / `series_cache_size` metrics.
  (`metrics_service.go`, `otel.go`)
  - Spec: design §Export handler §Metrics, AC-1 AC-5
  - Review: —
- [ ] [medium] **6. Read-path e2e integration test** — rewrite integration suite for the new
  schema: send Gauge+Sum via gRPC, then assert retrieval via the two-step join query (joining
  `otel_series` to `otel_metrics_gauge_datapoints`) filtered by `ServiceName` + time-frame; assert
  `otel_series` has one row per series after repeated datapoints (AC-2). Replace the old
  `otel_metrics_gauge`/`_sum` wide-column assertions. (`integration_test.go`, `server_test.go`)
  - Spec: AC-1 AC-2 AC-3, C-2
  - Review: —
- [ ] [small] **7. README: schema rationale + canonical query** — document the split, the series
  vs datapoint terminology (glossary), the SeriesId reference, the no-full-scan query pattern, and
  throughput choices (dedup cache, async_insert). Move the existing README.md to README_original.md and create a new `README.md` with the above content. (`README.md`)
  - Spec: AC-3, NG-1
  - Review: —

## Verification
`go build ./...` clean. `go test ./...` (unit) green. `go test -tags integration ./...`:
send Gauge+Sum over gRPC → the two-step time-bounded join returns the datapoints, and `otel_series`
holds exactly one active row per series after repeated ingest. README documents the query.
