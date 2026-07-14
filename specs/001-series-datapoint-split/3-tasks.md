# Series / Datapoint Split — Tasks
> Status: draft

`Review:` on each task tracks self-review state — `—` (not implemented), `pending` (shipped,
review deferred), `passed` (self-review ran clean).

## Prerequisites
- Repo is already under git; create a feature branch off `main`.
- ClickHouse via testcontainers already wired in `integration_test.go` (reuse).
- Dependencies to add: `github.com/cespare/xxhash/v2` (hasher),
  `github.com/hashicorp/golang-lru/v2` (expirable LRU for the dedup cache).

## Tasks

### Walking skeleton (chunks 1+2: write split, proven by read)
- [ ] [medium] **1. Schema: series + skinny datapoint tables** — replace the 5 wide-table DDLs with
  `createSeriesTableSQL` (`otel_series`, `AggregatingMergeTree`, `SimpleAggregateFunction` on every
  non-key column, `min FirstSeen`/`max LastSeen`, bloom indexes, `TTL LastSeen + INTERVAL 90 DAY`)
  and skinny `createGaugeTableSQL`/`createSumTableSQL` creating
  `otel_metrics_gauge_datapoints`/`otel_metrics_sum_datapoints` as **`ReplacingMergeTree`**
  `ORDER BY (SeriesId, TimeUnix)`, partitioned by day, distinct from legacy wide names.
  Update `CreateTables`. (`clickhouse_schema.go`, `clickhouse_client.go`)
  - Spec: design §Schema §Migration, AC-1 AC-4 AC-7, C-2
  - Review: —
- [ ] [medium] **2. SeriesId hasher + row types** — implement `seriesID(...)` to the **normative
  canonical encoding** (design §SeriesId canonical encoding): sorted map keys, exact field order,
  `\x1f` separator, `xxHash64` seed 0. Define `SeriesRow` and skinny `GaugeRow`/`SumRow`. Unit-test
  determinism (same identity → same id; different identity → different id; map key-order
  independence) **and a fixture test asserting Go's `seriesID` equals ClickHouse's `xxHash64` on the
  same canonical string** — the migration MV depends on this parity.
  (`metrics_mapper.go`, `clickhouse_client.go`)
  - Spec: design §SeriesId canonical encoding, AC-2; risk "Go/ClickHouse hash divergence"
  - Review: —
- [ ] [medium] **3. Series dedup cache** — `series_cache.go`: `hashicorp/golang-lru/v2/expirable`,
  `ShouldEmit(id, now)`, config with pinned defaults (`refreshInterval=5m`, `maxEntries=100_000`,
  `idleTTL=1h`), env-overridable. Unit-test: first sight emits, immediate re-sight suppressed,
  post-interval re-emits, eviction bounds size, concurrent access safe. (`series_cache.go`)
  - Spec: design §Series dedup cache, AC-2 AC-5, C-3
  - Review: —
- [ ] [medium] **4. Store: skinny GaugeRow/SumRow + InsertSeries** — make `GaugeRow`/`SumRow` skinny
  (SeriesId + timestamps + value), add `InsertSeries` to the `MetricsStore` interface, keep
  `InsertGauge`/`InsertSum`. Batch inserts with `async_insert = 1` **and `wait_for_async_insert = 1`**
  (durable ack; also stops reads racing the flush). Update `MapGaugeRows`/`MapSumRows` to emit skinny
  rows + `SeriesRow`, with `FirstSeen`/`LastSeen` derived from the datapoints' **event time**
  (`TimeUnix`), not wall clock. **BREAKING: row structs change shape.**
  (`clickhouse_client.go`, `metrics_mapper.go`)
  - Spec: design §MetricsStore §Schema, AC-1 AC-4
  - Review: —
- [ ] [small] **5. Wire Export handler** — compute SeriesIds, gate the series row via the cache,
  insert series-first then datapoints. Add `series_registered_total` / `series_cache_size` metrics.
  (`metrics_service.go`, `otel.go`)
  - Spec: design §Export handler §Metrics, AC-1 AC-5
  - Review: —
- [ ] [medium] **6. Query interface** — add `MetricsQuerier` with `DatapointQuery`/`Datapoint` types
  and `QueryDatapoints` on `ClickHouseMetricsStore`. Build the canonical query dynamically:
  `From`/`To` and `MetricType` are required (the latter selects the table, it is not a filter);
  **every filter emits its clause only when set** (C-2). Attribute matching uses
  `mapContains(m, k) AND m[k] = v` (a bare `m[k] = ''` would match rows *lacking* the key). Support
  `MetricName` and `ResourceAttributes` filters; apply `Limit` (default 10_000) and an explicit
  `ORDER BY`; `LIMIT 1 BY (SeriesId, TimeUnix)` to collapse pre-merge Replacing duplicates. No
  `FINAL`. Integration-test: query with **only a time-frame + type** returns all services (AC-6);
  each optional filter narrows correctly. (`metrics_query.go`, `clickhouse_client.go`)
  - Spec: design §MetricsQuerier §Canonical read query, AC-3 AC-6, C-2
  - Review: —
- [ ] [medium] **7. Read-path e2e integration test** — rewrite the integration suite for the new
  schema: send Gauge+Sum via gRPC, then assert retrieval **through `MetricsQuerier.QueryDatapoints`**
  (no raw SQL in the test). Cover: (a) a query with only a time-frame + `MetricType` returns all
  services/metrics of that type (AC-6);
  (b) `otel_series` holds one row per series after repeated datapoints (AC-2); (c) **re-sending the
  same `Export` does not double-count** values (AC-7); (d) **no full table scan** — assert partition +
  primary-index pruning via `EXPLAIN indexes = 1` or `read_rows` ≪ total rows from
  `system.query_log` (AC-3, C-2). Replace the old wide-column assertions.
  (`integration_test.go`, `server_test.go`)
  - Spec: AC-1 AC-2 AC-3 AC-6 AC-7, C-2
  - Review: —
- [ ] [small] **8. README: schema rationale + canonical query** — document the split, the series vs
  datapoint terminology (glossary), the SeriesId reference + canonical encoding, the no-full-scan
  query pattern, the `MetricsQuerier` interface, and throughput choices (dedup cache, async_insert).
  Move the assignment brief to `ASSIGNMENT.md` and write a new `README.md` with the above.
  (`README.md`, `ASSIGNMENT.md`)
  - Spec: AC-3 AC-6, NG-1
  - Review: —

## Verification
`go build ./...` clean. `go test ./...` (unit) green — including the Go↔ClickHouse hash-parity
fixture. `go test -tags integration ./...`: send Gauge+Sum over gRPC → `QueryDatapoints` with **only a
time-frame + `MetricType`** returns datapoints from all services (no raw SQL in the test); optional
filters narrow correctly; a replayed `Export` does not double-count; `otel_series` holds one row per series;
and `EXPLAIN indexes = 1` confirms partition + primary-index pruning (no full scan). README documents
the query and the interface.
