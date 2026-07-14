# Series / Datapoint Split — Tasks
> Status: draft

`Review:` tracks self-review state — `—` (not implemented), `pending` (shipped, review deferred),
`passed` (reviewed clean).

## Prerequisites
- Repo is already under git; create a feature branch off `main`.
- ClickHouse via testcontainers already wired in `integration_test.go` (reuse).
- Dependencies to add: `github.com/cespare/xxhash/v2`, `github.com/hashicorp/golang-lru/v2`.

## Tasks

- [ ] [medium] **1. Schema: series + skinny datapoint tables** — replace the 5 wide-table DDLs with
  `createSeriesTableSQL` (`otel_series`, **`ReplacingMergeTree(LastSeen)`**, plain columns, bloom
  indexes on resource/datapoint attribute maps, **no TTL**) and
  `createGaugeTableSQL`/`createSumTableSQL` creating `otel_metrics_gauge_datapoints` /
  `otel_metrics_sum_datapoints` as **`ReplacingMergeTree`** `ORDER BY (SeriesId, TimeUnix)`,
  `PARTITION BY toDate(TimeUnix)`, no TTL. Update `CreateTables`.
  (`clickhouse_schema.go`, `clickhouse_client.go`)
  - Spec: design §Schema, AC-1 AC-4 AC-7, C-2
  - Review: —
- [ ] [medium] **2. SeriesId hasher + row types** — implement `seriesID(...)` to the **normative
  length-prefixed encoding** (design §SeriesId canonical encoding): `lp(s) = len(s) ":" s` (byte
  length), maps as `lp(concat of lp(k)+lp(v) over bytewise-sorted keys)`, exact field order,
  `xxHash64` seed 0. Define `SeriesRow` + skinny `GaugeRow`/`SumRow`. Unit-test: same identity → same
  id; different identity → different id; map key-order independence; **and that values containing
  `,`, `=`, and control bytes do not collide** (the delimiter bug this encoding exists to prevent).
  (`metrics_mapper.go`, `clickhouse_client.go`)
  - Spec: design §SeriesId canonical encoding, AC-2
  - Review: —
- [ ] [medium] **3. Series dedup cache** — `series_cache.go` on `hashicorp/golang-lru/v2/expirable`.
  **`ShouldEmit(id, now) bool` decides only; `MarkEmitted(id, now)` records — never merged.** Config
  with pinned defaults (`refreshInterval=5m`, `maxEntries=100_000`, `idleTTL=1h`), env-overridable.
  Unit-test: first sight emits; re-sight before `MarkEmitted` still emits (a failed insert must not
  suppress the retry); after `MarkEmitted`, suppressed until `refreshInterval` elapses; eviction
  bounds size; concurrent access safe. (`series_cache.go`)
  - Spec: design §Series dedup cache, AC-2 AC-5, C-3, C-4
  - Review: —
- [ ] [medium] **4. Store: skinny rows + InsertSeries** — make `GaugeRow`/`SumRow` skinny (SeriesId +
  timestamps + value), add `InsertSeries` to `MetricsStore`, keep `InsertGauge`/`InsertSum`. Batch
  inserts with `async_insert = 1` **and `wait_for_async_insert = 1`** (durable ack; stops reads racing
  the flush). Update `MapGaugeRows`/`MapSumRows` to emit skinny rows + `SeriesRow`, setting
  `FirstSeen`/`LastSeen` to **ingest time (wall clock at emit), not the datapoint's `TimeUnix`** —
  `LastSeen` is the `ReplacingMergeTree` version column, and an event-time version lets a
  future-dated/backfilled datapoint win permanently over later emits (design §Schema).
  **BREAKING: row structs change shape.** (`clickhouse_client.go`, `metrics_mapper.go`)
  - Spec: design §MetricsStore §Schema, AC-1 AC-4
  - Review: —
- [ ] [small] **5. Wire Export handler** — compute SeriesIds, gate series rows via `ShouldEmit`,
  `InsertSeries` **then `MarkEmitted` only on success**, then insert datapoints. Add
  `series_registered_total` / `series_cache_size`. (`metrics_service.go`, `otel.go`)
  - Spec: design §Export handler §Data flow §Metrics, AC-1 AC-5
  - Review: —
- [ ] [medium] **6. Query interface** — `MetricsQuerier` with `DatapointQuery`/`Datapoint` and
  `QueryDatapoints(ctx, q) ([]Datapoint, truncated bool, err error)` on `ClickHouseMetricsStore`.
  Build the query dynamically: `From`/`To` + `MetricType` required (the latter selects the table, it
  is not a filter); **every filter emits its clause only when set** (C-2). Use
  **`SELECT DISTINCT`** in the series subquery — an `INNER JOIN` *multiplies* by duplicate unmerged
  right-side rows rather than absorbing them. Attribute matching via `mapContains(m,k) AND m[k]=v`.
  `ORDER BY (SeriesId, TimeUnix)`, `LIMIT 1 BY (SeriesId, TimeUnix)`, `LIMIT` (default 10_000) with a
  `truncated` flag. No `FINAL`. (`metrics_query.go`, `clickhouse_client.go`)
  - Spec: design §MetricsQuerier §Canonical read query, AC-3 AC-6, C-2
  - Review: —
- [ ] [medium] **7. Read-path e2e integration test** — rewrite the suite for the new schema: send
  Gauge+Sum via gRPC, assert retrieval **through `QueryDatapoints`** (no raw SQL in the test). Cover:
  (a) time-frame + `MetricType` only → all services/metrics of that type (AC-6); (b) optional filters
  narrow correctly; (c) `otel_series` holds one row per series after repeated datapoints — assert via
  **`SELECT count() FROM otel_series FINAL`** (or `count(DISTINCT SeriesId)`), since `ReplacingMergeTree`
  dedups lazily and a bare `count()` would flake; test-side `FINAL` is not a contradiction of the
  no-`FINAL` read path (AC-2);
  (d) **replaying the same `Export` does not double-count** (AC-7); (e) **no full table scan** —
  `EXPLAIN indexes = 1` shows partition + primary-index pruning (AC-3, C-2). Replace the old
  wide-column assertions. (`integration_test.go`, `server_test.go`)
  - Spec: AC-1 AC-2 AC-3 AC-6 AC-7, C-2
  - Review: —
- [ ] [small] **8. README** — document the split, series-vs-datapoint terminology, the SeriesId
  reference + canonical encoding, the no-full-scan query pattern, the `MetricsQuerier` interface, and
  throughput choices (dedup cache, async_insert). Note what is deliberately out of scope (other metric
  types, migration, retention). Move the assignment brief to `ASSIGNMENT.md`, write a new `README.md`.
  (`README.md`, `ASSIGNMENT.md`)
  - Spec: AC-3 AC-6, NG-1
  - Review: —

## Cut for scope (documented, not built)
- Migration MV + its ClickHouse hash-parity expression (`4-migration.md` is a runbook, not code).
- Retention/TTL on either table.
- `AggregatingMergeTree`/`SimpleAggregateFunction` on `otel_series` (nothing reads `FirstSeen`).

## Verification
`go build ./...` clean. `go test ./...` (unit) green. `go test -tags integration ./...`: Gauge+Sum
over gRPC → `QueryDatapoints` with only a time-frame + `MetricType` returns datapoints from all
services (no raw SQL in the test); filters narrow correctly; a replayed `Export` does not
double-count; `otel_series` holds one row per series; `EXPLAIN indexes = 1` confirms pruning.
