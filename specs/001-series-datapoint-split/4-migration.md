# Wide-Table → Series/Datapoint Migration — Runbook
> Status: draft

## Context
Rollout plan for this feature (001) when applied to an **existing** deployment (already writing the
legacy wide `otel_metrics_gauge`/`otel_metrics_sum` tables), moving it onto the new series/datapoint
schema with **no producer changes and no in-app dual-write logic**. For a greenfield deploy this is
N/A — just create the new schema forward-only (task 1). Assumes the 001 code (new-app image) is built.

## Strategy
A ClickHouse **Materialized View bridges the old write path**: old apps keep inserting fat rows
unchanged; an MV mirrors each new insert into the new tables; a one-time backfill covers history;
the new app writes the new tables directly. Two representations stay in sync at the *system* level
without any app writing to both schemas. Blue-green cutover via the load balancer.

Overlap is safe because **both new tables collapse duplicates by sorting key** —
`AggregatingMergeTree` on `otel_series`, and `ReplacingMergeTree ORDER BY (SeriesId, TimeUnix)` on
the datapoint tables. This is what makes the old path (via MV) and the new path (direct) able to write
the same rows concurrently without double-counting, and it is why the backfill/MV boundary below is a
non-issue rather than a hazard.

> Why no in-app dual-write: the MV substitutes for it on the old path, direct writes cover the new
> path, and LB coexistence covers the transition window.

## ⚠️ Precondition that will silently corrupt the migration if missed
The bridge MV must recompute `SeriesId` **in ClickHouse SQL** from the wide row, and it must agree
**byte-for-byte** with the Go implementation — same field order, same `\x1f` separator, same sorted
map-key serialisation, `xxHash64` seed 0 (see design §SeriesId canonical encoding). If the two
diverge, the old path and new path mint **different ids for the same series**: identities and
datapoints split in half, and the Step-4 row-count parity check **will still pass**, because the row
counts are right — only the ids are wrong. Run the Go↔ClickHouse hash-parity fixture test (task 2)
against the exact MV expression before attaching it.

## Preconditions
- 001 shipped: new-app image writes `otel_series` + skinny `_datapoints` tables via app-side dedup.
- New tables created (`otel_series`, `otel_metrics_gauge_datapoints`, `otel_metrics_sum_datapoints`).
- Hash-parity verified (above).

## Steps
1. **Create new tables.** `otel_series` + skinny `_datapoints` tables. No traffic yet; harmless if empty.
2. **Attach bridge MVs** on the existing wide tables → derive `otel_series` rows and skinny datapoint
   rows from every *new* fat insert, computing `SeriesId` with the canonical SQL expression. Old apps
   remain untouched and keep serving.
3. **Backfill history.** One-time `INSERT INTO <new> SELECT … FROM <wide>`. The MV only fires on
   inserts made *after* its creation, so pre-existing history must be backfilled explicitly.
   Overlap with the MV at the boundary — and any late-arriving row the MV also writes — is **harmless**:
   a repeated `(SeriesId, TimeUnix)` collapses under `ReplacingMergeTree`. No cutover timestamp, no
   engine juggling. (An earlier draft proposed "temporarily switch the datapoint tables to
   ReplacingMergeTree" — that is not a real operation; a table's engine cannot be swapped in place.
   Adopting `ReplacingMergeTree` permanently, per the design, removes the need.)
4. **Verify parity.** Compare row counts **and spot-check `SeriesId` values** between the MV-derived
   rows and ids computed by the new app for the same series. Counts alone do not detect hash divergence.
5. **Deploy the new app** (writes new tables directly). Both paths now populate the new tables:
   old-app traffic via the MV, new-app traffic direct.
6. **LB cutover.** Shift traffic old→new gradually (canary → full). Overlap is safe (duplicates collapse).
7. **Retire.** Drain and stop old apps → drop the bridge MVs → drop or TTL-expire the wide tables.

## Rollback
- Steps 1–4 are additive and non-destructive: the wide tables and old apps are untouched — abort by
  dropping the new tables/MVs.
- Steps 5–6: if the new app misbehaves, shift the LB back to old apps; they never stopped writing
  the wide tables, so no data is lost.
- Step 7 is the point of no return — only after parity and full cutover are confirmed.

## Risks
- **Go/ClickHouse hash divergence** — the one failure that survives a parity check. Mitigated by the
  fixture test and the Step-4 id spot-check above.
- **MV insert-cost on the old path** during the window — the series MV amplifies (one row per
  datapoint) unless it `GROUP BY SeriesId` per block; acceptable for a bounded migration window.
- **Backfill load** — large `INSERT … SELECT` competes with live ingest; run off-peak or chunk by
  time partition.
- **Pre-merge duplicates** — until `ReplacingMergeTree` merges, a read may see a datapoint twice;
  the canonical query's `LIMIT 1 BY (SeriesId, TimeUnix)` absorbs this.

## Verification
Hash parity confirmed before Step 2. Parity check (Step 4) passes on both counts and ids. After
cutover, `MetricsQuerier.QueryDatapoints` returns the same datapoints the legacy wide-table query did
for an overlapping window; `otel_series` holds one row per series; wide tables retired with no data loss.
