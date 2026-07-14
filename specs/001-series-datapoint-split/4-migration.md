# Wide-Table → Series/Datapoint Migration — Runbook
> Status: draft

## Context
> **Documented, not implemented.** This is the rollout plan for 001 *if* it is ever applied to an
> existing wide-table deployment. The delivered code is greenfield/forward-only (task 1). Nothing in
> this runbook is built or tested.

Rollout plan for this feature (001) when applied to an **existing** deployment (already writing the
legacy wide `otel_metrics_gauge`/`otel_metrics_sum` tables), moving it onto the new series/datapoint
schema with **no producer changes and no in-app dual-write logic**.

## Strategy
A ClickHouse **Materialized View bridges the old write path**: old apps keep inserting fat rows
unchanged; an MV mirrors each new insert into the new tables; a one-time backfill covers history;
the new app writes the new tables directly. Two representations stay in sync at the *system* level
without any app writing to both schemas. Blue-green cutover via the load balancer.

Overlap is safe because **both new tables collapse duplicates by sorting key** — `ReplacingMergeTree`
on `otel_series` and on the datapoint tables (`ORDER BY (SeriesId, TimeUnix)`). This is what lets the
old path (via MV) and the new path (direct) write the same rows concurrently without double-counting,
and why the backfill/MV boundary below is a non-issue rather than a hazard.

> Why no in-app dual-write: the MV substitutes for it on the old path, direct writes cover the new
> path, and LB coexistence covers the transition window.

## ⚠️ Precondition that will silently corrupt the migration if missed
The bridge MV must recompute `SeriesId` **in ClickHouse SQL** from the wide row, and it must agree
**byte-for-byte** with Go's `seriesID` (design §SeriesId canonical encoding — length-prefixed, sorted
map keys, `xxHash64` seed 0). If the two diverge, the old path and new path mint **different ids for
the same series**: identities and datapoints split in half, and the Step-4 row-count parity check
**will still pass** — the counts are right, only the ids are wrong.

The SQL is written out here so it isn't improvised during a migration. `length()` returns **bytes**
(matching Go's `len`) — never use `lengthUTF8`:

```sql
CREATE FUNCTION lp        AS (s) -> concat(toString(length(s)), ':', s);
CREATE FUNCTION encodeMap AS (m) -> lp(arrayStringConcat(
    arrayMap(k -> concat(lp(k), lp(m[k])), arraySort(mapKeys(m)))));

-- must equal Go's seriesID() for the same series:
xxHash64(concat(
    lp(ServiceName), lp(MetricName), lp(MetricType), lp(ResourceSchemaUrl),
    lp(ScopeName), lp(ScopeVersion), lp(ScopeSchemaUrl),
    encodeMap(ResourceAttributes), encodeMap(ScopeAttributes), encodeMap(Attributes)))
```

**Before attaching the MV**, verify parity against the shipped Go implementation on a fixture that
includes attribute values containing `,`, `=`, and control bytes. (This parity check is *not* part of
the delivered test suite — the migration is documented, not built. Whoever runs the migration writes
it.)

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
