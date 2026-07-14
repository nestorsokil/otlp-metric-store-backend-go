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
without any app writing to both schemas. Blue-green cutover via the load balancer; overlap is safe
because all writes are idempotent into the same new tables (`AggregatingMergeTree` on `otel_series`,
append on skinny datapoints).

> Why no in-app dual-write: the MV substitutes for it on the old path, direct writes cover the new
> path, and LB coexistence covers the transition window.

## Preconditions
- 001 shipped: new-app image writes `otel_series` + skinny `_datapoints` tables via app-side dedup.
- New tables created (`otel_series`, `otel_metrics_gauge_datapoints`, `otel_metrics_sum_datapoints`).
- A chosen cutover timestamp `T` for backfill boundary (see Step 3 dedup note).

## Steps
1. **Create new tables.** `otel_series` + skinny `_datapoints` tables. No traffic yet; harmless if empty.
2. **Attach bridge MVs** on the existing wide tables → derive `otel_series` rows (dedup key) and
   skinny datapoint rows from every *new* fat insert. Old apps remain untouched and keep serving.
3. **Backfill history.** One-time `INSERT INTO <new> SELECT … FROM <wide> WHERE TimeUnix < T`.
   The MV only catches inserts *after* its creation, so history must be backfilled explicitly.
   **Boundary dedup**: bound the backfill by `T` (and/or use a temporary `ReplacingMergeTree` on
   the skinny tables during the window) so a row near the boundary isn't inserted by both backfill
   and MV — skinny tables are append `MergeTree` and won't self-dedup.
4. **Verify parity.** Compare row counts / spot-check values between wide and new tables over an
   overlapping window before shifting any traffic.
5. **Deploy the new app** (writes new tables directly). Now both paths populate the new tables:
   old-app traffic via the MV, new-app traffic direct.
6. **LB cutover.** Shift traffic old→new gradually (canary → full). Overlap is safe (idempotent).
7. **Retire.** Drain and stop old apps → drop the bridge MVs → drop or TTL-expire the wide tables.

## Rollback
- Steps 1–4 are additive and non-destructive: the wide tables and old apps are untouched — abort by
  dropping the new tables/MVs.
- Steps 5–6: if the new app misbehaves, shift the LB back to old apps; they never stopped writing
  the wide tables, so no data is lost.
- Step 7 is the point of no return — only after parity and full cutover are confirmed.

## Risks
- **Boundary double-insert** on skinny datapoints — mitigated by the `T` bound / temporary
  `ReplacingMergeTree` in Step 3.
- **MV insert-cost on the old path** during the window — the series MV amplifies unless it
  `GROUP BY SeriesId` per block; acceptable for a bounded migration window.
- **Backfill load** — large `INSERT … SELECT` competes with live ingest; run off-peak or chunk by
  time partition.

## Verification
Parity check (Step 4) passes; after cutover, the 001 canonical two-step query returns the same
datapoints the legacy wide-table query did for an overlapping window; `otel_series` holds one row
per series; wide tables retired with no data loss.
