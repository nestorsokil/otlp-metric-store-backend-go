package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// MetricsQuerier is the read-side analog of MetricsStore: it encapsulates the two-step
// series-then-datapoint join behind a typed filter so callers never write SQL. Not exposed over
// the network (NG-1) — it backs integration tests and any future endpoint.
type MetricsQuerier interface {
	QueryDatapoints(ctx context.Context, q DatapointQuery) (points []Datapoint, truncated bool, err error)
}

// DatapointQuery filters a read. From/To and MetricType are the only required fields (C-2) —
// MetricType selects the datapoint table rather than filtering it. Every other field is optional
// and emits no SQL clause when left unset.
type DatapointQuery struct {
	From, To           time.Time
	MetricType         string // "gauge" | "sum"
	ServiceName        string
	MetricName         string
	Attributes         map[string]string // datapoint-level attribute equality
	ResourceAttributes map[string]string // resource-level attribute equality
	Limit              int               // default defaultQueryLimit
}

// Datapoint is one observation, resolved against its series so results are self-describing.
type Datapoint struct {
	SeriesId    uint64
	ServiceName string
	MetricName  string
	TimeUnix    time.Time
	Value       float64
}

const defaultQueryLimit = 10_000

var datapointTableByMetricType = map[string]string{
	metricTypeGauge: "otel_datapoints_gauge",
	metricTypeSum:   "otel_datapoints_sum",
}

// QueryDatapoints resolves SeriesIds from otel_series (narrowed by MetricType and any set
// filters), then range-scans the matching datapoint table by SeriesId + time. See 2-design.md
// §Canonical read query for why the subquery needs DISTINCT and the read path needs no FINAL.
func (s *ClickHouseMetricsStore) QueryDatapoints(ctx context.Context, q DatapointQuery) ([]Datapoint, bool, error) {
	query, args, limit, err := buildDatapointQuery(q)
	if err != nil {
		return nil, false, err
	}

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("querying datapoints: %w", err)
	}
	defer rows.Close()

	var points []Datapoint
	for rows.Next() {
		var p Datapoint
		if err := rows.Scan(&p.SeriesId, &p.ServiceName, &p.MetricName, &p.TimeUnix, &p.Value); err != nil {
			return nil, false, fmt.Errorf("scanning datapoint row: %w", err)
		}
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterating datapoint rows: %w", err)
	}

	// Truncated reports that Limit was hit. Results are ORDER BY (SeriesId, TimeUnix), so
	// truncation is biased (all of the lowest SeriesIds, never a sample) — surfaced here rather
	// than silently dropped.
	truncated := len(points) > limit
	if truncated {
		points = points[:limit]
	}
	return points, truncated, nil
}

// buildDatapointQuery renders the canonical read query and its positional ($N) arguments. Pure —
// no ClickHouse dependency — so the filter-emission logic is unit-testable on its own.
func buildDatapointQuery(q DatapointQuery) (query string, args []any, limit int, err error) {
	table, ok := datapointTableByMetricType[q.MetricType]
	if !ok {
		return "", nil, 0, fmt.Errorf("unsupported MetricType %q", q.MetricType)
	}

	limit = q.Limit
	if limit <= 0 {
		limit = defaultQueryLimit
	}

	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	var subWhere strings.Builder
	subWhere.WriteString("MetricType = ")
	subWhere.WriteString(arg(q.MetricType))
	if q.ServiceName != "" {
		subWhere.WriteString(" AND ServiceName = ")
		subWhere.WriteString(arg(q.ServiceName))
	}
	if q.MetricName != "" {
		subWhere.WriteString(" AND MetricName = ")
		subWhere.WriteString(arg(q.MetricName))
	}
	appendMapFilter(&subWhere, "Attributes", q.Attributes, arg)
	appendMapFilter(&subWhere, "ResourceAttributes", q.ResourceAttributes, arg)

	from := arg(q.From)
	to := arg(q.To)
	// Query limit+1 so a full page tells us whether Limit was actually hit, without a second
	// COUNT query.
	lim := arg(limit + 1)

	query = fmt.Sprintf(`SELECT s.SeriesId, s.ServiceName, s.MetricName, dp.TimeUnix, dp.Value
FROM %s AS dp
INNER JOIN (
        SELECT DISTINCT SeriesId, ServiceName, MetricName
        FROM otel_series
        WHERE %s
    ) AS s ON s.SeriesId = dp.SeriesId
WHERE dp.TimeUnix BETWEEN %s AND %s
ORDER BY dp.SeriesId, dp.TimeUnix
LIMIT 1 BY dp.SeriesId, dp.TimeUnix
LIMIT %s`, table, subWhere.String(), from, to, lim)

	return query, args, limit, nil
}

// appendMapFilter appends "AND mapContains(col, $n) AND col[$n] = $m" for every key in m, sorted
// bytewise for deterministic query text. A bare "col[$n] = v" would wrongly match rows lacking
// the key too — ClickHouse returns ” for a missing map key (2-design.md §MetricsQuerier).
func appendMapFilter(w *strings.Builder, column string, m map[string]string, arg func(any) string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kArg := arg(k)
		fmt.Fprintf(w, " AND mapContains(%s, %s) AND %s[%s] = %s", column, kArg, column, kArg, arg(m[k]))
	}
}
