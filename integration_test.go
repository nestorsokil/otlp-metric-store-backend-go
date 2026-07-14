//go:build integration

package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestMain lets LOG_LEVEL=debug surface the Export handler's Debug-level batch logging (datapoint
// counts, series emitted vs. deduped) during a test run. Nothing in the test path calls
// slog.SetDefault (that only happens in run()/main()), so the default handler's Info level would
// otherwise silently drop those lines.
func TestMain(m *testing.M) {
	if os.Getenv("LOG_LEVEL") == "debug" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	os.Exit(m.Run())
}

func setupClickHouse(t *testing.T) (*ClickHouseMetricsStore, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("starting clickhouse container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("getting mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := NewClickHouseMetricsStore(ctx, addr, "default", "default", "test")
	if err != nil {
		t.Fatalf("creating clickhouse metrics store: %v", err)
	}

	cleanup := func() {
		store.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminating clickhouse container: %v", err)
		}
	}

	return store, cleanup
}

// startTestServer wires a gRPC MetricsServiceClient (over bufconn) to store, so tests exercise
// the real Export handler rather than calling MetricsStore methods directly.
func startTestServer(t *testing.T, store MetricsStore) colmetricspb.MetricsServiceClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return colmetricspb.NewMetricsServiceClient(conn)
}

func kvPairs(m map[string]string) []*commonpb.KeyValue {
	kvs := make([]*commonpb.KeyValue, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}})
	}
	return kvs
}

func withServiceName(service string, resourceAttrs map[string]string) map[string]string {
	m := map[string]string{"service.name": service}
	for k, v := range resourceAttrs {
		m[k] = v
	}
	return m
}

// gaugeExport builds an ExportMetricsServiceRequest carrying a single Gauge datapoint.
func gaugeExport(service, metricName string, resourceAttrs, dpAttrs map[string]string, value float64, ts time.Time) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{Attributes: kvPairs(withServiceName(service, resourceAttrs))},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "integration-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: metricName,
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												Attributes:   kvPairs(dpAttrs),
												TimeUnixNano: uint64(ts.UnixNano()),
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// sumExport builds an ExportMetricsServiceRequest carrying a single cumulative, monotonic Sum
// datapoint.
func sumExport(service, metricName string, resourceAttrs, dpAttrs map[string]string, value float64, ts time.Time) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{Attributes: kvPairs(withServiceName(service, resourceAttrs))},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "integration-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: metricName,
								Data: &metricspb.Metric_Sum{
									Sum: &metricspb.Sum{
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										IsMonotonic:            true,
										DataPoints: []*metricspb.NumberDataPoint{
											{
												Attributes:   kvPairs(dpAttrs),
												TimeUnixNano: uint64(ts.UnixNano()),
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestCreateTables(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	expectedTables := []string{"otel_series", "otel_datapoints_gauge", "otel_datapoints_sum"}
	for _, table := range expectedTables {
		var count uint64
		err := store.conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, count)
		}
	}
}

// TestQueryDatapoints_TimeFrameAndMetricTypeOnlyReturnsAllServices covers AC-6: the time-frame is
// the only mandatory filter, and MetricType selects the table rather than narrowing results —
// leaving every other filter unset must return datapoints across all services/metrics of that
// type.
func TestQueryDatapoints_TimeFrameAndMetricTypeOnlyReturnsAllServices(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()
	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client := startTestServer(t, store)
	now := time.Now().UTC()

	if _, err := client.Export(ctx, gaugeExport("service-a", "cpu.utilization", nil, nil, 1, now)); err != nil {
		t.Fatalf("exporting service-a gauge: %v", err)
	}
	if _, err := client.Export(ctx, gaugeExport("service-b", "cpu.utilization", nil, nil, 2, now)); err != nil {
		t.Fatalf("exporting service-b gauge: %v", err)
	}
	if _, err := client.Export(ctx, sumExport("service-a", "http.requests.total", nil, nil, 3, now)); err != nil {
		t.Fatalf("exporting service-a sum: %v", err)
	}

	points, truncated, err := store.QueryDatapoints(ctx, DatapointQuery{
		From: now.Add(-time.Minute), To: now.Add(time.Minute), MetricType: metricTypeGauge,
	})
	if err != nil {
		t.Fatalf("querying datapoints: %v", err)
	}
	if truncated {
		t.Errorf("did not expect truncation")
	}
	if len(points) != 2 {
		t.Fatalf("expected 2 gauge datapoints across both services, got %d: %+v", len(points), points)
	}
	services := map[string]bool{}
	for _, p := range points {
		services[p.ServiceName] = true
	}
	if !services["service-a"] || !services["service-b"] {
		t.Errorf("expected datapoints from both services, got %v", services)
	}
}

// TestQueryDatapoints_OptionalFiltersNarrowResults covers AC-6's optional-filter half: each set
// filter must narrow the result, unset filters must not.
func TestQueryDatapoints_OptionalFiltersNarrowResults(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()
	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client := startTestServer(t, store)
	now := time.Now().UTC()

	if _, err := client.Export(ctx, gaugeExport("svc-a", "cpu.utilization",
		map[string]string{"region": "eu"}, map[string]string{"cpu": "0"}, 1, now)); err != nil {
		t.Fatalf("export 1: %v", err)
	}
	if _, err := client.Export(ctx, gaugeExport("svc-b", "cpu.utilization",
		map[string]string{"region": "us"}, map[string]string{"cpu": "1"}, 2, now)); err != nil {
		t.Fatalf("export 2: %v", err)
	}
	if _, err := client.Export(ctx, gaugeExport("svc-a", "mem.usage",
		map[string]string{"region": "eu"}, nil, 3, now)); err != nil {
		t.Fatalf("export 3: %v", err)
	}

	from, to := now.Add(-time.Minute), now.Add(time.Minute)

	byService, _, err := store.QueryDatapoints(ctx, DatapointQuery{From: from, To: to, MetricType: metricTypeGauge, ServiceName: "svc-a"})
	if err != nil {
		t.Fatalf("querying by ServiceName: %v", err)
	}
	if len(byService) != 2 {
		t.Errorf("expected 2 datapoints for svc-a, got %d: %+v", len(byService), byService)
	}

	byMetric, _, err := store.QueryDatapoints(ctx, DatapointQuery{From: from, To: to, MetricType: metricTypeGauge, ServiceName: "svc-a", MetricName: "cpu.utilization"})
	if err != nil {
		t.Fatalf("querying by MetricName: %v", err)
	}
	if len(byMetric) != 1 {
		t.Errorf("expected 1 datapoint for svc-a/cpu.utilization, got %d: %+v", len(byMetric), byMetric)
	}

	byRegion, _, err := store.QueryDatapoints(ctx, DatapointQuery{From: from, To: to, MetricType: metricTypeGauge, ResourceAttributes: map[string]string{"region": "eu"}})
	if err != nil {
		t.Fatalf("querying by ResourceAttributes: %v", err)
	}
	if len(byRegion) != 2 {
		t.Errorf("expected 2 datapoints for region=eu, got %d: %+v", len(byRegion), byRegion)
	}

	byAttr, _, err := store.QueryDatapoints(ctx, DatapointQuery{From: from, To: to, MetricType: metricTypeGauge, Attributes: map[string]string{"cpu": "1"}})
	if err != nil {
		t.Fatalf("querying by Attributes: %v", err)
	}
	if len(byAttr) != 1 {
		t.Errorf("expected 1 datapoint for cpu=1, got %d: %+v", len(byAttr), byAttr)
	}
}

// TestSeriesTable_RepeatedInsertsCollapseToOneLogicalRow covers AC-2 at the storage layer:
// otel_series holds exactly one logical row per SeriesId. ReplacingMergeTree dedups lazily, so a
// bare count() would flake — assert against the merged state via FINAL, per 2-design.md §Canonical
// read query. Inserted directly (bypassing the Export/dedup-cache layer, which has its own unit
// tests) so this specifically exercises the table's ReplacingMergeTree collapsing.
func TestSeriesTable_RepeatedInsertsCollapseToOneLogicalRow(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()
	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	identity := SeriesIdentity{ServiceName: "svc", MetricName: "requests", MetricType: metricTypeGauge}
	id := seriesID(identity)

	for i := 0; i < 3; i++ {
		row := SeriesRow{
			SeriesId: id, ServiceName: "svc", MetricName: "requests", MetricType: metricTypeGauge,
			FirstSeen: time.Now(), LastSeen: time.Now(),
		}
		if err := store.InsertSeries(ctx, []SeriesRow{row}); err != nil {
			t.Fatalf("inserting series row (attempt %d): %v", i, err)
		}
	}

	var count uint64
	if err := store.conn.QueryRow(ctx, "SELECT count() FROM otel_series FINAL WHERE SeriesId = $1", id).Scan(&count); err != nil {
		t.Fatalf("counting series rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected ReplacingMergeTree to collapse repeated inserts of the same SeriesId to 1 logical row, got %d", count)
	}
}

// TestExport_RetriedBatchDoesNotDoubleCount covers AC-7: the routine OTLP client retry (same
// batch delivered twice after a timeout/UNAVAILABLE) must not double-count. Verified end-to-end
// through gRPC + QueryDatapoints — no raw SQL.
func TestExport_RetriedBatchDoesNotDoubleCount(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()
	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client := startTestServer(t, store)
	ts := time.Now().UTC()
	req := gaugeExport("retry-svc", "requests", nil, nil, 42, ts)

	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("1st export: %v", err)
	}
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("2nd (retried) export: %v", err)
	}

	points, _, err := store.QueryDatapoints(ctx, DatapointQuery{
		From: ts.Add(-time.Minute), To: ts.Add(time.Minute), MetricType: metricTypeGauge, ServiceName: "retry-svc",
	})
	if err != nil {
		t.Fatalf("querying datapoints: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("expected the retried batch to collapse to 1 datapoint, got %d: %+v", len(points), points)
	}
	if points[0].Value != 42 {
		t.Errorf("expected Value=42 (not summed/doubled), got %f", points[0].Value)
	}
}

// TestQueryDatapoints_NoFullTableScan covers AC-3/C-2: a time-scoped query must prune by
// partition rather than scanning every part. EXPLAIN indexes = 1 surfaces the planner's Partition
// key evidence for toDate(TimeUnix).
func TestQueryDatapoints_NoFullTableScan(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()
	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client := startTestServer(t, store)

	// Spread datapoints across 3 day-partitions so a time-scoped query has something to prune.
	base := time.Now().UTC().Truncate(24 * time.Hour)
	for i := 0; i < 3; i++ {
		day := base.AddDate(0, 0, -i)
		if _, err := client.Export(ctx, gaugeExport("scan-svc", "requests", nil, nil, float64(i), day)); err != nil {
			t.Fatalf("export day %d: %v", i, err)
		}
	}

	query, args, _, err := buildDatapointQuery(DatapointQuery{From: base, To: base.Add(24 * time.Hour), MetricType: metricTypeGauge})
	if err != nil {
		t.Fatalf("building query: %v", err)
	}

	rows, err := store.conn.Query(ctx, "EXPLAIN indexes = 1 "+query, args...)
	if err != nil {
		t.Fatalf("running EXPLAIN: %v", err)
	}
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scanning EXPLAIN line: %v", err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating EXPLAIN rows: %v", err)
	}

	planText := plan.String()
	if !strings.Contains(planText, "Partition") || !strings.Contains(planText, "TimeUnix") {
		t.Errorf("expected EXPLAIN plan to show partition pruning on TimeUnix (PARTITION BY toDate(TimeUnix)), got:\n%s", planText)
	}
}
