package main

import (
	"context"
	"errors"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// fakeMetricsStore is an in-process MetricsStore fake for exercising the Export handler's
// gating/ordering logic without a real ClickHouse — the black-box gRPC-to-ClickHouse coverage for
// this flow ships in task 7's read-path integration suite.
type fakeMetricsStore struct {
	insertSeriesErr error

	callOrder   []string
	seriesCalls [][]SeriesRow
	gaugeCalls  [][]GaugeRow
	sumCalls    [][]SumRow
}

func (f *fakeMetricsStore) CreateTables(context.Context) error { return nil }

func (f *fakeMetricsStore) InsertSeries(_ context.Context, rows []SeriesRow) error {
	f.callOrder = append(f.callOrder, "series")
	if f.insertSeriesErr != nil {
		return f.insertSeriesErr
	}
	f.seriesCalls = append(f.seriesCalls, append([]SeriesRow(nil), rows...))
	return nil
}

func (f *fakeMetricsStore) InsertGauge(_ context.Context, rows []GaugeRow) error {
	f.callOrder = append(f.callOrder, "gauge")
	f.gaugeCalls = append(f.gaugeCalls, append([]GaugeRow(nil), rows...))
	return nil
}

func (f *fakeMetricsStore) InsertSum(_ context.Context, rows []SumRow) error {
	f.callOrder = append(f.callOrder, "sum")
	f.sumCalls = append(f.sumCalls, append([]SumRow(nil), rows...))
	return nil
}

func (f *fakeMetricsStore) Close() error { return nil }

func newTestServer(store MetricsStore) *dash0MetricsServiceServer {
	return &dash0MetricsServiceServer{
		store: store,
		cache: NewSeriesCache(SeriesCacheConfig{RefreshInterval: time.Minute, MaxEntries: 1000, IdleTTL: time.Hour}),
	}
}

func gaugeExportRequest() *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "cpu.utilization",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{TimeUnixNano: uint64(time.Now().UnixNano()), Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}},
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

func TestExport_SeriesInsertedBeforeDatapoints(t *testing.T) {
	fake := &fakeMetricsStore{}
	srv := newTestServer(fake)

	if _, err := srv.Export(context.Background(), gaugeExportRequest()); err != nil {
		t.Fatalf("Export returned error: %v", err)
	}

	if len(fake.callOrder) != 2 || fake.callOrder[0] != "series" || fake.callOrder[1] != "gauge" {
		t.Fatalf("expected series insert before gauge insert, got call order %v", fake.callOrder)
	}
	if len(fake.seriesCalls) != 1 || len(fake.seriesCalls[0]) != 1 {
		t.Fatalf("expected exactly one series row inserted, got %v", fake.seriesCalls)
	}
}

func TestExport_RepeatedSeriesDedupedAcrossCalls(t *testing.T) {
	fake := &fakeMetricsStore{}
	srv := newTestServer(fake)
	req := gaugeExportRequest()

	if _, err := srv.Export(context.Background(), req); err != nil {
		t.Fatalf("1st Export returned error: %v", err)
	}
	if _, err := srv.Export(context.Background(), req); err != nil {
		t.Fatalf("2nd Export returned error: %v", err)
	}

	if len(fake.seriesCalls) != 1 {
		t.Fatalf("expected the series row to be emitted once (deduped on 2nd call), got %d series inserts", len(fake.seriesCalls))
	}
	if len(fake.gaugeCalls) != 2 {
		t.Fatalf("expected both datapoint batches to be inserted regardless of series dedup, got %d gauge inserts", len(fake.gaugeCalls))
	}
}

func TestExport_FailedSeriesInsertBlocksDatapointsAndDoesNotSuppressRetry(t *testing.T) {
	fake := &fakeMetricsStore{insertSeriesErr: errors.New("insert series failed")}
	srv := newTestServer(fake)
	req := gaugeExportRequest()

	if _, err := srv.Export(context.Background(), req); err == nil {
		t.Fatalf("expected Export to surface the InsertSeries error")
	}
	if len(fake.gaugeCalls) != 0 {
		t.Fatalf("expected series-first ordering to skip datapoint insert after a series insert failure, got %d gauge inserts", len(fake.gaugeCalls))
	}

	// The OTLP client retries the same batch; because MarkEmitted was never called for the failed
	// insert, ShouldEmit must still say "emit" so the retry actually writes the series row.
	fake.insertSeriesErr = nil
	if _, err := srv.Export(context.Background(), req); err != nil {
		t.Fatalf("retry Export returned error: %v", err)
	}
	if len(fake.seriesCalls) != 1 {
		t.Fatalf("expected the retried series insert to succeed exactly once, got %d successful series inserts", len(fake.seriesCalls))
	}
	if len(fake.gaugeCalls) != 1 {
		t.Fatalf("expected the retried datapoint insert to happen after series insert succeeds, got %d gauge inserts", len(fake.gaugeCalls))
	}
}
