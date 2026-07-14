package main

import (
	"context"
	"log/slog"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// transientStoreError wraps a ClickHouse write failure as a retryable gRPC status. A plain
// returned error surfaces as code Unknown, which OTLP clients treat as terminal — killing the
// retry this feature's whole retry-safety design (ReplacingMergeTree, ShouldEmit/MarkEmitted,
// AC-7) depends on. Unavailable tells a conforming client it's safe to retry the same batch.
func transientStoreError(err error) error {
	return status.Error(codes.Unavailable, err.Error())
}

type dash0MetricsServiceServer struct {
	addr  string
	store MetricsStore
	cache *SeriesCache

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{
		addr:  addr,
		store: store,
		cache: NewSeriesCache(DefaultSeriesCacheConfig()),
	}
}

// Export maps the request to skinny datapoint rows and candidate SeriesRows, gates the
// candidates through the dedup cache, and writes series-first: a crash between the two inserts
// leaves at worst a harmless orphan series row, never a dangling datapoint (2-design.md §Data
// flow). cache.MarkEmitted only runs after InsertSeries confirms success, so a failed insert
// leaves the series unmarked and the client's OTLP retry emits it again (2-design.md §Series
// dedup cache).
func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store == nil {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	rm := request.GetResourceMetrics()
	now := time.Now()

	gaugeRows, gaugeSeries := MapGaugeRows(rm, now)
	sumRows, sumSeries := MapSumRows(rm, now)

	if len(gaugeRows) == 0 && len(sumRows) == 0 {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	candidates := make([]SeriesRow, 0, len(gaugeSeries)+len(sumSeries))
	candidates = append(candidates, gaugeSeries...)
	candidates = append(candidates, sumSeries...)

	toEmit := make([]SeriesRow, 0, len(candidates))
	for _, s := range candidates {
		if m.cache.ShouldEmit(s.SeriesId, now) {
			toEmit = append(toEmit, s)
		}
	}

	if len(toEmit) > 0 {
		if err := m.store.InsertSeries(ctx, toEmit); err != nil {
			slog.ErrorContext(ctx, "inserting series rows",
				slog.String("error", err.Error()), slog.Int("series_count", len(toEmit)))
			return nil, transientStoreError(err)
		}
		for _, s := range toEmit {
			m.cache.MarkEmitted(s.SeriesId, now)
		}
		seriesRegisteredCounter.Add(ctx, int64(len(toEmit)))
	}
	seriesCacheSizeGauge.Record(ctx, int64(m.cache.Len()))

	if len(gaugeRows) > 0 {
		if err := m.store.InsertGauge(ctx, gaugeRows); err != nil {
			slog.ErrorContext(ctx, "inserting gauge rows",
				slog.String("error", err.Error()), slog.Int("count", len(gaugeRows)))
			return nil, transientStoreError(err)
		}
	}
	if len(sumRows) > 0 {
		if err := m.store.InsertSum(ctx, sumRows); err != nil {
			slog.ErrorContext(ctx, "inserting sum rows",
				slog.String("error", err.Error()), slog.Int("count", len(sumRows)))
			return nil, transientStoreError(err)
		}
	}

	slog.DebugContext(ctx, "Exported metrics batch",
		slog.Int("gauge_datapoints", len(gaugeRows)),
		slog.Int("sum_datapoints", len(sumRows)),
		slog.Int("series_candidates", len(candidates)),
		slog.Int("series_emitted", len(toEmit)),
		slog.Int("series_deduped", len(candidates)-len(toEmit)),
	)

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
