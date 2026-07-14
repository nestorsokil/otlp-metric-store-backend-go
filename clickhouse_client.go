package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// GaugeRow represents a single skinny gauge datapoint for ClickHouse insertion: value + timestamp
// referencing a SeriesId. Series-level fields live on SeriesRow instead (see 2-design.md §Schema).
type GaugeRow struct {
	SeriesId      uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// SumRow represents a single skinny sum datapoint for ClickHouse insertion. Identical shape to
// GaugeRow — AggregationTemporality/IsMonotonic are series-level constants and live on SeriesRow.
type SumRow struct {
	SeriesId      uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// SeriesRow represents one series identity row for insertion into otel_series — the series-level
// constants and dimension attributes, stored once per SeriesId rather than repeated per datapoint.
type SeriesRow struct {
	SeriesId               uint64
	ServiceName            string
	MetricName             string
	MetricType             string
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeAttributes        map[string]string
	ScopeDroppedAttrCount  uint32
	ScopeSchemaUrl         string
	Attributes             map[string]string
	AggregationTemporality int32
	IsMonotonic            bool
	MetricDescription      string
	MetricUnit             string
	FirstSeen              time.Time
	LastSeen               time.Time
}

// MetricsStore defines the interface for storing metrics in ClickHouse.
type MetricsStore interface {
	CreateTables(ctx context.Context) error
	InsertSeries(ctx context.Context, rows []SeriesRow) error
	InsertGauge(ctx context.Context, rows []GaugeRow) error
	InsertSum(ctx context.Context, rows []SumRow) error
	Close() error
}

// withAsyncInsert returns a context carrying ClickHouse's async_insert settings for batch
// inserts: async_insert=1 keeps server-side batching, wait_for_async_insert=1 makes the ack
// durable (the Export handler must not report success before the write is committed, since OTLP
// clients treat success as delivered) and stops reads racing the flush.
func withAsyncInsert(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":          1,
		"wait_for_async_insert": 1,
	}))
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn driver.Conn
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn}, nil
}

// CreateTables executes DDL for the series lookup table and the skinny datapoint tables.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	ddls := []string{
		createSeriesTableSQL,
		createGaugeTableSQL,
		createSumTableSQL,
	}
	for _, ddl := range ddls {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// InsertSeries batch-inserts series identity rows into otel_series.
func (s *ClickHouseMetricsStore) InsertSeries(ctx context.Context, rows []SeriesRow) error {
	batch, err := s.conn.PrepareBatch(withAsyncInsert(ctx), "INSERT INTO otel_series")
	if err != nil {
		return fmt.Errorf("preparing series batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesId,
			r.ServiceName,
			r.MetricName,
			r.MetricType,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.Attributes,
			r.AggregationTemporality,
			r.IsMonotonic,
			r.MetricDescription,
			r.MetricUnit,
			r.FirstSeen,
			r.LastSeen,
		); err != nil {
			return fmt.Errorf("appending series row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGauge batch-inserts skinny gauge rows into otel_datapoints_gauge.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []GaugeRow) error {
	batch, err := s.conn.PrepareBatch(withAsyncInsert(ctx), "INSERT INTO otel_datapoints_gauge")
	if err != nil {
		return fmt.Errorf("preparing gauge batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesId,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending gauge row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSum batch-inserts skinny sum rows into otel_datapoints_sum.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []SumRow) error {
	batch, err := s.conn.PrepareBatch(withAsyncInsert(ctx), "INSERT INTO otel_datapoints_sum")
	if err != nil {
		return fmt.Errorf("preparing sum batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesId,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending sum row: %w", err)
		}
	}
	return batch.Send()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
