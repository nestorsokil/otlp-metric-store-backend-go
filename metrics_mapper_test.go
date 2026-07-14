package main

import (
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func baseIdentity() SeriesIdentity {
	return SeriesIdentity{
		ServiceName:        "svc",
		MetricName:         "http.requests",
		MetricType:         "sum",
		ResourceSchemaUrl:  "https://opentelemetry.io/schemas/1.4.0",
		ScopeName:          "scope",
		ScopeVersion:       "1.0.0",
		ScopeSchemaUrl:     "",
		ResourceAttributes: map[string]string{"host.name": "h1", "region": "eu"},
		ScopeAttributes:    map[string]string{"a": "1"},
		Attributes:         map[string]string{"method": "GET", "status": "200"},
	}
}

func TestSeriesID_SameIdentityYieldsSameID(t *testing.T) {
	a := baseIdentity()
	b := baseIdentity()
	if seriesID(a) != seriesID(b) {
		t.Fatalf("expected same identity to hash to the same SeriesId")
	}
}

func TestSeriesID_DifferentIdentityYieldsDifferentID(t *testing.T) {
	base := baseIdentity()
	id := seriesID(base)

	variants := []func(*SeriesIdentity){
		func(i *SeriesIdentity) { i.ServiceName = "other-svc" },
		func(i *SeriesIdentity) { i.MetricName = "other.metric" },
		func(i *SeriesIdentity) { i.MetricType = "gauge" },
		func(i *SeriesIdentity) { i.ResourceSchemaUrl = "other" },
		func(i *SeriesIdentity) { i.ScopeName = "other-scope" },
		func(i *SeriesIdentity) { i.ScopeVersion = "2.0.0" },
		func(i *SeriesIdentity) { i.ScopeSchemaUrl = "other" },
		func(i *SeriesIdentity) {
			i.ResourceAttributes = map[string]string{"host.name": "h2", "region": "eu"}
		},
		func(i *SeriesIdentity) { i.ScopeAttributes = map[string]string{"a": "2"} },
		func(i *SeriesIdentity) {
			i.Attributes = map[string]string{"method": "POST", "status": "200"}
		},
	}

	for idx, mutate := range variants {
		v := baseIdentity()
		mutate(&v)
		if got := seriesID(v); got == id {
			t.Errorf("variant %d: expected different SeriesId, got same id %d", idx, got)
		}
	}
}

func TestSeriesID_MapKeyOrderIndependent(t *testing.T) {
	a := baseIdentity()
	a.Attributes = map[string]string{"method": "GET", "status": "200", "path": "/x"}

	b := baseIdentity()
	b.Attributes = map[string]string{}
	b.Attributes["path"] = "/x"
	b.Attributes["status"] = "200"
	b.Attributes["method"] = "GET"

	if seriesID(a) != seriesID(b) {
		t.Fatalf("expected map key insertion order to not affect SeriesId")
	}
}

// TestSeriesID_NoDelimiterCollision guards against the exact bug length-prefixing exists to
// prevent: {a: "b,c=d"} and {a: "b", c: "d"} both render "a=b,c=d" under a naive
// delimiter-separated encoding, silently merging two distinct series into one id.
func TestSeriesID_NoDelimiterCollision(t *testing.T) {
	a := baseIdentity()
	a.Attributes = map[string]string{"a": "b,c=d"}

	b := baseIdentity()
	b.Attributes = map[string]string{"a": "b", "c": "d"}

	if seriesID(a) == seriesID(b) {
		t.Fatalf("expected distinct attribute maps to hash differently, got colliding SeriesId")
	}
}

func TestSeriesID_ControlBytesDoNotCollide(t *testing.T) {
	a := baseIdentity()
	a.Attributes = map[string]string{"k": "v\x00extra"}

	b := baseIdentity()
	b.Attributes = map[string]string{"k": "v"}

	if seriesID(a) == seriesID(b) {
		t.Fatalf("expected values differing by control bytes to hash differently")
	}
}

func gaugeResourceMetrics(dpTime time.Time) []*metricspb.ResourceMetrics {
	return []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "test-scope", Version: "1.0.0"},
					Metrics: []*metricspb.Metric{
						{
							Name:        "cpu.utilization",
							Description: "CPU utilization percentage",
							Unit:        "%",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes:   []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
											TimeUnixNano: uint64(dpTime.UnixNano()),
											Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
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

func TestMapGaugeRows_SkinnyRowAndSeriesRow(t *testing.T) {
	dpTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	ingestTime := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)

	rows, series := MapGaugeRows(gaugeResourceMetrics(dpTime), ingestTime)

	if len(rows) != 1 || len(series) != 1 {
		t.Fatalf("expected 1 datapoint row and 1 series row, got %d rows, %d series", len(rows), len(series))
	}

	if rows[0].SeriesId != series[0].SeriesId {
		t.Errorf("expected datapoint row and series row to share a SeriesId, got %d vs %d", rows[0].SeriesId, series[0].SeriesId)
	}
	if !rows[0].TimeUnix.Equal(dpTime) {
		t.Errorf("expected datapoint TimeUnix to be the event time %v, got %v", dpTime, rows[0].TimeUnix)
	}
	if rows[0].Value != 42.5 {
		t.Errorf("expected Value=42.5, got %f", rows[0].Value)
	}

	s := series[0]
	if s.ServiceName != "test-service" || s.MetricName != "cpu.utilization" || s.MetricType != metricTypeGauge {
		t.Errorf("unexpected series identity fields: %+v", s)
	}
	if s.MetricDescription != "CPU utilization percentage" || s.MetricUnit != "%" {
		t.Errorf("expected series-level constants (description/unit) to be populated, got %+v", s)
	}
	// FirstSeen/LastSeen must be ingest time, not the datapoint's own (far earlier) event time —
	// see 2-design.md §Schema on why LastSeen (the ReplacingMergeTree version column) must not
	// let a backfilled/future-dated datapoint freeze the surviving row.
	if !s.FirstSeen.Equal(ingestTime) || !s.LastSeen.Equal(ingestTime) {
		t.Errorf("expected FirstSeen/LastSeen to equal ingest time %v, got FirstSeen=%v LastSeen=%v", ingestTime, s.FirstSeen, s.LastSeen)
	}
}

func TestMapSumRows_SeriesLevelConstantsNotOnDatapointRow(t *testing.T) {
	dpTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Now()

	rm := []*metricspb.ResourceMetrics{
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
							Name: "http.requests.total",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: uint64(dpTime.UnixNano()), Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1234}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	rows, series := MapSumRows(rm, now)
	if len(rows) != 1 || len(series) != 1 {
		t.Fatalf("expected 1 datapoint row and 1 series row, got %d rows, %d series", len(rows), len(series))
	}
	if rows[0].Value != 1234 {
		t.Errorf("expected Value=1234, got %f", rows[0].Value)
	}
	// AggregationTemporality/IsMonotonic are series-level constants: SumRow has no such fields
	// (compile-time guarantee), they must land only on the SeriesRow.
	if series[0].AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
		t.Errorf("expected AggregationTemporality on the series row, got %d", series[0].AggregationTemporality)
	}
	if !series[0].IsMonotonic {
		t.Errorf("expected IsMonotonic=true on the series row")
	}
	if series[0].MetricType != metricTypeSum {
		t.Errorf("expected MetricType=%q, got %q", metricTypeSum, series[0].MetricType)
	}
}

func TestMapGaugeRows_RepeatedIdentityYieldsSameSeriesId(t *testing.T) {
	now := time.Now()
	rm := gaugeResourceMetrics(now)
	// Duplicate the single datapoint so the same series identity appears twice in one batch.
	dp := rm[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints[0]
	rm[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints = append(rm[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints, dp)

	_, series := MapGaugeRows(rm, now)
	if len(series) != 2 {
		t.Fatalf("expected 2 series row candidates, got %d", len(series))
	}
	if series[0].SeriesId != series[1].SeriesId {
		t.Errorf("expected repeated identity within a batch to hash to the same SeriesId, got %d vs %d", series[0].SeriesId, series[1].SeriesId)
	}
}
