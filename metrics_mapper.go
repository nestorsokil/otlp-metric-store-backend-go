package main

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// SeriesIdentity is the tuple of fields that uniquely identifies a series: two datapoints with
// the same identity belong to the same time-stream and hash to the same SeriesId.
type SeriesIdentity struct {
	ServiceName        string
	MetricName         string
	MetricType         string
	ResourceSchemaUrl  string
	ScopeName          string
	ScopeVersion       string
	ScopeSchemaUrl     string
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	Attributes         map[string]string
}

// seriesID derives the deterministic SeriesId for a series identity per the canonical
// length-prefixed encoding (2-design.md SeriesId canonical encoding). Length-prefixing, not
// delimiter-separation, is load-bearing: attribute keys/values are arbitrary user-controlled
// UTF-8, so a naive "k=v,k=v" join collides deterministically when a value itself contains ","
// or "=" (e.g. {a: "b,c=d"} and {a: "b", c: "d"} both render "a=b,c=d"). A length prefix is
// unambiguous for any byte content, no escaping needed.
func seriesID(identity SeriesIdentity) uint64 {
	var buf []byte
	buf = appendLengthPrefixed(buf, identity.ServiceName)
	buf = appendLengthPrefixed(buf, identity.MetricName)
	buf = appendLengthPrefixed(buf, identity.MetricType)
	buf = appendLengthPrefixed(buf, identity.ResourceSchemaUrl)
	buf = appendLengthPrefixed(buf, identity.ScopeName)
	buf = appendLengthPrefixed(buf, identity.ScopeVersion)
	buf = appendLengthPrefixed(buf, identity.ScopeSchemaUrl)
	buf = appendEncodedMap(buf, identity.ResourceAttributes)
	buf = appendEncodedMap(buf, identity.ScopeAttributes)
	buf = appendEncodedMap(buf, identity.Attributes)
	return xxhash.Sum64(buf)
}

// appendLengthPrefixed appends lp(s) = decimal(byte_len(s)) + ":" + s. Byte length (Go len), never
// rune count, so the prefix agrees with the raw bytes that follow regardless of encoding.
func appendLengthPrefixed(buf []byte, s string) []byte {
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, ':')
	buf = append(buf, s...)
	return buf
}

// appendEncodedMap appends lp(k)+lp(v) for every entry, iterated over keys sorted bytewise so
// the encoding is independent of Go's randomized map iteration order.
func appendEncodedMap(buf []byte, m map[string]string) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buf = appendLengthPrefixed(buf, k)
		buf = appendLengthPrefixed(buf, m[k])
	}
	return buf
}

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// Metric type discriminators used both as the SeriesIdentity.MetricType field and as the
// DatapointQuery.MetricType table selector (2-design.md §MetricsQuerier).
const (
	metricTypeGauge = "gauge"
	metricTypeSum   = "sum"
)

// buildSeriesRow assembles the series lookup row for a datapoint's identity. FirstSeen/LastSeen
// are ingest time (now), not the datapoint's own TimeUnix — see 2-design.md §Schema for why
// LastSeen, the ReplacingMergeTree version column, must not be event time.
func buildSeriesRow(id uint64, identity SeriesIdentity, scopeDroppedAttrCount uint32, aggregationTemporality int32, isMonotonic bool, description, unit string, now time.Time) SeriesRow {
	return SeriesRow{
		SeriesId:               id,
		ServiceName:            identity.ServiceName,
		MetricName:             identity.MetricName,
		MetricType:             identity.MetricType,
		ResourceAttributes:     identity.ResourceAttributes,
		ResourceSchemaUrl:      identity.ResourceSchemaUrl,
		ScopeName:              identity.ScopeName,
		ScopeVersion:           identity.ScopeVersion,
		ScopeAttributes:        identity.ScopeAttributes,
		ScopeDroppedAttrCount:  scopeDroppedAttrCount,
		ScopeSchemaUrl:         identity.ScopeSchemaUrl,
		Attributes:             identity.Attributes,
		AggregationTemporality: aggregationTemporality,
		IsMonotonic:            isMonotonic,
		MetricDescription:      description,
		MetricUnit:             unit,
		FirstSeen:              now,
		LastSeen:               now,
	}
}

// MapGaugeRows converts an ExportMetricsServiceRequest into skinny GaugeRows plus one SeriesRow
// per datapoint (candidates — dedup against the series cache happens in the Export handler). now
// is the ingest time stamped onto every returned SeriesRow's FirstSeen/LastSeen.
func MapGaugeRows(resourceMetrics []*metricspb.ResourceMetrics, now time.Time) ([]GaugeRow, []SeriesRow) {
	var rows []GaugeRow
	var series []SeriesRow
	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				gauge := metric.GetGauge()
				if gauge == nil {
					continue
				}
				for _, dp := range gauge.GetDataPoints() {
					identity := SeriesIdentity{
						ServiceName:        svcName,
						MetricName:         metric.GetName(),
						MetricType:         metricTypeGauge,
						ResourceSchemaUrl:  resSchemaUrl,
						ScopeName:          scope.GetName(),
						ScopeVersion:       scope.GetVersion(),
						ScopeSchemaUrl:     sm.GetSchemaUrl(),
						ResourceAttributes: resAttrs,
						ScopeAttributes:    scopeAttrs,
						Attributes:         kvToMap(dp.GetAttributes()),
					}
					id := seriesID(identity)

					rows = append(rows, GaugeRow{
						SeriesId:      id,
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})
					series = append(series, buildSeriesRow(id, identity, scope.GetDroppedAttributesCount(), 0, false, metric.GetDescription(), metric.GetUnit(), now))
				}
			}
		}
	}
	return rows, series
}

// MapSumRows converts an ExportMetricsServiceRequest into skinny SumRows plus one SeriesRow per
// datapoint. See MapGaugeRows for the candidate/dedup split and the meaning of now.
func MapSumRows(resourceMetrics []*metricspb.ResourceMetrics, now time.Time) ([]SumRow, []SeriesRow) {
	var rows []SumRow
	var series []SeriesRow
	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				sum := metric.GetSum()
				if sum == nil {
					continue
				}
				aggregationTemporality := int32(sum.GetAggregationTemporality())
				isMonotonic := sum.GetIsMonotonic()

				for _, dp := range sum.GetDataPoints() {
					identity := SeriesIdentity{
						ServiceName:        svcName,
						MetricName:         metric.GetName(),
						MetricType:         metricTypeSum,
						ResourceSchemaUrl:  resSchemaUrl,
						ScopeName:          scope.GetName(),
						ScopeVersion:       scope.GetVersion(),
						ScopeSchemaUrl:     sm.GetSchemaUrl(),
						ResourceAttributes: resAttrs,
						ScopeAttributes:    scopeAttrs,
						Attributes:         kvToMap(dp.GetAttributes()),
					}
					id := seriesID(identity)

					rows = append(rows, SumRow{
						SeriesId:      id,
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})
					series = append(series, buildSeriesRow(id, identity, scope.GetDroppedAttributesCount(), aggregationTemporality, isMonotonic, metric.GetDescription(), metric.GetUnit(), now))
				}
			}
		}
	}
	return rows, series
}
