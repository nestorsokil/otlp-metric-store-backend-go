package main

// createSeriesTableSQL creates the series lookup table: one logical row per unique series
// identity (see 2-design.md SeriesId canonical encoding). ReplacingMergeTree(LastSeen) collapses
// repeated emits of the same series; no TTL (retention must be symmetric with datapoints, see
// design's Schema changes section).
const createSeriesTableSQL = `
CREATE TABLE IF NOT EXISTS otel_series (
    SeriesId               UInt64 CODEC(ZSTD(1)),
    ServiceName            LowCardinality(String) CODEC(ZSTD(1)),
    MetricName             LowCardinality(String) CODEC(ZSTD(1)),
    MetricType             LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl      String CODEC(ZSTD(1)),
    ScopeName              String CODEC(ZSTD(1)),
    ScopeVersion           String CODEC(ZSTD(1)),
    ScopeAttributes        Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount  UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl         String CODEC(ZSTD(1)),
    Attributes             Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic            Bool CODEC(ZSTD(1)),
    MetricDescription      String CODEC(ZSTD(1)),
    MetricUnit             String CODEC(ZSTD(1)),
    FirstSeen              DateTime64(9) CODEC(ZSTD(1)),
    LastSeen               DateTime64(9) CODEC(ZSTD(1)),

    INDEX idx_res_attr_key   mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key       mapKeys(Attributes)           TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value     mapValues(Attributes)         TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(LastSeen)
ORDER BY (ServiceName, MetricName, SeriesId)
SETTINGS index_granularity = 8192;
`

// createGaugeTableSQL creates the skinny gauge datapoint table: value + timestamp + a SeriesId
// reference into otel_series. ReplacingMergeTree collapses duplicate (SeriesId, TimeUnix) rows
// from retried Export calls (see design's Retry safety section).
const createGaugeTableSQL = `
CREATE TABLE IF NOT EXISTS otel_datapoints_gauge (
    SeriesId      UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Value         Float64 CODEC(ZSTD(1)),
    Flags         UInt32 CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesId, TimeUnix)
SETTINGS index_granularity = 8192;
`

// createSumTableSQL creates the skinny sum datapoint table. Identical shape to
// createGaugeTableSQL — AggregationTemporality/IsMonotonic are series-level constants and live
// on otel_series instead.
const createSumTableSQL = `
CREATE TABLE IF NOT EXISTS otel_datapoints_sum (
    SeriesId      UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    Value         Float64 CODEC(ZSTD(1)),
    Flags         UInt32 CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (SeriesId, TimeUnix)
SETTINGS index_granularity = 8192;
`
