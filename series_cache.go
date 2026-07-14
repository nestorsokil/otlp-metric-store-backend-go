package main

import (
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	envRefreshInterval = "SERIES_CACHE_REFRESH_INTERVAL"
	envMaxEntries      = "SERIES_CACHE_MAX_ENTRIES"
	envIdleTTL         = "SERIES_CACHE_IDLE_TTL"

	defaultRefreshInterval = 5 * time.Minute
	defaultMaxEntries      = 100_000
	defaultIdleTTL         = time.Hour
)

// SeriesCacheConfig configures the dedup cache. Defaults are pinned per 2-design.md §Series dedup
// cache; each can be overridden via its environment variable.
type SeriesCacheConfig struct {
	// RefreshInterval is how long a marked-emitted series is suppressed before ShouldEmit allows
	// it through again.
	RefreshInterval time.Duration
	// MaxEntries bounds the cache size (LRU eviction beyond this).
	MaxEntries int
	// IdleTTL evicts a series that hasn't been marked emitted for this long, bounding memory for
	// series that have gone silent.
	IdleTTL time.Duration
}

// DefaultSeriesCacheConfig returns the pinned defaults, overridden by any set environment
// variables. An invalid override value falls back to the default.
func DefaultSeriesCacheConfig() SeriesCacheConfig {
	return SeriesCacheConfig{
		RefreshInterval: envDuration(envRefreshInterval, defaultRefreshInterval),
		MaxEntries:      envInt(envMaxEntries, defaultMaxEntries),
		IdleTTL:         envDuration(envIdleTTL, defaultIdleTTL),
	}
}

func envDuration(name string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func envInt(name string, fallback int) int {
	v, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// SeriesCache keeps series-table writes off the per-datapoint hot path (C-3): a series row is
// re-emitted at most once per RefreshInterval, not once per datapoint.
//
// ShouldEmit and MarkEmitted are deliberately separate — decide, then record only after a
// confirmed write. A ShouldEmit that also recorded would corrupt on failure: it marks the series
// emitted, InsertSeries fails, the caller returns the error, the OTLP client retries, and
// ShouldEmit now says "skip" — the datapoints land with no series row, invisible to every query
// for a full RefreshInterval. Recording only after InsertSeries returns nil closes that hole.
//
// Correctness never depends on cache state (C-4): losing an entry (eviction, restart) just costs
// a redundant, idempotent re-emit, because otel_series is content-addressed and no query reads
// FirstSeen/LastSeen.
type SeriesCache struct {
	refreshInterval time.Duration
	lastEmitted     *expirable.LRU[uint64, time.Time]
}

// NewSeriesCache builds a SeriesCache from cfg. The underlying LRU is thread-safe.
func NewSeriesCache(cfg SeriesCacheConfig) *SeriesCache {
	return &SeriesCache{
		refreshInterval: cfg.RefreshInterval,
		lastEmitted:     expirable.NewLRU[uint64, time.Time](cfg.MaxEntries, nil, cfg.IdleTTL),
	}
}

// ShouldEmit reports whether the series identified by id should be (re-)written to otel_series:
// true if it has never been marked emitted, or if it was last marked emitted at least
// RefreshInterval ago. Decides only — never mutates the cache.
func (c *SeriesCache) ShouldEmit(id uint64, now time.Time) bool {
	lastMarked, ok := c.lastEmitted.Get(id)
	if !ok {
		return true
	}
	return now.Sub(lastMarked) >= c.refreshInterval
}

// MarkEmitted records that id was just successfully written to otel_series. Call this only after
// InsertSeries has returned nil for id — see the type doc for why.
func (c *SeriesCache) MarkEmitted(id uint64, now time.Time) {
	c.lastEmitted.Add(id, now)
}

// Len reports the number of series currently tracked (active series), for the
// series_cache_size gauge.
func (c *SeriesCache) Len() int {
	return c.lastEmitted.Len()
}
