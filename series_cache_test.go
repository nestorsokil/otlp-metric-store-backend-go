package main

import (
	"sync"
	"testing"
	"time"
)

func testCache(refresh time.Duration, maxEntries int, idleTTL time.Duration) *SeriesCache {
	return NewSeriesCache(SeriesCacheConfig{
		RefreshInterval: refresh,
		MaxEntries:      maxEntries,
		IdleTTL:         idleTTL,
	})
}

func TestSeriesCache_FirstSightEmits(t *testing.T) {
	c := testCache(5*time.Minute, 100, time.Hour)
	now := time.Now()

	if !c.ShouldEmit(1, now) {
		t.Fatalf("expected first sight of an unseen series to emit")
	}
}

func TestSeriesCache_RecheckBeforeMarkEmittedStillEmits(t *testing.T) {
	c := testCache(5*time.Minute, 100, time.Hour)
	now := time.Now()

	// A failed InsertSeries never calls MarkEmitted, so repeated ShouldEmit calls for the same
	// unmarked id must keep saying "emit" — otherwise a failed insert would silently suppress the
	// client's retry.
	if !c.ShouldEmit(1, now) {
		t.Fatalf("expected 1st ShouldEmit (unmarked) to emit")
	}
	if !c.ShouldEmit(1, now.Add(time.Second)) {
		t.Fatalf("expected 2nd ShouldEmit (still unmarked) to emit")
	}
}

func TestSeriesCache_SuppressedUntilRefreshIntervalElapses(t *testing.T) {
	refresh := 5 * time.Minute
	c := testCache(refresh, 100, time.Hour)
	t0 := time.Now()

	c.MarkEmitted(1, t0)

	if c.ShouldEmit(1, t0) {
		t.Fatalf("expected suppression immediately after MarkEmitted")
	}
	if c.ShouldEmit(1, t0.Add(refresh-time.Nanosecond)) {
		t.Fatalf("expected suppression just before RefreshInterval elapses")
	}
	if !c.ShouldEmit(1, t0.Add(refresh)) {
		t.Fatalf("expected emit once RefreshInterval has elapsed")
	}
}

func TestSeriesCache_EvictionBoundsSize(t *testing.T) {
	c := testCache(5*time.Minute, 2, time.Hour)
	now := time.Now()

	c.MarkEmitted(1, now)
	c.MarkEmitted(2, now)
	c.MarkEmitted(3, now) // evicts id 1 (LRU, capacity 2)

	if got := c.Len(); got > 2 {
		t.Fatalf("expected cache size bounded to 2, got %d", got)
	}
	// The evicted series reverts to "unseen" -> ShouldEmit is true again.
	if !c.ShouldEmit(1, now) {
		t.Fatalf("expected evicted series to be treated as unseen (should emit)")
	}
}

func TestSeriesCache_ConcurrentAccessSafe(t *testing.T) {
	c := testCache(time.Millisecond, 1000, time.Hour)
	now := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				n := now.Add(time.Duration(j) * time.Millisecond)
				if c.ShouldEmit(id, n) {
					c.MarkEmitted(id, n)
				}
				_ = c.Len()
			}
		}(uint64(i % 10))
	}
	wg.Wait()
}
