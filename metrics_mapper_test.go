package main

import "testing"

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
