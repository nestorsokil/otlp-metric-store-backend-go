package main

import (
	"strings"
	"testing"
	"time"
)

func baseQuery() DatapointQuery {
	return DatapointQuery{
		From:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		To:         time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		MetricType: metricTypeGauge,
	}
}

func TestBuildDatapointQuery_UnknownMetricTypeErrors(t *testing.T) {
	q := baseQuery()
	q.MetricType = "histogram"

	if _, _, _, err := buildDatapointQuery(q); err == nil {
		t.Fatalf("expected an error for an unsupported MetricType")
	}
}

func TestBuildDatapointQuery_SelectsTableByMetricType(t *testing.T) {
	gaugeQuery, _, _, err := buildDatapointQuery(baseQuery())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(gaugeQuery, "FROM otel_datapoints_gauge AS dp") {
		t.Errorf("expected gauge query to read from otel_datapoints_gauge, got:\n%s", gaugeQuery)
	}

	sumQ := baseQuery()
	sumQ.MetricType = metricTypeSum
	sumQuery, _, _, err := buildDatapointQuery(sumQ)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(sumQuery, "FROM otel_datapoints_sum AS dp") {
		t.Errorf("expected sum query to read from otel_datapoints_sum, got:\n%s", sumQuery)
	}
}

func TestBuildDatapointQuery_OnlyRequiredFieldsEmitNoOptionalClauses(t *testing.T) {
	q := baseQuery()

	query, args, limit, err := buildDatapointQuery(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, unwanted := range []string{"ServiceName =", "MetricName =", "mapContains"} {
		if strings.Contains(query, unwanted) {
			t.Errorf("expected no %q clause when the filter is unset, got:\n%s", unwanted, query)
		}
	}

	// MetricType, From, To, Limit — in that order, nothing else.
	if len(args) != 4 {
		t.Fatalf("expected 4 args (type, from, to, limit), got %d: %v", len(args), args)
	}
	if args[0] != metricTypeGauge {
		t.Errorf("expected 1st arg to be the MetricType, got %v", args[0])
	}
	if args[3] != defaultQueryLimit+1 {
		t.Errorf("expected the LIMIT arg to be default+1 (%d) to detect truncation, got %v", defaultQueryLimit+1, args[3])
	}
	if limit != defaultQueryLimit {
		t.Errorf("expected default limit %d, got %d", defaultQueryLimit, limit)
	}
}

func TestBuildDatapointQuery_OptionalFiltersEmitClausesAndArgsInOrder(t *testing.T) {
	q := baseQuery()
	q.ServiceName = "checkout"
	q.MetricName = "cpu.utilization"
	q.Limit = 50

	query, args, limit, err := buildDatapointQuery(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(query, "ServiceName = $2") {
		t.Errorf("expected ServiceName clause bound to $2, got:\n%s", query)
	}
	if !strings.Contains(query, "MetricName = $3") {
		t.Errorf("expected MetricName clause bound to $3, got:\n%s", query)
	}
	wantArgs := []any{metricTypeGauge, "checkout", "cpu.utilization", q.From, q.To, 51}
	if len(args) != len(wantArgs) {
		t.Fatalf("expected %d args, got %d: %v", len(wantArgs), len(args), args)
	}
	for i, want := range wantArgs {
		if args[i] != want {
			t.Errorf("arg[%d]: expected %v, got %v", i, want, args[i])
		}
	}
	if limit != 50 {
		t.Errorf("expected custom limit 50 to be honored, got %d", limit)
	}
}

func TestBuildDatapointQuery_AttributeFilterUsesMapContainsGuard(t *testing.T) {
	q := baseQuery()
	q.Attributes = map[string]string{"method": "GET"}

	query, args, _, err := buildDatapointQuery(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(query, "mapContains(Attributes, $2) AND Attributes[$2] = $3") {
		t.Errorf("expected a mapContains-guarded equality clause, got:\n%s", query)
	}
	if args[1] != "method" || args[2] != "GET" {
		t.Errorf("expected key then value args for the attribute filter, got %v", args[1:3])
	}
}

func TestBuildDatapointQuery_ResourceAttributeFilterAppliesToResourceAttributesColumn(t *testing.T) {
	q := baseQuery()
	q.ResourceAttributes = map[string]string{"region": "eu"}

	query, _, _, err := buildDatapointQuery(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(query, "mapContains(ResourceAttributes, $2) AND ResourceAttributes[$2] = $3") {
		t.Errorf("expected resource attribute filter against ResourceAttributes column, got:\n%s", query)
	}
}

func TestBuildDatapointQuery_MultiKeyAttributeFilterIsDeterministic(t *testing.T) {
	q := baseQuery()
	q.Attributes = map[string]string{"status": "200", "method": "GET", "path": "/x"}

	query1, args1, _, _ := buildDatapointQuery(q)
	query2, args2, _, _ := buildDatapointQuery(q)

	if query1 != query2 {
		t.Fatalf("expected repeated calls with the same filter map to produce identical SQL text (sorted keys), got:\n%s\nvs\n%s", query1, query2)
	}
	if len(args1) != len(args2) {
		t.Fatalf("expected identical arg lists across calls")
	}
	for i := range args1 {
		if args1[i] != args2[i] {
			t.Errorf("arg[%d] differs between calls: %v vs %v", i, args1[i], args2[i])
		}
	}
	// Keys are parameterized ($N), not inlined literally, so assert ordering via the arg values:
	// args[0] is MetricType, then (key, value) pairs in bytewise-sorted key order: method < path < status.
	wantKeyOrder := []any{"method", "GET", "path", "/x", "status", "200"}
	if len(args1) < 1+len(wantKeyOrder) {
		t.Fatalf("expected at least %d args, got %d: %v", 1+len(wantKeyOrder), len(args1), args1)
	}
	for i, want := range wantKeyOrder {
		if got := args1[1+i]; got != want {
			t.Errorf("arg[%d]: expected %v (bytewise-sorted key order), got %v", 1+i, want, got)
		}
	}
}
