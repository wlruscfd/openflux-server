package store

import (
	"strings"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

func TestUpdateKeyAssignmentsOnlyTouchWhatWasSent(t *testing.T) {
	set, args := UpdateKeyParams{Label: ptr("new-label")}.assignments()
	if !strings.HasPrefix(set, "label = $1") {
		t.Fatalf("expected only label in SET, got %q", set)
	}
	if !strings.Contains(set, "updated_at = now()") {
		t.Error("updated_at must always be touched")
	}
	if len(args) != 1 || args[0] != "new-label" {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestUpdateKeyAssignmentsEmptyMeansNoOp(t *testing.T) {
	set, args := UpdateKeyParams{}.assignments()
	if set != "" || len(args) != 0 {
		t.Fatalf("empty update should produce nothing, got %q %#v", set, args)
	}
}

func TestUpdateKeyAssignmentsNumbersPlaceholdersInOrder(t *testing.T) {
	limit := int64(1024)
	enabled := false
	p := UpdateKeyParams{
		Label:             ptr("l"),
		Transport:         ptr("mts"),
		TrafficLimitBytes: &OptionalInt64{Set: true, Value: &limit},
		Enabled:           &enabled,
	}
	set, args := p.assignments()
	for _, want := range []string{"label = $1", "transport = $2", "traffic_limit_bytes = $3", "enabled = $4"} {
		if !strings.Contains(set, want) {
			t.Errorf("SET clause %q missing %q", set, want)
		}
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}
	if args[2] != &limit {
		t.Error("traffic limit arg must be the pointer, so SQL NULL clears the limit")
	}
}

func TestUpdateKeyNullsAreDistinguishedFromAbsent(t *testing.T) {
	var noNode *string
	clear := UpdateKeyParams{FinalExitNodeID: &noNode}
	set, args := clear.assignments()
	if !strings.Contains(set, "final_exit_node_id = $1") {
		t.Fatalf("explicit null must still produce a SET entry, got %q", set)
	}
	if args[0] != (*string)(nil) {
		t.Errorf("expected a nil *string arg, got %#v", args[0])
	}

	if set, _ := (UpdateKeyParams{}).assignments(); strings.Contains(set, "final_exit_node_id") {
		t.Error("absent field must not appear in SET")
	}
}

func TestUpdateKeyExpiresAtRoundTrip(t *testing.T) {
	when := time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC)
	var expires *time.Time = &when
	set, args := UpdateKeyParams{ExpiresAt: &expires}.assignments()
	if !strings.Contains(set, "expires_at = $1") {
		t.Fatalf("SET clause missing expires_at: %q", set)
	}
	got, ok := args[0].(*time.Time)
	if !ok {
		t.Fatalf("expires_at arg has type %T", args[0])
	}
	if got == nil || !got.Equal(when) {
		t.Errorf("expires_at arg did not carry the timestamp: %#v", got)
	}
}

func TestUpdateKeyDocURLsTouched(t *testing.T) {
	urls := []string{"https://example/1"}
	if (UpdateKeyParams{Label: ptr("x")}).docURLsTouched() {
		t.Error("a label-only edit must not re-check the document link")
	}
	if !(UpdateKeyParams{DocURL: ptr("https://example/2")}).docURLsTouched() {
		t.Error("doc_url change must re-check the document link")
	}
	if !(UpdateKeyParams{DocURLs: &urls}).docURLsTouched() {
		t.Error("doc_urls change must re-check the document link")
	}
}

func TestUpdateKeyURLSetCollectsBothForms(t *testing.T) {
	urls := []string{"https://example/a", "https://example/b"}
	set := UpdateKeyParams{DocURL: ptr("https://example/main"), DocURLs: &urls}.urlSet()
	if len(set) != 3 {
		t.Fatalf("expected 3 urls, got %#v", set)
	}
	found := map[string]bool{}
	for _, u := range set {
		found[u] = true
	}
	for _, want := range []string{"https://example/main", "https://example/a", "https://example/b"} {
		if !found[want] {
			t.Errorf("urlSet missing %q", want)
		}
	}
}
