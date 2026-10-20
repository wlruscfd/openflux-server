package nodeagent

import "testing"

// TestFailedStartBacksOffBeforeTryingAgain guards the pace of a key the
// provider is refusing.
//
// A key that cannot start is retried on every reconcile otherwise, and a
// provider that throttles this node answers every attempt with an error, so
// the retry keeps the block alive and the key never gets a document.
func TestFailedStartBacksOffBeforeTryingAgain(t *testing.T) {
	o := &Orchestrator{}

	if o.startBackoffActive("key-1") {
		t.Fatal("a key that never failed must be startable right away")
	}

	o.noteStartFailure("key-1")
	if !o.startBackoffActive("key-1") {
		t.Fatal("a key that just failed must wait before it is tried again")
	}
	if o.startBackoffActive("key-2") {
		t.Fatal("the backoff of one key must not hold back another")
	}

	o.clearStartFailure("key-1")
	if o.startBackoffActive("key-1") {
		t.Fatal("a key that started must be startable again")
	}
}
