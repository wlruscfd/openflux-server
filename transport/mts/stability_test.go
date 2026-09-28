package mts

import (
	"testing"
	"time"
)

// A 10-20s stall on mts had two causes in this file, both silent before: the keepalive loop ended
// itself on a failed ping and never ran again, and the reconnect backoff grew to a 15s ceiling so a
// brief board-side refusal cost many long retries.

func TestReconnectBackoffStaysShortEnoughToRecoverQuickly(t *testing.T) {
	// The old growth allowed a 8s base plus jitter, so a few refused reconnects added up to the
	// 10-20s stall that was reported. Keep every attempt under 8s, jitter included.
	for attempt := 1; attempt <= 12; attempt++ {
		d := reconnectBackoff(attempt)
		if d >= 8*time.Second {
			t.Errorf("attempt %d backoff = %v, want under 8s so a drop recovers quickly", attempt, d)
		}
	}
}

func TestReconnectBackoffGrowsMonotonically(t *testing.T) {
	prev := reconnectBackoff(1)
	for attempt := 2; attempt <= 4; attempt++ {
		cur := reconnectBackoff(attempt)
		if cur < prev {
			t.Errorf("attempt %d backoff = %v, less than the previous %v", attempt, cur, prev)
		}
		prev = cur
	}
}

// The boards transport runs a 20s ping against a comparable websocket board, which is the
// reference for how often a keepalive has to go out. mts must not be rarer than that.
const boardsComparablePingInterval = 20 * time.Second

func TestPingIntervalIsNoRarerThanBoards(t *testing.T) {
	if mtsPingInterval > boardsComparablePingInterval {
		t.Errorf("ping interval = %v, want at most %v so the board does not idle us out",
			mtsPingInterval, boardsComparablePingInterval)
	}
}
