package transport

import (
	"testing"
	"time"
)

func TestConnectGateSpacesAttempts(t *testing.T) {
	g := newConnectGate(50)
	start := time.Now()
	for i := 0; i < 11; i++ {
		if !g.wait(nil) {
			t.Fatal("wait reported done")
		}
	}
	if elapsed := time.Since(start); elapsed < 190*time.Millisecond {
		t.Fatalf("11 attempts at 50/s took %v, want at least 200ms", elapsed)
	}
}

func TestConnectGateStopsWhenDone(t *testing.T) {
	g := newConnectGate(1)
	g.wait(nil)
	done := make(chan struct{})
	close(done)
	if g.wait(done) {
		t.Fatal("a closed done channel must cancel the wait")
	}
}

func TestConnectGateDisabledAtZero(t *testing.T) {
	g := newConnectGate(0)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		g.wait(nil)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("a disabled gate must not delay")
	}
}
