package transport

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type stubTransport struct {
	startErr  error
	connectAt time.Duration
	started   atomic.Bool
	stopped   atomic.Bool
	began     time.Time
	cb        func([]byte)
}

func (s *stubTransport) Start() error {
	s.started.Store(true)
	s.began = time.Now()
	return s.startErr
}
func (s *stubTransport) Stop() error             { s.stopped.Store(true); return nil }
func (s *stubTransport) Send(data []byte) error  { return nil }
func (s *stubTransport) Receive(cb func([]byte)) { s.cb = cb }
func (s *stubTransport) IsConnected() bool {
	return s.started.Load() && s.connectAt >= 0 && time.Since(s.began) >= s.connectAt
}
func (s *stubTransport) Stats() TransportStats { return TransportStats{} }

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFallbackKeepsAPrimaryThatConnects(t *testing.T) {
	primary := &stubTransport{connectAt: 100 * time.Millisecond}
	alt := &stubTransport{}
	f := NewFallbackTransport(primary, func() Transport { return alt }, 2*time.Second, nil)
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "primary connected", f.IsConnected)
	time.Sleep(400 * time.Millisecond)
	if f.Switched() || alt.started.Load() {
		t.Fatal("a primary that connects must stay")
	}
}

func TestFallbackSwitchesWhenThePrimaryNeverConnects(t *testing.T) {
	primary := &stubTransport{connectAt: -1}
	alt := &stubTransport{}
	got := make(chan bool, 1)
	f := NewFallbackTransport(primary, func() Transport { return alt }, 600*time.Millisecond, func(ok bool) { got <- ok })
	var received func([]byte)
	f.Receive(func([]byte) {})
	_ = received
	if err := f.Start(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "switch", f.Switched)
	if !primary.stopped.Load() || !alt.started.Load() {
		t.Fatal("the primary must be stopped and the alternative started")
	}
	if alt.cb == nil {
		t.Fatal("the receive callback must follow to the alternative")
	}
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("the alternative connects, so the primary was at fault")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no verdict")
	}
}

func TestFallbackSwitchesAtOnceWhenThePrimaryCannotStart(t *testing.T) {
	primary := &stubTransport{startErr: errors.New("broken script")}
	alt := &stubTransport{}
	f := NewFallbackTransport(primary, func() Transport { return alt }, time.Minute, nil)
	if err := f.Start(); err != nil {
		t.Fatalf("the alternative started, so Start must succeed: %v", err)
	}
	if !f.Switched() || !alt.started.Load() {
		t.Fatal("must have switched")
	}
}

func TestFallbackStopStopsWhatRuns(t *testing.T) {
	primary := &stubTransport{connectAt: time.Millisecond}
	f := NewFallbackTransport(primary, func() Transport { return &stubTransport{} }, time.Second, nil)
	f.Start()
	if err := f.Stop(); err != nil || !primary.stopped.Load() {
		t.Fatalf("stop: %v stopped=%v", err, primary.stopped.Load())
	}
	if err := f.Stop(); err != nil {
		t.Fatal("a second Stop must be harmless")
	}
}
