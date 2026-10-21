package outbound

import (
	"context"
	"errors"
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

func threeKeyPool(t *testing.T) (*Pool, map[string]*stubDialer) {
	t.Helper()
	dialers := map[string]*stubDialer{"a": {}, "b": {}, "c": {}}
	p := NewPool(PoolConfig{})
	var specs []KeySpec
	for _, id := range []string{"a", "b", "c"} {
		specs = append(specs, KeySpec{ID: id, Label: id, Dial: dialers[id]})
	}
	p.SetKeys(specs)
	return p, dialers
}

func TestSpreadDialPutsEachSubjectOnADifferentKey(t *testing.T) {
	p, dialers := threeKeyPool(t)
	for _, subject := range []string{"cupsonline|room-0", "cupsonline|room-1", "cupsonline|room-2"} {
		conn, err := p.DialSpread(context.Background(), subject, "tcp", "cups.online:443")
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}
	for id, d := range dialers {
		if d.calls != 1 {
			t.Fatalf("key %s was used %d times, want 1", id, d.calls)
		}
	}
}

func TestPenalizedKeyIsSkippedByTheNextSpreadDial(t *testing.T) {
	p, dialers := threeKeyPool(t)
	conn, err := p.DialSpread(context.Background(), "cupsonline|room-0", "tcp", "cups.online:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	first := p.Pinned("cupsonline|room-0")
	p.Penalize("cupsonline|room-0", errors.New("403"))
	if p.Pinned("cupsonline|room-0") != "" {
		t.Fatal("a penalized subject must lose its key")
	}
	conn, err = p.DialSpread(context.Background(), "cupsonline|room-0", "tcp", "cups.online:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if second := p.Pinned("cupsonline|room-0"); second == first || second == "" {
		t.Fatalf("retry stayed on %q after it was penalized", first)
	}
	if dialers[first].calls != 1 {
		t.Fatalf("penalized key dialed again: %d", dialers[first].calls)
	}
}

func TestRouterUsesTheHintedSubjectAndFeedbackReachesThePool(t *testing.T) {
	r := NewRouter()
	r.LoadCarrierHosts()
	pool, _ := threeKeyPool(t)
	r.SetPool("cupsonline", pool)
	direct, seen := recordingDirect(t)

	ctx := transport.WithDialHint(context.Background(), transport.DialHint{Subject: "room-3", Spread: true})
	conn, err := r.Dial(ctx, "tcp", "interview.cups.online:443", direct)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if len(*seen) != 0 {
		t.Fatalf("a carrier host must not go direct: %v", *seen)
	}
	if pool.Pinned("cupsonline|room-3") == "" {
		t.Fatal("hinted subject was not used")
	}

	feedback := r.HintFeedback()
	feedback("room-3", errors.New("403"))
	if pool.Pinned("cupsonline|room-3") != "" {
		t.Fatal("feedback did not release the key")
	}
}

func TestHintFeedbackThroughTheTransportHook(t *testing.T) {
	r := NewRouter()
	pool, _ := threeKeyPool(t)
	r.SetPool("cupsonline", pool)
	pool.pinned["cupsonline|room-9"] = "a"
	transport.SetDialFeedback(r.HintFeedback())
	defer transport.SetDialFeedback(nil)

	ctx := transport.WithDialHint(context.Background(), transport.DialHint{Subject: "room-9"})
	transport.ReportDial(ctx, errors.New("403"))
	if pool.Pinned("cupsonline|room-9") != "" {
		t.Fatal("ReportDial did not reach the pool")
	}
	transport.ReportDial(context.Background(), errors.New("403"))
}
