package transport

import (
	"testing"
	"time"
)

func TestHostRatesKeepDefaultsAndTakeEnvOverrides(t *testing.T) {
	t.Setenv("OPENFLUX_HOST_RATE", "cloud.mail.ru=1, example.org=3, broken, bad=x")
	rates := hostRates()
	if rates["cloud.mail.ru"] != 1 || rates["example.org"] != 3 {
		t.Fatalf("overrides not applied: %v", rates)
	}
	if rates["code.avito.ru"] != defaultHostRates["code.avito.ru"] {
		t.Fatalf("default lost: %v", rates)
	}
}

func TestHostGateSpacesRequestsAndLetsUnknownHostsThrough(t *testing.T) {
	if !WaitHostSlot("no-limit.example", nil) {
		t.Fatal("unlimited host was held")
	}
	g := newConnectGate(50)
	start := time.Now()
	for i := 0; i < 4; i++ {
		g.wait(nil)
	}
	if elapsed := time.Since(start); elapsed < 55*time.Millisecond {
		t.Fatalf("4 slots at 50/s took %v, want at least 3 intervals", elapsed)
	}
}
