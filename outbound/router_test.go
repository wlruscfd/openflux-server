package outbound

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

func recordingDirect(t *testing.T) (transport.DialFunc, *[]string) {
	t.Helper()
	var seen []string
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		seen = append(seen, address)
		return nil, errors.New("direct dial recorded")
	}, &seen
}

func TestRouterLeavesLocalAddressesToTheDirectDialer(t *testing.T) {
	r := NewRouter()
	r.SetPool("yandex", poolWith(t, "a", &stubDialer{}))
	direct, seen := recordingDirect(t)
	for _, address := range []string{
		"127.0.0.1:41000", "localhost:8080", "[::1]:9000", "10.0.0.5:443",
	} {
		if _, err := r.Dial(context.Background(), "tcp", address, direct); err == nil {
			t.Fatalf("%s must not report success", address)
		}
	}
	if len(*seen) != 4 {
		t.Fatalf("direct saw %v", *seen)
	}
}

func TestRouterPicksThePoolOfTheCarrierHost(t *testing.T) {
	r := NewRouter()
	r.LoadCarrierHosts()
	yandex := poolWith(t, "a", &stubDialer{})
	boards := poolWith(t, "b", &stubDialer{})
	r.SetPool("yandex", yandex)
	r.SetPool("boards", boards)
	direct, seen := recordingDirect(t)
	conn, err := r.Dial(context.Background(), "tcp", "docs.yandex.ru:443", direct)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if len(*seen) != 0 {
		t.Fatalf("a carrier host must not go direct: %v", *seen)
	}
	if got := yandex.Pinned("yandex|docs.yandex.ru:443"); got != "a" {
		t.Fatalf("yandex pool pinned %q", got)
	}
	if boards.Len() == 0 {
		t.Fatal("boards pool disappeared")
	}
}

func TestRouterKeepsCarrierTypesApart(t *testing.T) {
	r := NewRouter()
	r.LoadCarrierHosts()
	pools := map[string]*Pool{}
	for _, name := range []string{"yandex", "boards", "mailru", "mts"} {
		p := poolWith(t, name, &stubDialer{})
		pools[name] = p
		r.SetPool(name, p)
	}
	direct, seen := recordingDirect(t)
	cases := []struct{ address, carrier string }{
		{"docs.yandex.ru:443", "yandex"},
		{"boards.yandex.ru:443", "boards"},
		{"cloud.mail.ru:443", "mailru"},
		{"wsboard2.mts-link.ru:443", "mts"},
	}
	for _, tc := range cases {
		conn, err := r.Dial(context.Background(), "tcp", tc.address, direct)
		if err != nil {
			t.Fatalf("%s: %v", tc.address, err)
		}
		conn.Close()
	}
	if len(*seen) != 0 {
		t.Fatalf("every carrier host has a pool and must not go direct: %v", *seen)
	}
	for _, tc := range cases {
		subject := tc.carrier + "|" + tc.address
		if got := pools[tc.carrier].Pinned(subject); got != tc.carrier {
			t.Fatalf("%s should have been pinned in the %s pool, got %q", tc.address, tc.carrier, got)
		}
		for other, p := range pools {
			if other == tc.carrier {
				continue
			}
			if p.Pinned(subject) != "" {
				t.Fatalf("%s leaked into the %s pool", subject, other)
			}
		}
	}
}

func TestRouterLearnsDynamicCarrierHosts(t *testing.T) {
	r := NewRouter()
	r.SetPool("telemost", poolWith(t, "s", &stubDialer{}))
	r.RegisterHost("telemost", "https://sfu-42.example.net:8443/path")
	direct, seen := recordingDirect(t)
	conn, err := r.Dial(context.Background(), "tcp", "sfu-42.example.net:8443", direct)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if len(*seen) != 0 {
		t.Fatalf("a registered host must not go direct: %v", *seen)
	}
}

func TestRouterExemptsHosts(t *testing.T) {
	r := NewRouter()
	r.SetPool("yandex", poolWith(t, "a", &stubDialer{}))
	r.RegisterHost("yandex", "docs.yandex.ru")
	r.Exempt("docs.yandex.ru")
	direct, seen := recordingDirect(t)
	if _, err := r.Dial(context.Background(), "tcp", "docs.yandex.ru:443", direct); err == nil {
		t.Fatal("an exempt host must not be proxied")
	}
	if len(*seen) != 1 {
		t.Fatalf("exempt host did not go direct: %v", *seen)
	}
}

func TestRouterFallsBackWhenNoPoolIsConfigured(t *testing.T) {
	r := NewRouter()
	r.LoadCarrierHosts()
	direct, seen := recordingDirect(t)
	if _, err := r.Dial(context.Background(), "tcp", "docs.yandex.ru:443", direct); err == nil {
		t.Fatal("expected the direct dial to report its error")
	}
	if len(*seen) != 1 {
		t.Fatalf("without a pool the carrier must go direct: %v", *seen)
	}
}

func TestRouterSurfacesPoolFailureInsteadOfLeakingDirect(t *testing.T) {
	r := NewRouter()
	r.LoadCarrierHosts()
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{{ID: "a", Label: "a", Dial: &stubDialer{err: errors.New("proxy down")}}})
	r.SetPool("yandex", p)
	direct, seen := recordingDirect(t)
	_, err := r.Dial(context.Background(), "tcp", "docs.yandex.ru:443", direct)
	if err == nil {
		t.Fatal("a broken proxy must not silently fall back to the blocked address")
	}
	if len(*seen) != 0 {
		t.Fatalf("the direct dialer must not be used when a pool exists: %v", *seen)
	}
}

func TestRouterLeavesNonTCPAlone(t *testing.T) {
	r := NewRouter()
	r.SetPool("yandex", poolWith(t, "a", &stubDialer{}))
	direct, seen := recordingDirect(t)
	r.Dial(context.Background(), "udp", "docs.yandex.ru:443", direct)
	if len(*seen) != 1 {
		t.Fatalf("udp must go direct: %v", *seen)
	}
}

func TestNormalizeHostStripsURLNoise(t *testing.T) {
	for raw, want := range map[string]string{
		"wss://push.yandex.ru:443/path": "push.yandex.ru",
		"HTTPS://Docs.Yandex.RU":        "docs.yandex.ru",
		"user@cloud.mail.ru:443":        "cloud.mail.ru",
		"[2001:db8::1]:443":             "2001:db8::1",
		"  disks.yandex.com  ":          "disks.yandex.com",
	} {
		if got := normalizeHost(raw); got != want {
			t.Fatalf("normalizeHost(%q) = %q, want %q", raw, got, want)
		}
	}
}

func poolWith(t *testing.T, id string, d Dialer) *Pool {
	t.Helper()
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{{ID: id, Label: id, Dial: d}})
	return p
}

func TestRouterDialDoesNotRecurseIntoItsOwnProxyPort(t *testing.T) {
	r := NewRouter()
	r.SetPool("yandex", poolWith(t, "a", &stubDialer{}))
	direct, seen := recordingDirect(t)
	start := time.Now()
	r.Dial(context.Background(), "tcp", "127.0.0.1:41000", direct)
	if time.Since(start) > time.Second {
		t.Fatal("the router must hand its own local socks port straight to the direct dialer")
	}
	if len(*seen) != 1 {
		t.Fatalf("the local socks port must never be proxied: %v", *seen)
	}
}
