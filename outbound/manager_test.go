package outbound

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

type fakeSidecar struct {
	mu       sync.Mutex
	applied  []Key
	stopped  bool
	applyErr error
}

func (f *fakeSidecar) Apply(keys []Key) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.applyErr != nil {
		return f.applyErr
	}
	f.applied = append([]Key(nil), keys...)
	return nil
}

func (f *fakeSidecar) DialerFor(keyID string) Dialer {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.applied {
		if k.ID == keyID {
			return &stubDialer{}
		}
	}
	return nil
}

func (f *fakeSidecar) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = true
}

func (f *fakeSidecar) keys() []Key {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Key(nil), f.applied...)
}

func managerWith(f *fakeSidecar) *Manager {
	return &Manager{router: NewRouter(), sidecar: f}
}

func testKey(id, name, server string) Key {
	return Key{ID: id, Name: name, Kind: KindShadowsocks, Server: server, Port: 443,
		Method: "aes-256-gcm", Password: "pw"}
}

func TestManagerBuildsOnePoolPerCarrier(t *testing.T) {
	m := managerWith(&fakeSidecar{})
	m.LoadCarrierHosts()
	err := m.Apply(map[string][]Key{
		"yandex": {testKey("y1", "yandex-a", "a.example"), testKey("y2", "yandex-b", "b.example")},
		"mailru": {testKey("m1", "mailru-a", "c.example")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Router().Pool("yandex").Len(); got != 2 {
		t.Fatalf("yandex pool has %d keys, want 2", got)
	}
	if got := m.Router().Pool("mailru").Len(); got != 1 {
		t.Fatalf("mailru pool has %d keys, want 1", got)
	}
	if got := m.Router().Pool("mts"); got != nil {
		t.Fatal("a carrier nobody configured must not get a pool")
	}
}

func TestManagerCapsKeysPerCarrier(t *testing.T) {
	f := &fakeSidecar{}
	m := managerWith(f)
	m.LoadCarrierHosts()
	keys := make([]Key, 0, 200)
	for i := 0; i < 200; i++ {
		keys = append(keys, testKey(fmt.Sprintf("k%d", i), fmt.Sprintf("key-%d", i), "a.example"))
	}
	if err := m.Apply(map[string][]Key{"yandex": keys}); err != nil {
		t.Fatal(err)
	}
	if got := m.Router().Pool("yandex").Len(); got != MaxKeysPerCarrier {
		t.Fatalf("pool kept %d keys, want the cap of %d", got, MaxKeysPerCarrier)
	}
	if got := len(f.keys()); got != MaxKeysPerCarrier {
		t.Fatalf("sing-box was given %d keys, want %d", got, MaxKeysPerCarrier)
	}
}

func TestManagerKeepsTheFirstKeysOfAnOverlongList(t *testing.T) {
	m := managerWith(&fakeSidecar{})
	m.LoadCarrierHosts()
	keys := []Key{testKey("first", "first", "a.example"), testKey("second", "second", "a.example")}
	for i := 0; i < MaxKeysPerCarrier+10; i++ {
		keys = append(keys, testKey(fmt.Sprintf("x%d", i), "x", "a.example"))
	}
	if err := m.Apply(map[string][]Key{"yandex": keys}); err != nil {
		t.Fatal(err)
	}
	if got := m.Router().Pool("yandex").Pinned("yandex|docs.yandex.ru:443"); got != "" {
		t.Fatalf("unexpected pin %q", got)
	}
	conn, err := m.Router().Pool("yandex").DialContext(context.Background(), "yandex|docs.yandex.ru:443", "tcp", "docs.yandex.ru:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := m.Router().Pool("yandex").Pinned("yandex|docs.yandex.ru:443"); got != "first" {
		t.Fatalf("pinned %q, want the first key of the list", got)
	}
}

func TestManagerApplyIsDeterministic(t *testing.T) {
	m := managerWith(&fakeSidecar{})
	m.LoadCarrierHosts()
	build := func() []string {
		if err := m.Apply(map[string][]Key{
			"yandex": {testKey("y", "y", "a.example")},
			"mailru": {testKey("m", "m", "b.example")},
			"boards": {testKey("b", "b", "c.example")},
		}); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, carrier := range m.Router().CarrierTypes() {
			out = append(out, carrier)
		}
		sort.Strings(out)
		return out
	}
	first := build()
	for i := 0; i < 5; i++ {
		got := build()
		if len(got) != len(first) {
			t.Fatalf("run %d saw %v, want %v", i, got, first)
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("run %d saw %v, want %v", i, got, first)
			}
		}
	}
}

func TestManagerSharesOneSidecarForEveryCarrier(t *testing.T) {
	f := &fakeSidecar{}
	m := managerWith(f)
	err := m.Apply(map[string][]Key{
		"yandex": {testKey("y1", "a", "a.example"), testKey("y2", "b", "b.example")},
		"mailru": {testKey("m1", "c", "c.example"), testKey("y1", "dup", "a.example")},
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, k := range f.keys() {
		if ids[k.ID] {
			t.Fatalf("key %s was handed to sing-box twice", k.ID)
		}
		ids[k.ID] = true
	}
	if len(ids) != 3 {
		t.Fatalf("sing-box got %d unique keys, want 3", len(ids))
	}
}

func TestManagerRoutesEachCarrierToItsOwnPool(t *testing.T) {
	m := managerWith(&fakeSidecar{})
	m.LoadCarrierHosts()
	err := m.Apply(map[string][]Key{
		"yandex": {testKey("y1", "yandex-a", "a.example")},
		"mailru": {testKey("m1", "mailru-a", "b.example")},
	})
	if err != nil {
		t.Fatal(err)
	}
	direct := func(ctx context.Context, network, address string) (net.Conn, error) {
		return nil, errors.New("direct must not be used")
	}
	for _, tc := range []struct{ address, carrier string }{
		{"docs.yandex.ru:443", "yandex"},
		{"cloud.mail.ru:443", "mailru"},
	} {
		conn, err := m.Router().Dial(context.Background(), "tcp", tc.address, direct)
		if err != nil {
			t.Fatalf("%s: %v", tc.address, err)
		}
		conn.Close()
		subject := tc.carrier + "|" + tc.address
		if got := m.Router().Pool(tc.carrier).Pinned(subject); got == "" {
			t.Fatalf("%s was not pinned in the %s pool", tc.address, tc.carrier)
		}
	}
}

func TestManagerFailsWithoutChangingPoolsWhenTheSidecarRefuses(t *testing.T) {
	m := managerWith(&fakeSidecar{})
	m.LoadCarrierHosts()
	if err := m.Apply(map[string][]Key{"yandex": {testKey("y1", "a", "a.example")}}); err != nil {
		t.Fatal(err)
	}
	before := m.Router().Pool("yandex").Len()
	m.sidecar = &fakeSidecar{applyErr: errors.New("sing-box is missing")}
	if err := m.Apply(map[string][]Key{"yandex": {testKey("y2", "b", "b.example")}}); err == nil {
		t.Fatal("a failing sidecar must surface the error")
	}
	if got := m.Router().Pool("yandex").Len(); got != before {
		t.Fatalf("pools changed after a failed apply: %d -> %d", before, got)
	}
}

func TestManagerInstallAndUninstallTheHook(t *testing.T) {
	f := &fakeSidecar{}
	m := managerWith(f)
	m.LoadCarrierHosts()
	if err := m.Apply(map[string][]Key{"yandex": {testKey("y1", "a", "a.example")}}); err != nil {
		t.Fatal(err)
	}
	transport.SetDialHook(nil)
	m.Install()
	if !transport.DialHookInstalled() {
		t.Fatal("Install did not register the dial hook")
	}
	m.Stop()
	if transport.DialHookInstalled() {
		t.Fatal("Stop left the dial hook in place")
	}
	if !f.stopped {
		t.Fatal("Stop must stop the sidecar")
	}
}

func TestManagerKeepsLoopbackOnTheDirectDialer(t *testing.T) {
	m := managerWith(&fakeSidecar{})
	m.LoadCarrierHosts()
	if err := m.Apply(map[string][]Key{"yandex": {testKey("y1", "a", "a.example")}}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
		}
	}()
	conn, err := transport.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("loopback must not be proxied: %v", err)
	}
	conn.Close()
}

func TestManagerAppliesKeyPriority(t *testing.T) {
	m := managerWith(&fakeSidecar{})
	weighted := testKey("w", "weighted", "a.example")
	weighted.Priority = 5
	if err := m.Apply(map[string][]Key{"yandex": {weighted}}); err != nil {
		t.Fatal(err)
	}
	pool := m.Router().Pool("yandex")
	conn, err := pool.DialContext(context.Background(), "yandex|docs.yandex.ru:443", "tcp", "docs.yandex.ru:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := pool.Pinned("yandex|docs.yandex.ru:443"); got != "w" {
		t.Fatalf("pinned %q", got)
	}
}
