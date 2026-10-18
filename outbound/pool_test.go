package outbound

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type scriptSocksServer struct {
	t       *testing.T
	ln      net.Listener
	want    string
	reply   byte
	seen    chan string
	errs    chan string
	closeFn func()
}

func newScriptSocksServer(t *testing.T, reply byte) *scriptSocksServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &scriptSocksServer{t: t, ln: ln, reply: reply, seen: make(chan string, 4), errs: make(chan string, 4)}
	go s.serve()
	return s
}

func (s *scriptSocksServer) failure() string {
	select {
	case e := <-s.errs:
		return e
	default:
		return ""
	}
}

func (s *scriptSocksServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			fail := func(stage string, err error) {
				select {
				case s.errs <- stage + ": " + err.Error():
				default:
				}
				c.Close()
			}
			greeting := make([]byte, 3)
			if _, err := io.ReadFull(c, greeting); err != nil {
				fail("greeting", err)
				return
			}
			if greeting[0] != socks5Version {
				fail("version", errors.New("not socks5"))
				return
			}
			if _, err := c.Write([]byte{socks5Version, socksNoAuth}); err != nil {
				fail("method reply", err)
				return
			}
			head := make([]byte, 4)
			if _, err := io.ReadFull(c, head); err != nil {
				fail("head", err)
				return
			}
			host, err := decodeSocksAddress(c, head[3])
			if err != nil {
				fail("address atyp="+itoa(int(head[3])), err)
				return
			}
			portBuf := make([]byte, 2)
			if _, err := io.ReadFull(c, portBuf); err != nil {
				fail("port", err)
				return
			}
			port := int(portBuf[0])<<8 | int(portBuf[1])
			target := net.JoinHostPort(host, itoa(port))
			select {
			case s.seen <- target:
			default:
			}
			reply := s.reply
			if _, err := c.Write([]byte{socks5Version, reply, 0x00, socksAtypIPv4, 127, 0, 0, 1, 0, 0}); err != nil {
				fail("reply", err)
				return
			}
			io.Copy(io.Discard, c)
		}(conn)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func (s *scriptSocksServer) addr() string { return s.ln.Addr().String() }

func TestSocksDialerReachesTargetThroughServer(t *testing.T) {
	srv := newScriptSocksServer(t, 0)
	defer srv.ln.Close()

	d := newSocksDialer(srv.addr(), directDialer{}, 5*time.Second)
	conn, err := d.DialContext(context.Background(), "tcp", "docs.yandex.ru:443")
	if err != nil {
		if srvErr := srv.failure(); srvErr != "" {
			t.Fatalf("client: %v; server: %s", err, srvErr)
		}
		t.Fatal(err)
	}
	conn.Close()
	select {
	case got := <-srv.seen:
		if got != "docs.yandex.ru:443" {
			t.Fatalf("proxy was asked for %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the SOCKS server never saw a connect request")
	}
}

func TestSocksDialerReportsRefused(t *testing.T) {
	srv := newScriptSocksServer(t, socksReplyConnRefused)
	defer srv.ln.Close()

	d := newSocksDialer(srv.addr(), directDialer{}, 5*time.Second)
	if _, err := d.DialContext(context.Background(), "tcp", "docs.yandex.ru:443"); err == nil {
		t.Fatal("a refused connect must not look like success")
	} else if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("error %q does not name the SOCKS reply", err)
	}
}

func TestSocksDialerRejectsNonTCP(t *testing.T) {
	d := newSocksDialer("127.0.0.1:1", directDialer{}, time.Second)
	if _, err := d.DialContext(context.Background(), "udp", "8.8.8.8:53"); err == nil {
		t.Fatal("udp must be refused: the carriers dial tcp")
	}
}

type stubDialer struct {
	mu      sync.Mutex
	latency time.Duration
	err     error
	calls   int
}

func (s *stubDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	s.mu.Lock()
	s.calls++
	latency, err := s.latency, s.err
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if latency > 0 {
		time.Sleep(latency)
	}
	c1, _ := net.Pipe()
	return c1, nil
}

func (s *stubDialer) set(err error, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err, s.latency = err, latency
}

func (s *stubDialer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestPoolPrefersFasterKey(t *testing.T) {
	slow := &stubDialer{latency: 80 * time.Millisecond}
	fast := &stubDialer{latency: 5 * time.Millisecond}
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{{ID: "slow", Label: "slow", Dial: slow}, {ID: "fast", Label: "fast", Dial: fast}})
	ctx := context.Background()
	p.Pin("slow-subject", "slow")
	if err := p.Probe(ctx, "slow-subject", "tcp", "a:1"); err != nil {
		t.Fatal(err)
	}
	p.Pin("fast-subject", "fast")
	if err := p.Probe(ctx, "fast-subject", "tcp", "a:1"); err != nil {
		t.Fatal(err)
	}
	p.Unpin("slow-subject")
	p.Unpin("fast-subject")
	p.DialContext(ctx, "subject-1", "tcp", "a:1")
	p.DialContext(ctx, "subject-2", "tcp", "a:1")
	if got := p.Pinned("subject-1"); got != "fast" {
		t.Fatalf("subject-1 pinned to %q, want fast", got)
	}
	if got := p.Pinned("subject-2"); got != "fast" {
		t.Fatalf("subject-2 pinned to %q, want fast", got)
	}
}

func TestPoolKeepsSubjectStickyAcrossDial(t *testing.T) {
	first := &stubDialer{}
	second := &stubDialer{}
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{{ID: "a", Label: "a", Dial: first}, {ID: "b", Label: "b", Dial: second}})
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		p.DialContext(ctx, "pinned", "tcp", "a:1")
	}
	if got := p.Pinned("pinned"); got == "" {
		t.Fatal("the subject lost its proxy key")
	}
	pinned := p.Pinned("pinned")
	chosen, other := first, second
	if pinned == "b" {
		chosen, other = second, first
	}
	if chosen.count() != 20 || other.count() != 0 {
		t.Fatalf("sticky violated: chosen=%d other=%d", chosen.count(), other.count())
	}
}

func TestPoolMovesSubjectWhenItsKeyFails(t *testing.T) {
	bad := &stubDialer{err: errors.New("no route to host")}
	good := &stubDialer{}
	p := NewPool(PoolConfig{Cooldown: time.Hour})
	p.SetKeys([]KeySpec{{ID: "bad", Label: "bad", Dial: bad}, {ID: "good", Label: "good", Dial: good}})
	ctx := context.Background()
	if _, err := p.DialContext(ctx, "subject", "tcp", "a:1"); err == nil {
		t.Fatal("a failing proxy must surface the error")
	}
	if got := p.Pinned("subject"); got == "bad" {
		t.Fatal("a failed proxy must not stay pinned")
	}
	if _, err := p.DialContext(ctx, "subject", "tcp", "a:1"); err != nil {
		t.Fatal(err)
	}
	if got := p.Pinned("subject"); got != "good" {
		t.Fatalf("subject moved to %q, want good", got)
	}
}

func TestPoolReportsErrNoProxyWithoutKeys(t *testing.T) {
	p := NewPool(PoolConfig{})
	if _, err := p.DialContext(context.Background(), "s", "tcp", "a:1"); !errors.Is(err, ErrNoProxy) {
		t.Fatalf("got %v, want ErrNoProxy", err)
	}
}

func TestPoolReportsErrProxyBannedWhenCooling(t *testing.T) {
	bad := &stubDialer{err: errors.New("down")}
	p := NewPool(PoolConfig{Cooldown: time.Hour})
	p.SetKeys([]KeySpec{{ID: "bad", Label: "bad", Dial: bad}})
	ctx := context.Background()
	p.DialContext(ctx, "s", "tcp", "a:1")
	if _, err := p.DialContext(ctx, "s", "tcp", "a:1"); !errors.Is(err, ErrProxyBanned) {
		t.Fatalf("got %v, want ErrProxyBanned", err)
	}
}

func TestPoolStopsFlappingAfterRepeatedMoves(t *testing.T) {
	first := &stubDialer{err: errors.New("down")}
	second := &stubDialer{err: errors.New("down")}
	p := NewPool(PoolConfig{Cooldown: time.Second, RebalanceHold: time.Hour, FlapThreshold: 3})
	p.SetKeys([]KeySpec{{ID: "a", Label: "a", Dial: first}, {ID: "b", Label: "b", Dial: second}})
	ctx := context.Background()
	sawBan := false
	for i := 0; i < 10; i++ {
		if _, err := p.DialContext(ctx, "s", "tcp", "a:1"); errors.Is(err, ErrProxyBanned) {
			sawBan = true
			break
		}
	}
	if !sawBan {
		t.Fatal("a subject must stop being moved once every key keeps failing")
	}
}

func TestPoolSetKeysKeepsHealthOfSurvivingKeys(t *testing.T) {
	good := &stubDialer{}
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{{ID: "good", Label: "good", Dial: good}})
	ctx := context.Background()
	if _, err := p.DialContext(ctx, "s", "tcp", "a:1"); err != nil {
		t.Fatal(err)
	}
	p.SetKeys([]KeySpec{{ID: "good", Label: "good", Dial: good}, {ID: "new", Label: "new", Dial: &stubDialer{}}})
	for _, snap := range p.Snapshots() {
		if snap.ID != "good" {
			continue
		}
		if !snap.Probed || !snap.Healthy || snap.Dials != 1 {
			t.Fatalf("a surviving key lost its health when the set changed: %+v", snap)
		}
		return
	}
	t.Fatal("the key vanished from the pool")
}

func TestPoolSnapshotsCoverEveryKey(t *testing.T) {
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{
		{ID: "b", Label: "b", Dial: &stubDialer{}},
		{ID: "a", Label: "a", Dial: &stubDialer{}},
	})
	snaps := p.Snapshots()
	if len(snaps) != 2 || snaps[0].ID != "a" || snaps[1].ID != "b" {
		t.Fatalf("snapshots %+v", snaps)
	}
}

func TestPoolProbeMarksKeyUsable(t *testing.T) {
	d := &stubDialer{}
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{{ID: "a", Label: "a", Dial: d}})
	if err := p.Probe(context.Background(), "probe", "tcp", "docs.yandex.ru:443"); err != nil {
		t.Fatal(err)
	}
	snaps := p.Snapshots()
	if !snaps[0].Healthy || !snaps[0].Probed {
		t.Fatalf("probe did not mark the key healthy: %+v", snaps[0])
	}
	if got := p.Pinned("probe"); got != "" {
		t.Fatalf("a background probe must not claim a subject, pinned %q", got)
	}
}

func TestPoolPinRejectsUnknownKey(t *testing.T) {
	p := NewPool(PoolConfig{})
	p.SetKeys([]KeySpec{{ID: "a", Label: "a", Dial: &stubDialer{}}})
	p.Pin("s", "ghost")
	if got := p.Pinned("s"); got != "" {
		t.Fatalf("pinned to an unknown key %q", got)
	}
}
