package outbound

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestRouterPinRestoresAMemorizedAssignment(t *testing.T) {
	r := NewRouter()
	r.LoadCarrierHosts()
	pool := NewPool(PoolConfig{})
	pool.SetKeys([]KeySpec{{ID: "new", Label: "new", Dial: &stubDialer{}}, {ID: "old", Label: "old", Dial: &stubDialer{}}})
	r.SetPool("yandex", pool)
	r.Pin("yandex|docs.yandex.ru:443", "old")
	direct := func(ctx context.Context, network, address string) (net.Conn, error) {
		return nil, errors.New("direct")
	}
	conn, err := r.Dial(context.Background(), "tcp", "docs.yandex.ru:443", direct)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := pool.Pinned("yandex|docs.yandex.ru:443"); got != "old" {
		t.Fatalf("restored pin is %q, want old", got)
	}
}

func TestRouterPinIgnoresUnknownCarriersAndKeys(t *testing.T) {
	r := NewRouter()
	pool := poolWith(t, "a", &stubDialer{})
	r.SetPool("yandex", pool)
	r.Pin("nosuchcarrier|docs.yandex.ru:443", "a")
	r.Pin("yandex|docs.yandex.ru:443", "ghost")
	if got := r.Assignments(); len(got) != 0 {
		t.Fatalf("a pin that names nothing must not be remembered: %v", got)
	}
}

func TestRouterPinIgnoresASubjectWithoutACarrier(t *testing.T) {
	r := NewRouter()
	r.SetPool("yandex", poolWith(t, "a", &stubDialer{}))
	r.Pin("docs.yandex.ru:443", "a")
	if got := r.Assignments(); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestRouterAssignmentsCollectsEveryPool(t *testing.T) {
	r := NewRouter()
	r.SetPool("yandex", poolWith(t, "y1", &stubDialer{}))
	r.SetPool("mailru", poolWith(t, "m1", &stubDialer{}))
	r.Pin("yandex|docs.yandex.ru:443", "y1")
	r.Pin("mailru|cloud.mail.ru:443", "m1")
	got := r.Assignments()
	if got["yandex|docs.yandex.ru:443"] != "y1" || got["mailru|cloud.mail.ru:443"] != "m1" {
		t.Fatalf("got %v", got)
	}
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestUnpinClearsTheAssignment(t *testing.T) {
	r := NewRouter()
	pool := poolWith(t, "a", &stubDialer{})
	r.SetPool("yandex", pool)
	r.Pin("yandex|docs.yandex.ru:443", "a")
	pool.Unpin("yandex|docs.yandex.ru:443")
	if got := r.Assignments(); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}
