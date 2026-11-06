package avito

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

func init() { os.Setenv("OPENFLUX_HOST_RATE", "code.avito.ru=1000") }

type fakeAvito struct {
	srv *httptest.Server

	mu       sync.Mutex
	docs     map[string]*fakeDoc
	created  int
	nextConn int
	roomHits int
	cookie   string
}

type fakeDoc struct {
	ver     int
	made    bool
	data    string
	members map[*fakeMember]bool
}

type fakeMember struct {
	id string
	ws *websocket.Conn
	mu sync.Mutex
}

func (m *fakeMember) send(v interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.ws.WriteJSON(v)
}

func newFakeAvito(t *testing.T) *fakeAvito {
	t.Helper()
	f := &fakeAvito{docs: map[string]*fakeDoc{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/web/1/code/room", f.room)
	mux.HandleFunc("/sharedb", f.sharedb)
	f.srv = httptest.NewServer(mux)
	old := baseURL
	baseURL = f.srv.URL
	t.Cleanup(func() {
		baseURL = old
		f.srv.Close()
	})
	return f
}

func (f *fakeAvito) room(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.roomHits++
	f.mu.Unlock()
	if !strings.Contains(r.Header.Get("Cookie"), "sid=ok") {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	f.mu.Lock()
	f.created++
	id := fmt.Sprintf("room%012d", f.created)
	f.docs[id] = &fakeDoc{members: map[*fakeMember]bool{}}
	f.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{"room": map[string]string{"id": id}})
}

func (f *fakeAvito) sharedb(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{}
	ws, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.cookie = r.Header.Get("Cookie")
	f.nextConn++
	m := &fakeMember{id: fmt.Sprintf("conn%d", f.nextConn), ws: ws}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		for _, d := range f.docs {
			delete(d.members, m)
		}
		f.mu.Unlock()
		ws.Close()
	}()
	m.send(map[string]string{"id": m.id})
	for {
		var msg map[string]interface{}
		if err := ws.ReadJSON(&msg); err != nil {
			return
		}
		room, _ := msg["d"].(string)
		f.mu.Lock()
		doc := f.docs[room]
		f.mu.Unlock()
		if doc == nil {
			m.send(map[string]interface{}{"error": map[string]string{"message": "no such room"}})
			continue
		}
		switch msg["a"] {
		case "s":
			f.mu.Lock()
			doc.members[m] = true
			reply := map[string]interface{}{"a": "s", "data": map[string]interface{}{"v": doc.ver, "data": doc.data}}
			if doc.made {
				reply["data"].(map[string]interface{})["type"] = shareDBType
			}
			f.mu.Unlock()
			m.send(reply)
		case "op":
			f.mu.Lock()
			if msg["create"] != nil {
				doc.made = true
				doc.ver = 1
				f.mu.Unlock()
				m.send(map[string]interface{}{"a": "op", "src": m.id, "v": 0})
				continue
			}
			ops, _ := msg["op"].([]interface{})
			if len(ops) > 0 {
				if last, ok := ops[len(ops)-1].(string); ok {
					doc.data = last
				}
			}
			ver := doc.ver
			doc.ver++
			var peers []*fakeMember
			for p := range doc.members {
				if p != m {
					peers = append(peers, p)
				}
			}
			f.mu.Unlock()
			for _, p := range peers {
				p.send(map[string]interface{}{"a": "op", "d": room, "src": m.id, "v": ver, "op": ops})
			}
			m.send(map[string]interface{}{"a": "op", "d": room, "src": m.id, "seq": msg["seq"], "v": ver})
		}
	}
}

type endpoint struct {
	t    *AvitoTransport
	mu   sync.Mutex
	got  [][]byte
	seen atomic.Int64
}

func start(t *testing.T, url, cookies string, client bool) *endpoint {
	t.Helper()
	e := &endpoint{t: NewAvitoTransport(url, transport.DefaultConfig(), cookies, client)}
	e.t.config.RoomCreatePause = time.Millisecond
	e.t.Receive(func(p []byte) {
		e.mu.Lock()
		e.got = append(e.got, append([]byte(nil), p...))
		e.mu.Unlock()
		e.seen.Add(1)
	})
	if err := e.t.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { e.t.Stop() })
	return e
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestExitCreatesRoomsAndFramesCrossBothWays(t *testing.T) {
	f := newFakeAvito(t)
	exit := start(t, "", "sid=ok", false)

	list := exit.t.RoomList()
	if list == "" {
		t.Fatal("exit published no room list")
	}
	if got := len(parseRoomList(list)); got != exit.t.config.NumRooms*2 {
		t.Fatalf("rooms = %d, want %d", got, exit.t.config.NumRooms*2)
	}
	client := start(t, list, "", true)

	waitFor(t, "both sides connected", func() bool {
		for _, e := range []*endpoint{exit, client} {
			for _, ws := range e.t.allWS() {
				if !ws.connected.Load() {
					return false
				}
			}
		}
		return true
	})

	for i := 0; i < 20; i++ {
		if err := exit.t.Send([]byte(fmt.Sprintf("down-%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := client.t.Send([]byte(fmt.Sprintf("up-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "20 frames each way", func() bool { return exit.seen.Load() == 20 && client.seen.Load() == 20 })
	if f.created != exit.t.config.NumRooms*2 {
		t.Fatalf("created %d rooms", f.created)
	}
}

func TestRejectedCookiesFailFastWithoutRetryingEveryRoom(t *testing.T) {
	f := newFakeAvito(t)
	tr := NewAvitoTransport("", transport.DefaultConfig(), "sid=bad", false)
	tr.config.RoomCreatePause = time.Millisecond
	err := tr.Start()
	if !errors.Is(err, errAuth) {
		t.Fatalf("Start = %v, want errAuth", err)
	}
	if f.roomHits != 1 {
		t.Fatalf("%d room requests for rejected cookies, want 1", f.roomHits)
	}
}

func TestPingsKeepAnIdleRoomConnected(t *testing.T) {
	newFakeAvito(t)
	exit := NewAvitoTransport("", transport.DefaultConfig(), "sid=ok", false)
	exit.config.RoomCreatePause = time.Millisecond
	exit.config.NumRooms = 1
	exit.config.WSReadTimeout = 400 * time.Millisecond
	exit.config.WSPingInterval = 80 * time.Millisecond
	if err := exit.Start(); err != nil {
		t.Fatal(err)
	}
	defer exit.Stop()
	waitFor(t, "connected", func() bool { return exit.IsConnected() })
	time.Sleep(1500 * time.Millisecond)
	for _, ws := range exit.allWS() {
		if n := ws.stats.reconnects.Load(); n != 0 {
			t.Fatalf("idle room reconnected %d times despite pings", n)
		}
	}
}

func TestFleetSizeShrinksRoomsPerKey(t *testing.T) {
	defer SetFleetSize(1)
	for keys, want := range map[int]int{1: 4, 4: 4, 5: 3, 8: 2, 16: 1, 100: 1} {
		SetFleetSize(keys)
		if got := DefaultAvitoConfig().NumRooms; got != want {
			t.Errorf("%d keys: %d rooms, want %d", keys, got, want)
		}
	}
}

func TestExitWithoutCookiesDoesNotWaitOrCallTheServer(t *testing.T) {
	f := newFakeAvito(t)
	tr := NewAvitoTransport("", transport.DefaultConfig(), "", false)
	if err := tr.Start(); !errors.Is(err, errNoCookie) {
		t.Fatalf("Start = %v, want errNoCookie", err)
	}
	if f.roomHits != 0 {
		t.Fatalf("%d requests without cookies", f.roomHits)
	}
}

func TestProvidedCookiesReachTheRoomsOnReconnect(t *testing.T) {
	f := newFakeAvito(t)
	exit := start(t, "", "sid=ok", false)
	waitFor(t, "connected", func() bool { return exit.t.IsConnected() })

	exit.t.ProvideCookies("sid=ok; fresh=1")
	waitFor(t, "rooms reconnect with the new cookies", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.cookie == "sid=ok; fresh=1"
	})
}
