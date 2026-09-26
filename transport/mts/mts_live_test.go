package mts

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

// Live end-to-end check against a real MTS Link Boards share link. Opt-in, since it needs
// internet access and a board that still accepts anonymous guests:
//
//	MTS_LIVE_BOARD=https://my.mts-link.ru/boards/board/<uid> go test ./transport/mts -run Live -v
func TestLiveGuestCursorRoundTrip(t *testing.T) {
	boardURL := os.Getenv("MTS_LIVE_BOARD")
	if boardURL == "" {
		t.Skip("set MTS_LIVE_BOARD to run the live MTS round trip")
	}
	cfg := transport.DefaultConfig()

	sender := NewTransport(boardURL, cfg)
	receiver := NewTransport(boardURL, cfg)

	got := make(chan []byte, 4)
	receiver.Receive(func(b []byte) { got <- b })

	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver start: %v", err)
	}
	defer receiver.Stop()
	if err := sender.Start(); err != nil {
		t.Fatalf("sender start: %v", err)
	}
	defer sender.Stop()

	waitConnected(t, sender, "sender")
	waitConnected(t, receiver, "receiver")

	payload := []byte("mts-live-round-trip-payload")
	waitForPayload(t, sender, got, payload)

	// A real tunnel packet is MTU-sized, not a few bytes: cursorPosition.x carries it
	// base64-encoded, so every size class has to survive the board's own limits intact.
	for _, size := range []int{200, 1400, 4000, 16000, 60000} {
		big := makePayload(size)
		waitForPayload(t, sender, got, big)
	}
}

func makePayload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

func waitForPayload(t *testing.T, sender *Transport, got chan []byte, want []byte) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := sender.Send(want); err != nil {
			t.Fatalf("send %d bytes: %v", len(want), err)
		}
		select {
		case b := <-got:
			if !bytes.Equal(b, want) {
				t.Fatalf("payload corrupted: sent %d bytes, got %d bytes (equal prefix=%d)",
					len(want), len(b), commonPrefix(b, want))
			}
			t.Logf("round trip ok: %d bytes", len(b))
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("no %d byte packet came back through the other guest within 30s", len(want))
}

func commonPrefix(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// TestLiveReconnectAfterDrop kills the socket mid-session and requires the transport to come
// back on its own and carry traffic again - the case a flaky mobile network produces.
func TestLiveReconnectAfterDrop(t *testing.T) {
	sender, _, got := livePair(t)
	waitForPayload(t, sender, got, makePayload(1400))

	s := sender.session.Load()
	if s == nil || s.Conn == nil {
		t.Fatal("no live session to drop")
	}
	before := sender.Stats().Reconnects
	if err := s.Conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	t.Log("socket dropped")

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if sender.IsConnected() && sender.Stats().Reconnects > before {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !sender.IsConnected() {
		t.Fatal("transport never reconnected after the socket was dropped")
	}
	stats := sender.Stats()
	t.Logf("reconnected: reconnects=%d uptime=%s", stats.Reconnects, stats.Uptime.Round(time.Second))

	// The other guest is still on the board, so a fresh session must find it again.
	waitForPayload(t, sender, got, makePayload(1400))
}

// TestLiveManyGuestsOnOneBoard is the nodeagent question: managed mode runs one transport per
// key, and keys can share a board. Every guest has to get a session, and a packet sent by one
// guest has to reach the others.
func TestLiveManyGuestsOnOneBoard(t *testing.T) {
	boardURL := os.Getenv("MTS_LIVE_BOARD")
	if boardURL == "" {
		t.Skip("set MTS_LIVE_BOARD to run the live MTS checks")
	}
	count := 20
	if v := os.Getenv("MTS_LIVE_GUESTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 1 {
			count = n
		}
	}

	cfg := transport.DefaultConfig()
	guests := make([]*Transport, count)
	sinks := make([]chan []byte, count)
	for i := range guests {
		guests[i] = NewTransport(boardURL, cfg)
		sink := make(chan []byte, 64)
		sinks[i] = sink
		guests[i].Receive(func(b []byte) {
			select {
			case sink <- b:
			default:
			}
		})
	}
	for i, g := range guests {
		if err := g.Start(); err != nil {
			t.Fatalf("guest %d start: %v", i, err)
		}
		defer g.Stop()
	}
	for i, g := range guests {
		waitConnected(t, g, fmt.Sprintf("guest %d", i))
	}
	t.Logf("%d/%d guests connected to the same board", count, count)

	// The last guest must be visible to the first one.
	want := makePayload(1400)
	waitForPayload(t, guests[count-1], sinks[0], want)
	t.Logf("guest %d -> guest 0 delivered", count-1)
}

// TestLiveSustainedRate holds a steady rate for a while instead of one burst. The board
// throttles or drops under sustained load long before it refuses a burst, so this is the
// shape that matters for a real tunnel.
func TestLiveSustainedRate(t *testing.T) {
	sender, _, got := livePair(t)

	const size = 1400
	ratePerSec := 100
	if v := os.Getenv("MTS_LIVE_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			ratePerSec = n
		}
	}
	seconds := 30
	if v := os.Getenv("MTS_LIVE_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			seconds = n
		}
	}
	total := ratePerSec * seconds

	start := time.Now()
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
		defer ticker.Stop()
		for i := 0; i < total; i++ {
			<-ticker.C
			if err := sender.Send(makePayload(size)); err != nil {
				return
			}
		}
	}()

	received := 0
	latencies := make([]time.Duration, 0, total)
	var lastArrival time.Time
	stalls := 0
	deadline := time.Now().Add(time.Duration(seconds+30) * time.Second)
	for received < total && time.Now().Before(deadline) {
		select {
		case <-got:
			now := time.Now()
			if !lastArrival.IsZero() {
				gap := now.Sub(lastArrival)
				if gap > 2*time.Second {
					stalls++
					t.Logf("stall: %.1fs with no packets after %d received", gap.Seconds(), received)
				}
			}
			lastArrival = now
			latencies = append(latencies, now.Sub(start))
			received++
		case <-time.After(3 * time.Second):
		}
	}
	elapsed := time.Since(start)
	rate := float64(received*size) / elapsed.Seconds() / 1024
	t.Logf("sent %d, received %d in %s = %.0f KB/s, stalls=%d",
		total, received, elapsed.Round(time.Millisecond), rate, stalls)
	if received == 0 {
		t.Fatal("nothing came back at all")
	}
}

// TestLiveRoundTripLatency measures the per-message round trip. A board that relays cursor
// updates with a delay would still pass every loss test while throttling any real TCP flow,
// because the flow's window is latency-bound rather than bandwidth-bound.
func TestLiveRoundTripLatency(t *testing.T) {
	sender, _, got := livePair(t)

	const probes = 20
	rtts := make([]time.Duration, 0, probes)
	for i := 0; i < probes; i++ {
		before := time.Now()
		if err := sender.Send(makePayload(1400)); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		select {
		case <-got:
			rtts = append(rtts, time.Since(before))
		case <-time.After(10 * time.Second):
			t.Fatalf("probe %d never came back", i)
		}
		time.Sleep(120 * time.Millisecond)
	}

	sort.Slice(rtts, func(a, b int) bool { return rtts[a] < rtts[b] })
	p50 := rtts[len(rtts)/2]
	p95 := rtts[len(rtts)*95/100]
	t.Logf("round trip over %d probes: min=%s p50=%s p95=%s max=%s",
		probes, rtts[0].Round(time.Millisecond), p50.Round(time.Millisecond),
		p95.Round(time.Millisecond), rtts[len(rtts)-1].Round(time.Millisecond))
}

// TestLiveOrdering is the test that matters most for TCP: the board has to relay our cursor
// frames in the order we sent them. Out-of-order delivery looks exactly like loss to a TCP
// stack - every reordered segment triggers a spurious retransmit - which pins the tunnel's
// congestion window at a handful of packets no matter how fast the board relays.
func TestLiveOrdering(t *testing.T) {
	sender, _, got := livePair(t)

	const count = 300
	for i := 0; i < count; i++ {
		p := makePayload(1400)
		binary.BigEndian.PutUint32(p[:4], uint32(i))
		if err := sender.Send(p); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	seen := make(map[uint32]bool, count)
	next := uint32(0)
	var outOfOrder, gaps int
	deadline := time.After(60 * time.Second)
	for len(seen) < count {
		select {
		case b := <-got:
			id := binary.BigEndian.Uint32(b[:4])
			if id >= count {
				t.Fatalf("got bogus id %d", id)
			}
			if id < next {
				outOfOrder++
			} else if id > next {
				gaps++
			}
			seen[id] = true
			for seen[next] {
				next++
			}
		case <-deadline:
			t.Fatalf("only %d/%d arrived", len(seen), count)
		}
	}
	t.Logf("%d packets: out-of-order=%d forward-gaps=%d", count, outOfOrder, gaps)
	if outOfOrder > 0 {
		t.Fatalf("%d packets arrived out of order", outOfOrder)
	}
}

// TestLiveLatencyUnderLoad probes round-trip latency while a heavy stream is running. If the
// board queues broadcasts under load, latency balloons exactly when throughput matters, and a
// TCP flow's window shrinks with it.
func TestLiveLatencyUnderLoad(t *testing.T) {
	sender, _, got := livePair(t)

	// The sink carries the load too, so a probe has to be told apart by its own marker;
	// otherwise it just reads a backlogged load packet and reports a 0ms round trip.
	probe := func() time.Duration {
		marker := make([]byte, 1400)
		copy(marker, "MTSPROBE")
		before := time.Now()
		if err := sender.Send(marker); err != nil {
			t.Fatalf("probe send: %v", err)
		}
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case b := <-got:
				if bytes.HasPrefix(b, []byte("MTSPROBE")) {
					return time.Since(before)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("probe never came back")
			}
		}
		return 0
	}

	idle := []time.Duration{probe(), probe(), probe(), probe(), probe()}
	sort.Slice(idle, func(a, b int) bool { return idle[a] < idle[b] })

	stop := make(chan struct{})
	var loadWG sync.WaitGroup
	loadWG.Add(1)
	go func() {
		defer loadWG.Done()
		ticker := time.NewTicker(2 * time.Millisecond) // ~700 packets/s
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_ = sender.Send(makePayload(1400))
			}
		}
	}()
	// Let the stream reach the board before probing.
	time.Sleep(2 * time.Second)

	loaded := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		loaded = append(loaded, probe())
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	loadWG.Wait()
	sort.Slice(loaded, func(a, b int) bool { return loaded[a] < loaded[b] })

	t.Logf("idle   p50=%s max=%s", idle[len(idle)/2].Round(time.Millisecond), idle[len(idle)-1].Round(time.Millisecond))
	t.Logf("loaded p50=%s p95=%s max=%s", loaded[len(loaded)/2].Round(time.Millisecond),
		loaded[len(loaded)*95/100].Round(time.Millisecond), loaded[len(loaded)-1].Round(time.Millisecond))
}

func waitConnected(t *testing.T, tr *Transport, label string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if tr.IsConnected() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s never connected", label)
}

func livePair(t *testing.T) (*Transport, *Transport, chan []byte) {
	t.Helper()
	if os.Getenv("MTS_LIVE_DEBUG") != "" {
		utils.EnableDebug()
	}
	boardURL := os.Getenv("MTS_LIVE_BOARD")
	if boardURL == "" {
		t.Skip("set MTS_LIVE_BOARD to run the live MTS checks")
	}
	cfg := transport.DefaultConfig()
	sender := NewTransport(boardURL, cfg)
	receiver := NewTransport(boardURL, cfg)
	got := make(chan []byte, 8192)
	receiver.Receive(func(b []byte) { got <- b })
	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver start: %v", err)
	}
	t.Cleanup(func() { receiver.Stop() })
	if err := sender.Start(); err != nil {
		t.Fatalf("sender start: %v", err)
	}
	t.Cleanup(func() { sender.Stop() })
	waitConnected(t, sender, "sender")
	waitConnected(t, receiver, "receiver")
	return sender, receiver, got
}

// TestLiveBurstDrain pushes as hard as the queue allows and measures what actually comes
// back. Nothing here sleeps between sends: the point is the transport's ceiling, not the
// test's pacing.
func TestLiveBurstDrain(t *testing.T) {
	sender, _, got := livePair(t)

	const size = 1400
	const burst = 3000

	// Unique payloads: a count alone would also be satisfied by duplicates or a frame the
	// decoder replayed, which is exactly the kind of corruption that shows up as a flaky tunnel.
	payloads := make([][]byte, burst)
	for i := range payloads {
		p := makePayload(size)
		binary.BigEndian.PutUint32(p[:4], uint32(i))
		payloads[i] = p
	}

	start := time.Now()
	sent := 0
	for i := 0; i < burst; i++ {
		if err := sender.Send(payloads[i]); err != nil {
			t.Fatalf("send %d rejected: %v", i, err)
		}
		sent++
	}
	queued := time.Since(start)

	seen := make(map[uint32]int, burst)
	received := 0
	duplicates := 0
	corrupt := 0
	firstReceive := time.Duration(0)
	deadline := time.Now().Add(120 * time.Second)
	for received < sent && time.Now().Before(deadline) {
		select {
		case b := <-got:
			if firstReceive == 0 {
				firstReceive = time.Since(start)
			}
			if len(b) != size {
				corrupt++
			} else {
				id := binary.BigEndian.Uint32(b[:4])
				if !bytes.Equal(b, payloads[id]) {
					corrupt++
				}
				seen[id]++
				if seen[id] > 1 {
					duplicates++
				}
			}
			received++
		case <-time.After(200 * time.Millisecond):
		}
	}
	total := time.Since(start)
	rate := float64(received*size) / 1024 / total.Seconds()
	t.Logf("queued %d packets in %s; first back after %s; %d received in %s = %.0f KB/s",
		sent, queued.Round(time.Millisecond), firstReceive.Round(time.Millisecond),
		received, total.Round(time.Millisecond), rate)
	if received < sent {
		t.Fatalf("burst lost packets: %d/%d made it back", received, sent)
	}
	if corrupt > 0 {
		t.Fatalf("%d packets came back wrong", corrupt)
	}
	if duplicates > 0 {
		t.Fatalf("%d packets came back more than once", duplicates)
	}
}

// TestLiveIdleStability holds a connection open across several keepalive intervals.
// mtsReadDeadline is 90s and the ping only goes out every 60s, so an off-by-one there
// would show up as a reconnect every couple of minutes.
func TestLiveIdleStability(t *testing.T) {
	if os.Getenv("MTS_LIVE_IDLE_SECONDS") == "" {
		t.Skip("set MTS_LIVE_IDLE_SECONDS (e.g. 200) to run the idle stability check")
	}
	idle, _ := strconv.Atoi(os.Getenv("MTS_LIVE_IDLE_SECONDS"))
	sender, _, got := livePair(t)

	deadline := time.Now().Add(time.Duration(idle) * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)
		if !sender.IsConnected() {
			t.Fatalf("sender dropped the connection after %s", time.Since(deadline.Add(-time.Duration(idle)*time.Second)).Round(time.Second))
		}
	}
	stats := sender.Stats()
	t.Logf("still connected after %ds: reconnects=%d sent=%d recv=%d",
		idle, stats.Reconnects, stats.PacketsSent, stats.PacketsRecv)
	if stats.Reconnects != 0 {
		t.Fatalf("reconnected %d times while idle", stats.Reconnects)
	}
	waitForPayload(t, sender, got, makePayload(1400))
}
