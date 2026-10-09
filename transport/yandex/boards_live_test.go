package yandex

import (
	"crypto/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

func TestBoardsLiveThroughput(t *testing.T) {
	boardURL := os.Getenv("BOARDS_LIVE_URL")
	if boardURL == "" {
		t.Skip("set BOARDS_LIVE_URL to a disposable Yandex Boards guest link to measure throughput")
	}
	seconds := 15
	if v, err := strconv.Atoi(os.Getenv("BOARDS_LIVE_SECONDS")); err == nil && v > 0 {
		seconds = v
	}
	packet := 1400

	cfg := transport.DefaultConfig()
	var sender, receiver transport.Transport = NewBoardsTransport(boardURL, cfg), NewBoardsTransport(boardURL, cfg)
	if os.Getenv("BOARDS_LIVE_RAW") == "" {
		sender = transport.NewBatchedTransport(sender)
		receiver = transport.NewBatchedTransport(receiver)
	}

	var bytesReceived atomic.Int64
	receiver.Receive(func(b []byte) { bytesReceived.Add(int64(len(b))) })
	sender.Receive(func([]byte) {})

	if err := receiver.Start(); err != nil {
		t.Fatalf("receiver start: %v", err)
	}
	defer receiver.Stop()
	if err := sender.Start(); err != nil {
		t.Fatalf("sender start: %v", err)
	}
	defer sender.Stop()

	deadline := time.Now().Add(60 * time.Second)
	for !(sender.IsConnected() && receiver.IsConnected()) {
		if time.Now().After(deadline) {
			t.Fatal("transports did not connect within 60s")
		}
		time.Sleep(200 * time.Millisecond)
	}

	payload := make([]byte, packet)
	rand.Read(payload)

	var sent int64
	stop := time.Now().Add(time.Duration(seconds) * time.Second)
	start := time.Now()
	for time.Now().Before(stop) {
		if err := sender.Send(payload); err != nil {
			time.Sleep(time.Millisecond)
			continue
		}
		sent += int64(packet)
	}
	time.Sleep(3 * time.Second)

	elapsed := time.Since(start).Seconds()
	got := bytesReceived.Load()
	t.Logf("offered %.1f Mbit/s, delivered %.1f Mbit/s (%d of %d bytes) in %.0fs",
		float64(sent)*8/elapsed/1e6, float64(got)*8/elapsed/1e6, got, sent, elapsed)
}
