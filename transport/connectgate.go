package transport

import (
	"os"
	"strconv"
	"sync"
	"time"
)

var connectGate = newConnectGate(connectRateFromEnv())

type gate struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func newConnectGate(perSecond float64) *gate {
	if perSecond <= 0 {
		return &gate{}
	}
	return &gate{interval: time.Duration(float64(time.Second) / perSecond)}
}

func connectRateFromEnv() float64 {
	if v, err := strconv.ParseFloat(os.Getenv("OPENFLUX_CONNECT_RATE"), 64); err == nil {
		return v
	}
	return 20
}

func (g *gate) wait(done <-chan struct{}) bool {
	if g.interval <= 0 {
		return true
	}
	g.mu.Lock()
	now := time.Now()
	slot := g.next
	if slot.Before(now) {
		slot = now
	}
	g.next = slot.Add(g.interval)
	g.mu.Unlock()

	delay := time.Until(slot)
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	}
}

func WaitConnectSlot(done <-chan struct{}) bool {
	return connectGate.wait(done)
}
