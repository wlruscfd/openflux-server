package transport

import (
	"sync"
	"time"
)

// FallbackTransport runs a preferred transport and, if that one fails to start or never connects, swaps in the proven one, so a bad update of the preferred never leaves the user without a link.
type FallbackTransport struct {
	mu        sync.Mutex
	cur       Transport
	alt       func() Transport
	within    time.Duration
	onSwitch  func(altConnected bool)
	recvCB    func([]byte)
	eventCB   func(code, detail string)
	switched  bool
	stopped   bool
	startedAt time.Time
	done      chan struct{}
}

// NewFallbackTransport waits `within` for the primary to connect; onSwitch reports whether the alternative then connected, which is what shows the primary was at fault.
func NewFallbackTransport(primary Transport, alt func() Transport, within time.Duration, onSwitch func(altConnected bool)) *FallbackTransport {
	return &FallbackTransport{cur: primary, alt: alt, within: within, onSwitch: onSwitch, done: make(chan struct{})}
}

func (f *FallbackTransport) current() Transport {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur
}

func (f *FallbackTransport) Start() error {
	cur := f.current()
	f.mu.Lock()
	f.startedAt = time.Now()
	f.mu.Unlock()
	if err := cur.Start(); err != nil {
		return f.switchNow(cur)
	}
	go f.watch(cur)
	return nil
}

func (f *FallbackTransport) watch(primary Transport) {
	deadline := time.NewTimer(f.within)
	defer deadline.Stop()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-f.done:
			return
		case <-tick.C:
			if primary.IsConnected() {
				return
			}
		case <-deadline.C:
			if primary.IsConnected() {
				return
			}
			_ = f.switchNow(primary)
			return
		}
	}
}

func (f *FallbackTransport) switchNow(failed Transport) error {
	f.mu.Lock()
	if f.stopped || f.switched || f.cur != failed {
		f.mu.Unlock()
		return nil
	}
	f.switched = true
	f.mu.Unlock()

	_ = failed.Stop()
	next := f.alt()

	f.mu.Lock()
	recv, ev := f.recvCB, f.eventCB
	f.cur = next
	f.mu.Unlock()
	if recv != nil {
		next.Receive(recv)
	}
	if ev != nil {
		SetEventCallback(next, ev)
	}
	if err := next.Start(); err != nil {
		if f.onSwitch != nil {
			f.onSwitch(false)
		}
		return err
	}
	go func() {
		timer := time.NewTimer(f.within)
		defer timer.Stop()
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-f.done:
				return
			case <-tick.C:
				if next.IsConnected() {
					if f.onSwitch != nil {
						f.onSwitch(true)
					}
					return
				}
			case <-timer.C:
				if f.onSwitch != nil {
					f.onSwitch(false)
				}
				return
			}
		}
	}()
	return nil
}

func (f *FallbackTransport) Stop() error {
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return nil
	}
	f.stopped = true
	cur := f.cur
	f.mu.Unlock()
	close(f.done)
	return cur.Stop()
}

func (f *FallbackTransport) Send(data []byte) error { return f.current().Send(data) }

func (f *FallbackTransport) Receive(callback func([]byte)) {
	f.mu.Lock()
	f.recvCB = callback
	cur := f.cur
	f.mu.Unlock()
	cur.Receive(callback)
}

func (f *FallbackTransport) IsConnected() bool { return f.current().IsConnected() }

func (f *FallbackTransport) Stats() TransportStats { return f.current().Stats() }

func (f *FallbackTransport) SetEventCallback(fn func(code, detail string)) {
	f.mu.Lock()
	f.eventCB = fn
	cur := f.cur
	f.mu.Unlock()
	SetEventCallback(cur, fn)
}

func (f *FallbackTransport) ForceReconnect() { ForceReconnect(f.current()) }

func (f *FallbackTransport) ProvideCookies(cookieStr string) {
	if p, ok := f.current().(interface{ ProvideCookies(string) }); ok {
		p.ProvideCookies(cookieStr)
	}
}

// Switched says whether the preferred transport was given up on.
func (f *FallbackTransport) Switched() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.switched
}
