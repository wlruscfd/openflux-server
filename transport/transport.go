package transport

import (
	"sync"
	"sync/atomic"
	"time"
)

type TransportConfig struct {
	MaxReconnectAttempts int
	ReconnectDelay       time.Duration
	ReconnectMultiplier  float64
	MaxReconnectDelay    time.Duration
	MaxQueueSize         int
	KeepAliveInterval    time.Duration
}

type Transport interface {
	Start() error
	Stop() error
	Send(data []byte) error
	Receive(callback func([]byte))
	IsConnected() bool
	Stats() TransportStats
	SetEventCallback(fn func(code, detail string))
	// ForceReconnect lets a caller that already knows the connection is dead (a network-change callback) trigger an immediate retry instead of waiting for read/write to notice.
	ForceReconnect()
}

const (
	EventConnecting = "connecting"
	EventConnected  = "connected"
	EventRetrying   = "retrying"
	// EventCaptchaRequired's detail is the doc_url that needs solving.
	EventCaptchaRequired = "captcha_required"
)

type TransportStats struct {
	BytesSent     uint64
	BytesReceived uint64
	PacketsSent   uint64
	PacketsRecv   uint64
	Reconnects    uint64
	Connected     bool
	Uptime        time.Duration
}

func DefaultConfig() TransportConfig {
	return TransportConfig{
		MaxReconnectAttempts: 999999,
		// 1.5s floor, not lower: upstream (p1neappleXpress/OpenFlux@47b151f) found every WS reconnect registers as a fresh participant on Yandex's side regardless of our own userID reuse - a tighter floor just piles up ghost participants faster during a failure streak. Must stay nonzero either way: 0 makes the exponential backoff a permanent no-op, since 0 * anything is still 0.
		ReconnectDelay:      1500 * time.Millisecond,
		ReconnectMultiplier: 1.6,
		MaxReconnectDelay:   30 * time.Second,
		MaxQueueSize:        1024,
		KeepAliveInterval:   10 * time.Second,
	}
}

type BaseTransport struct {
	config    TransportConfig
	running   atomic.Int32
	connected atomic.Int32
	stats     TransportStats
	startTime time.Time

	receiveCallback func([]byte)
	eventCallback   func(code, detail string)
	Mu              sync.RWMutex

	reconnectAttempts atomic.Int32
}

func NewBaseTransport(config TransportConfig) *BaseTransport {
	return &BaseTransport{
		config:    config,
		startTime: time.Now(),
	}
}

func (b *BaseTransport) Start() error {
	b.running.Store(1)
	b.startTime = time.Now()
	return nil
}

func (b *BaseTransport) Stop() error {
	b.running.Store(0)
	b.connected.Store(0)
	return nil
}

func (b *BaseTransport) IsRunning() bool {
	return b.running.Load() == 1
}

func (b *BaseTransport) IsConnected() bool {
	return b.connected.Load() == 1
}

func (b *BaseTransport) SetConnected(connected bool) {
	if connected {
		b.connected.Store(1)
	} else {
		b.connected.Store(0)
	}
}

func (b *BaseTransport) Receive(callback func([]byte)) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.receiveCallback = callback
}

func (b *BaseTransport) CallReceive(data []byte) {
	b.Mu.RLock()
	cb := b.receiveCallback
	b.Mu.RUnlock()
	if cb != nil {
		cb(data)
	}
}

func (b *BaseTransport) SetEventCallback(fn func(code, detail string)) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.eventCallback = fn
}

func (b *BaseTransport) EmitEvent(code, detail string) {
	b.Mu.RLock()
	fn := b.eventCallback
	b.Mu.RUnlock()
	if fn != nil {
		fn(code, detail)
	}
}

func (b *BaseTransport) GetSession(accessor func(interface{})) {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
}

func (b *BaseTransport) Stats() TransportStats {
	return TransportStats{
		BytesSent:     atomic.LoadUint64(&b.stats.BytesSent),
		BytesReceived: atomic.LoadUint64(&b.stats.BytesReceived),
		PacketsSent:   atomic.LoadUint64(&b.stats.PacketsSent),
		PacketsRecv:   atomic.LoadUint64(&b.stats.PacketsRecv),
		Reconnects:    uint64(b.reconnectAttempts.Load()),
		Connected:     b.IsConnected(),
		Uptime:        time.Since(b.startTime),
	}
}

func (b *BaseTransport) RecordSend(bytes int) {
	atomic.AddUint64(&b.stats.BytesSent, uint64(bytes))
	atomic.AddUint64(&b.stats.PacketsSent, 1)
}

func (b *BaseTransport) RecordReceive(bytes int) {
	atomic.AddUint64(&b.stats.BytesReceived, uint64(bytes))
	atomic.AddUint64(&b.stats.PacketsRecv, 1)
}

func (b *BaseTransport) RecordReconnect() {
	b.reconnectAttempts.Add(1)
}

func (b *BaseTransport) GetConfig() TransportConfig {
	return b.config
}

func (b *BaseTransport) ForceReconnect() {}
