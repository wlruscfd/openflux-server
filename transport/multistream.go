package transport

import "sync"

type MultiStreamTransport struct {
	streams []Transport

	mu       sync.RWMutex
	callback func([]byte)
}

func NewMultiStreamTransport(streams []Transport) *MultiStreamTransport {
	m := &MultiStreamTransport{streams: streams}
	for _, s := range streams {
		s.Receive(func(data []byte) {
			m.mu.RLock()
			cb := m.callback
			m.mu.RUnlock()
			if cb != nil {
				cb(data)
			}
		})
	}
	return m
}

func (m *MultiStreamTransport) Start() error {
	for i, s := range m.streams {
		if err := s.Start(); err != nil {
			for _, prev := range m.streams[:i] {
				prev.Stop()
			}
			return err
		}
	}
	return nil
}

func (m *MultiStreamTransport) Stop() error {
	for _, s := range m.streams {
		s.Stop()
	}
	return nil
}

func (m *MultiStreamTransport) ProvideCookies(cookieStr string) {
	for _, s := range m.streams {
		if provider, ok := s.(CookieProvider); ok {
			provider.ProvideCookies(cookieStr)
		}
	}
}

func (m *MultiStreamTransport) Send(data []byte) error {
	return m.streams[streamIndex(data, len(m.streams))].Send(data)
}

// streamIndex hashes src^dst port; XOR keeps the result identical for a reply, whose ports are swapped relative to the request.
func streamIndex(data []byte, n int) int {
	if n <= 1 {
		return 0
	}
	if len(data) < 20 {
		return 0
	}
	ipHeaderLen := int(data[0]&0x0F) * 4
	if len(data) < ipHeaderLen+4 {
		return 0
	}
	l4 := data[ipHeaderLen:]
	srcPort := uint16(l4[0])<<8 | uint16(l4[1])
	dstPort := uint16(l4[2])<<8 | uint16(l4[3])
	return int(srcPort^dstPort) % n
}

func (m *MultiStreamTransport) Receive(callback func([]byte)) {
	m.mu.Lock()
	m.callback = callback
	m.mu.Unlock()
}

func (m *MultiStreamTransport) IsConnected() bool {
	for _, s := range m.streams {
		if s.IsConnected() {
			return true
		}
	}
	return false
}

func (m *MultiStreamTransport) Stats() TransportStats {
	var out TransportStats
	for _, s := range m.streams {
		st := s.Stats()
		out.BytesSent += st.BytesSent
		out.BytesReceived += st.BytesReceived
		out.PacketsSent += st.PacketsSent
		out.PacketsRecv += st.PacketsRecv
		out.Reconnects += st.Reconnects
		if st.Uptime > out.Uptime {
			out.Uptime = st.Uptime
		}
	}
	out.Connected = m.IsConnected()
	return out
}

func (m *MultiStreamTransport) SetEventCallback(fn func(code, detail string)) {
	for _, s := range m.streams {
		s.SetEventCallback(fn)
	}
}

func (m *MultiStreamTransport) ForceReconnect() {
	for _, s := range m.streams {
		s.ForceReconnect()
	}
}
