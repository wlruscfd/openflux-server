package nodetunnel

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

func localNonLoopbackIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipNet.IP.To4(); ip4 != nil {
			return ip4.String()
		}
	}
	return ""
}

type pipeTransport struct {
	peer *pipeTransport
	out  chan []byte
	cb   func([]byte)
}

func newPipePair() (a, b *pipeTransport) {
	a = &pipeTransport{out: make(chan []byte, 256)}
	b = &pipeTransport{out: make(chan []byte, 256)}
	a.peer = b
	b.peer = a
	go a.pump()
	go b.pump()
	return a, b
}

func (p *pipeTransport) pump() {
	for data := range p.out {
		if p.peer.cb != nil {
			p.peer.cb(data)
		}
	}
}

func (p *pipeTransport) Send(data []byte) error {
	p.out <- append([]byte(nil), data...)
	return nil
}
func (p *pipeTransport) Start() error                               { return nil }
func (p *pipeTransport) Stop() error                                { close(p.out); return nil }
func (p *pipeTransport) Receive(cb func([]byte))                    { p.cb = cb }
func (p *pipeTransport) IsConnected() bool                          { return true }
func (p *pipeTransport) Stats() transport.TransportStats            { return transport.TransportStats{} }
func (p *pipeTransport) SetEventCallback(func(code, detail string)) {}
func (p *pipeTransport) ForceReconnect()                            {}

func TestExitModeProxyRelaysRealTCP(t *testing.T) {
	ip := localNonLoopbackIP()
	if ip == "" {
		t.Skip("no non-loopback IPv4 address on this machine")
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	const want = "hello from proxy mode"
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, len(want))
		io.ReadFull(conn, buf)
		conn.Write(buf)
	}()

	clientSide, exitSide := newPipePair()

	exitTun := NewTCPTunnelMode(exitSide, true, ExitModeProxy)
	defer exitTun.Close()

	clientTun := NewTCPTunnel(clientSide, false)
	defer clientTun.Close()

	conn, err := clientTun.DialTCP(ln.Addr().String())
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(want)); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
