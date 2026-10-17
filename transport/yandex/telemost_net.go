package yandex

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/transport/v5"
	"github.com/pion/transport/v5/stdnet"
	"github.com/pion/webrtc/v4"

	otransport "github.com/p1neappleXpress/OpenFlux/transport"
)

type protectedNet struct {
	*stdnet.Net
}

func newProtectedNet() (transport.Net, error) {
	n, err := stdnet.NewNet()
	if err != nil {
		return nil, err
	}
	return protectedNet{n}, nil
}

func (n protectedNet) ListenPacket(network, address string) (net.PacketConn, error) {
	return otransport.ProtectedListenConfig().ListenPacket(context.Background(), network, address)
}

func (n protectedNet) ListenUDP(network string, laddr *net.UDPAddr) (transport.UDPConn, error) {
	addr := ":0"
	if laddr != nil {
		addr = laddr.String()
	}
	pc, err := otransport.ProtectedListenConfig().ListenPacket(context.Background(), network, addr)
	if err != nil {
		return nil, err
	}
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("telemost: %T is not a UDP socket", pc)
	}
	return uc, nil
}

func (n protectedNet) Dial(network, address string) (net.Conn, error) {
	return otransport.DialContext(context.Background(), network, address)
}

func (n protectedNet) DialUDP(network string, laddr, raddr *net.UDPAddr) (transport.UDPConn, error) {
	d := otransport.ProtectedDialer()
	if laddr != nil {
		d.LocalAddr = laddr
	}
	c, err := d.Dial(network, raddr.String())
	if err != nil {
		return nil, err
	}
	uc, ok := c.(*net.UDPConn)
	if !ok {
		_ = c.Close()
		return nil, fmt.Errorf("telemost: %T is not a UDP socket", c)
	}
	return uc, nil
}

func (n protectedNet) CreateDialer(d *net.Dialer) transport.Dialer {
	cp := *d
	cp.Control = otransport.ProtectedDialer().Control
	return n.Net.CreateDialer(&cp)
}

func (n protectedNet) CreateListenConfig(c *net.ListenConfig) transport.ListenConfig {
	cp := *c
	cp.Control = otransport.ProtectedListenConfig().Control
	return n.Net.CreateListenConfig(&cp)
}

// Every WebRTC socket must be exempt from an Android VpnService tunnel that includes this process.
func telemostSettingEngine() webrtc.SettingEngine {
	var se webrtc.SettingEngine
	if n, err := newProtectedNet(); err == nil {
		se.SetNet(n)
	}
	return se
}

func telemostHTTPClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second, Transport: otransport.ProtectedHTTPTransport()}
}

func telemostWSDialer() *websocket.Dialer {
	return &websocket.Dialer{
		NetDialContext:   otransport.DialContext,
		HandshakeTimeout: 45 * time.Second,
	}
}
