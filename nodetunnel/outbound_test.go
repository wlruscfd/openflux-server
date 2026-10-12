package nodetunnel

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestExitDialsThroughTheOutboundDialer(t *testing.T) {
	tun := &TCPTunnel{}
	var gotNet, gotAddr string
	tun.SetOutboundDialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		gotNet, gotAddr = network, addr
		return nil, errors.New("stop here")
	})
	if _, err := tun.dialExit("203.0.113.9:443"); err == nil {
		t.Fatal("the stub dialer's error must come back")
	}
	if gotNet != "tcp" || gotAddr != "203.0.113.9:443" {
		t.Fatalf("dialer saw %s %s", gotNet, gotAddr)
	}
}
