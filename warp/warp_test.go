package warp

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

func TestDialsThroughAWireGuardPeer(t *testing.T) {
	serverKey, _ := ecdh.X25519().GenerateKey(rand.Reader)
	clientKey, _ := ecdh.X25519().GenerateKey(rand.Reader)

	srvTun, srvNet, err := netstack.CreateNetTUN([]netip.Addr{netip.MustParseAddr("10.9.0.1")}, nil, 1280)
	if err != nil {
		t.Fatal(err)
	}
	srv := device.NewDevice(srvTun, conn.NewDefaultBind(), device.NewLogger(device.LogLevelSilent, ""))
	defer srv.Close()
	probe, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	probe.Close()
	ipc := fmt.Sprintf("private_key=%s\nlisten_port=%d\npublic_key=%s\nallowed_ip=10.9.0.2/32\n",
		hex.EncodeToString(serverKey.Bytes()), port, hex.EncodeToString(clientKey.PublicKey().Bytes()))
	if err := srv.IpcSet(ipc); err != nil {
		t.Fatal(err)
	}
	if err := srv.Up(); err != nil {
		t.Fatal(err)
	}
	ln, err := srvNet.ListenTCP(&net.TCPAddr{IP: net.ParseIP("10.9.0.1"), Port: 8080})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write([]byte("through the tunnel"))
	}()

	tun, err := New(Config{
		ProfileID:     "p1",
		PrivateKey:    base64.StdEncoding.EncodeToString(clientKey.Bytes()),
		PeerPublicKey: base64.StdEncoding.EncodeToString(serverKey.PublicKey().Bytes()),
		Endpoint:      fmt.Sprintf("127.0.0.1:%d", port),
		AddressV4:     "10.9.0.2",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := tun.DialContext(ctx, "tcp", "10.9.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := io.ReadAll(c)
	if err != nil || string(got) != "through the tunnel" {
		t.Fatalf("read %q, %v", got, err)
	}
}

func TestRejectsABadKey(t *testing.T) {
	if _, err := New(Config{PrivateKey: "not base64!!", PeerPublicKey: "x", Endpoint: "1.2.3.4:5", AddressV4: "10.0.0.2"}); err == nil {
		t.Fatal("a bad key must be refused")
	}
}
