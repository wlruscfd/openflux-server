package tunnel

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func listenOnBindableNonLoopback(t *testing.T) net.Listener {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Skipf("cannot list interfaces: %v", err)
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		ip4 := ipNet.IP.To4()
		if ip4 == nil {
			continue
		}
		ln, err := net.Listen("tcp", net.JoinHostPort(ip4.String(), "0"))
		if err != nil {
			continue
		}
		return ln
	}
	t.Skip("no bindable non-loopback IPv4 address on this machine")
	return nil
}

func TestTunnelThroughputOverPipe(t *testing.T) {
	ln := listenOnBindableNonLoopback(t)
	defer ln.Close()

	const chunk = 256 * 1024
	const chunks = 32
	const total = chunk * chunks
	payload := make([]byte, chunk)
	for i := range payload {
		payload[i] = byte(i)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				go io.Copy(io.Discard, c)
				for i := 0; i < chunks; i++ {
					if _, err := c.Write(payload); err != nil {
						return
					}
				}
			}(conn)
		}
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

	buf := make([]byte, 64*1024)
	start := time.Now()
	var got int
	for got < total {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		n, err := conn.Read(buf)
		got += n
		if err != nil {
			t.Fatalf("read %d/%d bytes: %v", got, total, err)
		}
	}
	elapsed := time.Since(start)
	mbps := float64(got) / 1024 / 1024 / elapsed.Seconds()
	t.Logf("tunnel moved %s in %s = %.1f MB/s over a zero-latency transport",
		fmt.Sprintf("%.2f MB", float64(got)/1024/1024), elapsed.Round(time.Millisecond), mbps)
	if mbps < 1 {
		t.Fatalf("tunnel throughput %.2f MB/s is far below any plausible transport", mbps)
	}
}
