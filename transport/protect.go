package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/p1neappleXpress/OpenFlux/netbind"
)

var protectFD func(fd int) bool

func ProtectedHTTPTransport() *http.Transport {
	dialer := netbind.Wrap(ProtectedDialer())
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t := dt.Clone()
		t.DialContext = dialer.DialContext
		return t
	}
	return &http.Transport{DialContext: dialer.DialContext, ForceAttemptHTTP2: true}
}

func SetProtector(fn func(fd int) bool) {
	protectFD = fn
}

// ProtectedDialer exempts a socket from an Android VpnService's own tunnel; without it, the transport's own connections get captured by the tunnel it's trying to establish, deadlocking it.
func ProtectedDialer() *net.Dialer {
	return &net.Dialer{
		Timeout: 30 * time.Second,
		Control: protectControl,
	}
}

var defaultBootstrapDNSServers = []string{"77.88.8.8:53", "8.8.8.8:53"}

var bootstrapDNSServers = append([]string(nil), defaultBootstrapDNSServers...)

func BootstrapDNSServers() []string {
	return append([]string(nil), bootstrapDNSServers...)
}

// SetBootstrapDNSServers REPLACES the defaults rather than trying them alongside, since falling back to a known-unreachable server just re-adds the delay; not concurrency-safe against an in-flight lookup.
func SetBootstrapDNSServers(servers []string) {
	if len(servers) == 0 {
		bootstrapDNSServers = append([]string(nil), defaultBootstrapDNSServers...)
		return
	}
	normalized := make([]string, len(servers))
	for i, s := range servers {
		if _, _, err := net.SplitHostPort(s); err != nil {
			s = net.JoinHostPort(s, "53")
		}
		normalized[i] = s
	}
	bootstrapDNSServers = normalized
}

// ProtectedResolver forces the pure-Go resolver: Android has no /etc/resolv.conf, so Go's OS resolver falls back to 127.0.0.1:53 where nothing listens - Dial here queries a real public resolver directly instead.
func ProtectedResolver() *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var lastErr error
			for _, server := range bootstrapDNSServers {
				conn, err := ProtectedDialer().DialContext(ctx, network, server)
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			return nil, lastErr
		},
	}
}

func protectControl(network, address string, c syscall.RawConn) error {
	if protectFD == nil {
		return nil
	}
	var protectErr error
	if err := c.Control(func(fd uintptr) {
		if !protectFD(int(fd)) {
			protectErr = fmt.Errorf("failed to protect socket for %s %s", network, address)
		}
	}); err != nil {
		return err
	}
	return protectErr
}
