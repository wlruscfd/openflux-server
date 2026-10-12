package warp

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

type Config struct {
	ProfileID     string
	PrivateKey    string
	PeerPublicKey string
	Endpoint      string
	AddressV4     string
	AddressV6     string
}

func (c Config) Fingerprint() string {
	return c.ProfileID + "|" + c.PeerPublicKey + "|" + c.Endpoint + "|" + c.AddressV4 + "|" + c.AddressV6
}

// Tunnel is a userspace WireGuard link to a WARP peer; connections dialed through it leave from Cloudflare's address.
type Tunnel struct {
	dev  *device.Device
	tnet *netstack.Net
	once sync.Once
}

// Cloudflare sends full 1280-byte inner packets whatever MSS is advertised; a smaller MTU here drops them and the link silently stalls (measured: 1200 carries nothing, 1280 works).
const defaultMTU = 1280

func tunnelMTU() int {
	if n, err := strconv.Atoi(os.Getenv("OPENFLUX_WARP_MTU")); err == nil && n >= 576 && n <= 1420 {
		return n
	}
	return defaultMTU
}

func New(cfg Config) (*Tunnel, error) {
	priv, err := keyHex(cfg.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("warp private key: %w", err)
	}
	peer, err := keyHex(cfg.PeerPublicKey)
	if err != nil {
		return nil, fmt.Errorf("warp peer key: %w", err)
	}
	ep, err := resolveEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	var local []netip.Addr
	for _, s := range []string{cfg.AddressV4, cfg.AddressV6} {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		a, err := netip.ParseAddr(strings.SplitN(s, "/", 2)[0])
		if err != nil {
			return nil, fmt.Errorf("warp address %q: %w", s, err)
		}
		local = append(local, a)
	}
	if len(local) == 0 {
		return nil, errors.New("warp: no interface address")
	}

	tun, tnet, err := netstack.CreateNetTUN(local, []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("1.0.0.1")}, tunnelMTU())
	if err != nil {
		return nil, fmt.Errorf("warp netstack: %w", err)
	}
	dev := device.NewDevice(tun, conn.NewDefaultBind(), device.NewLogger(device.LogLevelError, "[WARP] "))
	ipc := fmt.Sprintf("private_key=%s\npublic_key=%s\nendpoint=%s\nallowed_ip=0.0.0.0/0\nallowed_ip=::/0\npersistent_keepalive_interval=25\n", priv, peer, ep)
	if err := dev.IpcSet(ipc); err != nil {
		dev.Close()
		return nil, fmt.Errorf("warp configure: %w", err)
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, fmt.Errorf("warp up: %w", err)
	}
	return &Tunnel{dev: dev, tnet: tnet}, nil
}

func (t *Tunnel) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return t.tnet.DialContext(ctx, network, addr)
}

func (t *Tunnel) Close() {
	t.once.Do(func() { t.dev.Close() })
}

func keyHex(b64 string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return "", err
	}
	if len(raw) != 32 {
		return "", fmt.Errorf("want 32 bytes, got %d", len(raw))
	}
	return hex.EncodeToString(raw), nil
}

func resolveEndpoint(ep string) (string, error) {
	host, port, err := net.SplitHostPort(ep)
	if err != nil {
		return "", fmt.Errorf("warp endpoint %q: %w", ep, err)
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return ep, nil
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return "", fmt.Errorf("resolve warp endpoint %s: %v", host, err)
	}
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			return net.JoinHostPort(v4.String(), port), nil
		}
	}
	return net.JoinHostPort(ips[0].String(), port), nil
}

// Probe asks Cloudflare through the link whether the traffic really arrives as WARP, and how long the round trip takes.
func (t *Tunnel) Probe(ctx context.Context) (bool, time.Duration, error) {
	client := &http.Client{Transport: &http.Transport{DialContext: t.DialContext, DisableKeepAlives: true}, Timeout: 12 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://www.cloudflare.com/cdn-cgi/trace", nil)
	if err != nil {
		return false, 0, err
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return strings.Contains(string(body), "warp=on"), time.Since(start), nil
}
