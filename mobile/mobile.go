// Package mobile is the sole entry point bound into an Android .aar via `gomobile bind`; it stays a thin facade since gomobile only bridges a small set of types.
package mobile

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"universal-bypass-tool/gateway"
	"universal-bypass-tool/socks5"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

var originalResolver = net.DefaultResolver

// The "oneme" (MAX) transport needs `-ldflags=-checklinkname=0` passed to `gomobile bind` (see build_android_aar.sh) or Go 1.23+'s linker rejects its //go:linkname hook.

type Callback interface {
	OnStatus(status string) // "connecting" | "connected" | "error:<message>" | "stopped"
	OnStats(bytesSent int64, bytesReceived int64)
	// OnLogEvent is what surfaces silent background reconnects that OnStatus's one-time "connected" snapshot misses; code/detail are transport.Event* constants.
	OnLogEvent(code string, detail string)
	OnRawLog(line string)
}

// Protector exempts a raw socket from the Android VPN's own tunnel; without it every transport connection gets captured by the tunnel it's meant to carry, deadlocking it. May be nil outside an Android VpnService.
type Protector interface {
	Protect(fd int) bool
}

// Config's doc_url always comes from an imported deep link or pasted by hand - a prior live-resolve "key" mode was removed since that request had no disguise and was trivial to block.
type Config struct {
	Mode string `json:"mode"` // must be "manual" - see buildTransport

	Transport string `json:"transport"` // "yandex" (default), "volga", "max", or "yandex_multistream"
	DocURL    string `json:"doc_url"`   // yandex, volga
	MaxToken  string `json:"max_token"` // max: your MAX account's own auth token
	MaxUID    int64  `json:"max_uid"`   // max: the contact's user ID to place the call to

	DocURLs []string `json:"doc_urls,omitempty"`

	KeyToken string `json:"key_token,omitempty"`

	E2EEncryption bool `json:"e2e_encryption,omitempty"`

	MTU         int    `json:"mtu"`
	DNSUpstream string `json:"dns_upstream"`

	SiteSplitMode  string   `json:"site_split_mode,omitempty"`
	SiteSplitSites []string `json:"site_split_sites,omitempty"`

	// ForceBootstrapDNS overrides the fixed public resolvers used to resolve the transport's own hostnames before the tunnel exists; unrelated to the tunneled DNSUpstream.
	ForceBootstrapDNS string `json:"force_bootstrap_dns,omitempty"`

	VerboseLogging bool `json:"verbose_logging,omitempty"`
}

type session struct {
	trans     transport.Transport
	tun       *tunnel.TCPTunnel
	gw        *gateway.Server
	tunFile   *os.File
	stopStats chan struct{}
}

var (
	mu      sync.Mutex
	current *session
)

type socks5Session struct {
	trans  transport.Transport
	tun    *tunnel.TCPTunnel
	server *socks5.SOCKS5Server
}

var (
	socksMu      sync.Mutex
	currentSocks *socks5Session
)

func StartTunnel(tunFd int, configJSON string, protector Protector, cb Callback) error {
	mu.Lock()
	defer mu.Unlock()
	socksMu.Lock()
	running := currentSocks != nil
	socksMu.Unlock()
	if current != nil {
		return fmt.Errorf("a tunnel is already running; call StopTunnel first")
	}
	if running {
		return fmt.Errorf("a SOCKS5 proxy is already running; call StopSocks5Proxy first")
	}

	var cfg Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return fail(cb, fmt.Errorf("parse config: %w", err))
	}

	utils.SetVerbose(cfg.VerboseLogging)
	if cfg.VerboseLogging {
		utils.SetLogSink(func(line string) {
			if cb != nil {
				cb.OnRawLog(line)
			}
		})
	} else {
		utils.SetLogSink(nil)
	}

	if protector != nil {
		transport.SetProtector(protector.Protect)
		net.DefaultResolver = transport.ProtectedResolver()
		if cfg.ForceBootstrapDNS != "" {
			transport.SetBootstrapDNSServers([]string{cfg.ForceBootstrapDNS})
		}
	}

	notify(cb, "connecting")

	transportConfig := transport.DefaultConfig()
	trans, err := buildTransport(cfg, transportConfig)
	if err != nil {
		return fail(cb, err)
	}
	trans.SetEventCallback(func(code, detail string) {
		if cb != nil {
			cb.OnLogEvent(code, detail)
		}
	})

	if err := trans.Start(); err != nil {
		return fail(cb, fmt.Errorf("start transport: %w", err))
	}

	tun := tunnel.NewTCPTunnel(trans, false)
	if cfg.MTU > 0 {
		tun.SetMTU(uint32(cfg.MTU))
	}

	tunFile := os.NewFile(uintptr(tunFd), "tun")
	if tunFile == nil {
		trans.Stop()
		tun.Close()
		return fail(cb, fmt.Errorf("invalid tun file descriptor: %d", tunFd))
	}

	gw := gateway.NewServerWithPolicy(tun, cfg.DNSUpstream, sitePolicy(cfg))
	if cfg.MTU > 0 {
		gw.SetMTU(uint32(cfg.MTU))
	}
	if err := gw.Start(tunFile, tunFile); err != nil {
		trans.Stop()
		tun.Close()
		tunFile.Close()
		return fail(cb, fmt.Errorf("start gateway: %w", err))
	}

	s := &session{trans: trans, tun: tun, gw: gw, tunFile: tunFile, stopStats: make(chan struct{})}
	current = s

	go pumpStats(s, cb)
	notify(cb, "connected")
	return nil
}

// NetworkChanged lets the caller trigger an immediate retry instead of waiting for a read/write timeout to notice a dead network - a safe no-op when nothing is running.
func NetworkChanged() {
	mu.Lock()
	s := current
	mu.Unlock()
	if s != nil {
		s.trans.ForceReconnect()
	}
}

type cookieProvider interface {
	ProvideCookies(cookieStr string)
}

func ProvideCaptchaCookies(cookieStr string) error {
	mu.Lock()
	s := current
	mu.Unlock()
	if s != nil {
		cp, ok := s.trans.(cookieProvider)
		if !ok {
			return fmt.Errorf("active transport does not support captcha cookies")
		}
		cp.ProvideCookies(cookieStr)
		return nil
	}

	socksMu.Lock()
	ss := currentSocks
	socksMu.Unlock()
	if ss != nil {
		cp, ok := ss.trans.(cookieProvider)
		if !ok {
			return fmt.Errorf("active transport does not support captcha cookies")
		}
		cp.ProvideCookies(cookieStr)
		return nil
	}

	return fmt.Errorf("no tunnel or proxy is running")
}

func StopTunnel() error {
	mu.Lock()
	s := current
	current = nil
	mu.Unlock()

	transport.SetProtector(nil)
	transport.SetBootstrapDNSServers(nil)
	net.DefaultResolver = originalResolver
	utils.SetLogSink(nil)

	if s == nil {
		return nil
	}

	close(s.stopStats)
	s.gw.Close()      // marks it closed and destroys its gvisor stack
	s.tunFile.Close() // unblocks the gateway's tun read loop
	s.trans.Stop()
	s.tun.Close()
	return nil
}

// StartSocks5Proxy relays only what's explicitly pointed at listenAddr, so unlike StartTunnel it needs no VPN permission or Protector.
func StartSocks5Proxy(configJSON string, listenAddr string, cb Callback) error {
	socksMu.Lock()
	defer socksMu.Unlock()
	mu.Lock()
	running := current != nil
	mu.Unlock()
	if currentSocks != nil {
		return fmt.Errorf("a SOCKS5 proxy is already running; call StopSocks5Proxy first")
	}
	if running {
		return fmt.Errorf("a tunnel is already running; call StopTunnel first")
	}

	var cfg Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return fail(cb, fmt.Errorf("parse config: %w", err))
	}

	utils.SetVerbose(cfg.VerboseLogging)
	if cfg.VerboseLogging {
		utils.SetLogSink(func(line string) {
			if cb != nil {
				cb.OnRawLog(line)
			}
		})
	} else {
		utils.SetLogSink(nil)
	}

	notify(cb, "connecting")

	transportConfig := transport.DefaultConfig()
	trans, err := buildTransport(cfg, transportConfig)
	if err != nil {
		return fail(cb, err)
	}
	trans.SetEventCallback(func(code, detail string) {
		if cb != nil {
			cb.OnLogEvent(code, detail)
		}
	})

	if err := trans.Start(); err != nil {
		return fail(cb, fmt.Errorf("start transport: %w", err))
	}

	tun := tunnel.NewTCPTunnel(trans, false)
	if cfg.MTU > 0 {
		tun.SetMTU(uint32(cfg.MTU))
	}
	server := socks5.NewSOCKS5Server(listenAddr, tun)

	s := &socks5Session{trans: trans, tun: tun, server: server}
	currentSocks = s

	go func() {
		if err := server.Start(); err != nil {
			utils.Debugf("[SOCKS5] server stopped: %v", err)
		}
	}()

	notify(cb, "connected")
	return nil
}

func StopSocks5Proxy() error {
	socksMu.Lock()
	s := currentSocks
	currentSocks = nil
	socksMu.Unlock()

	utils.SetLogSink(nil)

	if s == nil {
		return nil
	}

	s.server.Stop()
	s.trans.Stop()
	s.tun.Close()
	return nil
}

func buildTransport(cfg Config, transportConfig transport.TransportConfig) (transport.Transport, error) {
	if cfg.Mode != "manual" {
		return nil, fmt.Errorf(`config.mode must be "manual", got %q`, cfg.Mode)
	}

	t := cfg.Transport
	if t == "" {
		t = "yandex"
	}
	switch t {
	case "yandex":
		if cfg.DocURL == "" {
			return nil, fmt.Errorf("doc_url is required")
		}
		return wrapYandex(yandex.NewYandexDocsTransport(cfg.DocURL, transportConfig), cfg, -1), nil
	case "volga":
		if cfg.DocURL == "" {
			return nil, fmt.Errorf("doc_url is required")
		}
		return wrapGeneric(yandex.NewYandexVolgaTransport(cfg.DocURL, transportConfig), cfg), nil
	case "max":
		if cfg.MaxToken == "" || cfg.MaxUID == 0 {
			return nil, fmt.Errorf("the max transport requires max_token and max_uid")
		}
		return wrapGeneric(oneme.NewOneMeTransport(false, cfg.MaxToken, cfg.MaxUID, transportConfig), cfg), nil
	case "yandex_multistream":
		if len(cfg.DocURLs) < 2 {
			return nil, fmt.Errorf("yandex_multistream requires at least 2 doc_urls")
		}
		streams := make([]transport.Transport, len(cfg.DocURLs))
		for i, url := range cfg.DocURLs {
			streams[i] = wrapYandex(yandex.NewYandexDocsTransport(url, transportConfig), cfg, i)
		}
		return transport.NewMultiStreamTransport(streams), nil
	default:
		return nil, fmt.Errorf("unsupported transport %q for this client", t)
	}
}

// wrapYandex: yd manages its own compression/encryption internally (see EnableSelfCompression/EnableEncryptedSelfCompression) rather than being wrapped externally - never worse, and better once the peer proves it too.
func wrapYandex(yd *yandex.YandexDocsTransport, cfg Config, streamIdx int) transport.Transport {
	if !cfg.E2EEncryption || cfg.KeyToken == "" {
		yd.EnableSelfCompression()
		return yd
	}
	if streamIdx >= 0 {
		yd.EnableEncryptedSelfCompressionForStream(cfg.KeyToken, false, streamIdx)
	} else {
		yd.EnableEncryptedSelfCompression(cfg.KeyToken, false)
	}
	return yd
}

func wrapGeneric(inner transport.Transport, cfg Config) transport.Transport {
	if cfg.E2EEncryption && cfg.KeyToken != "" {
		inner = transport.NewEncryptedTransport(inner, cfg.KeyToken, false)
	}
	return transport.NewCompressedTransport(inner)
}

func sitePolicy(cfg Config) *gateway.SitePolicy {
	return gateway.NewSitePolicy(gateway.ParseSiteSplitMode(cfg.SiteSplitMode), cfg.SiteSplitSites)
}

func pumpStats(s *session, cb Callback) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopStats:
			return
		case <-ticker.C:
			stats := s.trans.Stats()
			if cb != nil {
				cb.OnStats(int64(stats.BytesSent), int64(stats.BytesReceived))
			}
		}
	}
}

func notify(cb Callback, status string) {
	if cb != nil {
		cb.OnStatus(status)
	}
}

func fail(cb Callback, err error) error {
	notify(cb, "error:"+err.Error())
	return err
}
