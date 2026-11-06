package outbound

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"
)

// verifyTTL is how long a key that passed a provider-specific check stays trusted. A carrier whose
// provider limits how often one address may ask (Avito) is only served by keys that passed such a
// check lately; a key that was not looked at for this long drops back to unverified.
const verifyTTL = 45 * time.Minute

const checkUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

func (s *keyState) verified(now time.Time) bool {
	if s.probe.Load() != 1 {
		return false
	}
	at := s.verifiedAt.Load()
	return at != 0 && now.Sub(time.Unix(at, 0)) < verifyTTL
}

// SetVerifiedOnly makes the pool hand out only keys that passed the provider check lately.
func (p *Pool) SetVerifiedOnly(v bool) { p.verifiedOnly.Store(v) }

func (p *Pool) VerifiedOnly() bool { return p.verifiedOnly.Load() }

// HasVerified reports whether at least one verified key can be dialed through right now.
func (p *Pool) HasVerified() bool {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, st := range p.keys {
		if st.verified(now) && st.available(now) {
			return true
		}
	}
	return false
}

// VerifiedAt returns when the key last passed the provider check, if it is still trusted.
func (p *Pool) VerifiedAt(keyID string, now time.Time) (time.Time, bool) {
	p.mu.Lock()
	st := p.byID[keyID]
	p.mu.Unlock()
	if st == nil || !st.verified(now) {
		return time.Time{}, false
	}
	return time.Unix(st.verifiedAt.Load(), 0), true
}

// ProbeKeyHTTP asks the provider for its front page through the key. A TLS handshake is not
// enough for a provider that answers some addresses with a refusal or a reset after it: the key
// has to bring back a normal page. The request is a plain GET and creates nothing on the provider.
func (p *Pool) ProbeKeyHTTP(ctx context.Context, keyID, address, path string) (time.Duration, error) {
	p.mu.Lock()
	st := p.byID[keyID]
	p.mu.Unlock()
	if st == nil {
		return 0, ErrNoProxy
	}
	started := time.Now()
	fail := func(err error) (time.Duration, error) {
		st.probe.Store(2)
		return time.Since(started), err
	}
	conn, err := st.dial.DialContext(ctx, "tcp", address)
	if err != nil {
		return fail(err)
	}
	defer conn.Close()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	tlsConn := tls.Client(conn, &tls.Config{ServerName: host})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return fail(fmt.Errorf("tls %s: %w", host, err))
	}
	req := "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nUser-Agent: " + checkUserAgent +
		"\r\nAccept: */*\r\nConnection: close\r\n\r\n"
	if _, err := tlsConn.Write([]byte(req)); err != nil {
		return fail(fmt.Errorf("get %s: %w", host, err))
	}
	resp, err := http.ReadResponse(bufio.NewReader(tlsConn), nil)
	if err != nil {
		return fail(fmt.Errorf("get %s: %w", host, err))
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fail(fmt.Errorf("get %s: http %d", host, resp.StatusCode))
	}
	st.probe.Store(1)
	st.verifiedAt.Store(time.Now().Unix())
	return time.Since(started), nil
}
