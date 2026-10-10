package api

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/model"
	"openflux-control/internal/store"
)

type ctxKey int

const (
	ctxKeyNode ctxKey = iota
	ctxKeyIngestToken
)

func nodeFromContext(ctx context.Context) model.Node {
	n, _ := ctx.Value(ctxKeyNode).(model.Node)
	return n
}

func ingestTokenFromContext(ctx context.Context) model.IngestToken {
	t, _ := ctx.Value(ctxKeyIngestToken).(model.IngestToken)
	return t
}

func (a *App) withAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.ExtractBearer(r)
		if !ok || !auth.ConstantTimeEqual(token, a.Config.AdminToken) {
			writeError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		next(w, r)
	}
}

func (a *App) withIngestToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.ExtractBearer(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}

		it, err := a.Store.GetIngestTokenByHash(r.Context(), a.Hasher.Hash(token))
		if err == store.ErrNotFound {
			writeError(w, http.StatusUnauthorized, "invalid ingest token")
			return
		}
		if err != nil {
			writeInternalError(w, r, "lookup failed", err)
			return
		}

		ctx := context.WithValue(r.Context(), ctxKeyIngestToken, it)
		next(w, r.WithContext(ctx))
	}
}

func (a *App) withNodeToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.ExtractBearer(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}

		n, err := a.Store.GetNodeBySecretHash(r.Context(), a.Hasher.Hash(token))
		if err == store.ErrNotFound {
			writeError(w, http.StatusUnauthorized, "invalid node token")
			return
		}
		if err != nil {
			writeInternalError(w, r, "lookup failed", err)
			return
		}

		// "me" lets a node address itself without knowing its own generated ID up front; an explicit ID is still checked against the node token.
		if pathID := r.PathValue("id"); pathID != "" && pathID != "me" && pathID != n.ID {
			writeError(w, http.StatusForbidden, "node token does not match node id")
			return
		}

		ctx := context.WithValue(r.Context(), ctxKeyNode, n)
		next(w, r.WithContext(ctx))
	}
}

const (
	keyRequestsPerSecond   = 2
	keyRequestBurst        = 10
	floodRequestsPerSecond = 100
	floodRequestBurst      = 400
)

func (a *App) withRateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !a.floodLimiter.allow(ip) {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next(w, r)
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if peer == nil || !peer.IsLoopback() {
		return host
	}
	if real := strings.TrimSpace(r.Header.Get("X-Real-IP")); net.ParseIP(real) != nil {
		return real
	}
	first, _, _ := strings.Cut(r.Header.Get("X-Forwarded-For"), ",")
	if first = strings.TrimSpace(first); net.ParseIP(first) != nil {
		return first
	}
	return host
}

// ipRateLimiter is dependency-free, in-memory only: limits reset on restart and aren't shared across replicas - enough to blunt casual brute-forcing, not a substitute for a real WAF.
type ipRateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rps     float64
	burst   int
}

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

func newIPRateLimiter(rps float64, burst int) *ipRateLimiter {
	if rps <= 0 {
		rps = 1
	}
	if burst <= 0 {
		burst = 1
	}
	l := &ipRateLimiter{
		buckets: make(map[string]*bucket),
		rps:     rps,
		burst:   burst,
	}
	go l.sweepStale()
	return l
}

// bucketStaleAfter: a bucket this old is already back at full burst, so evicting it costs nothing - the next request just allocates a fresh one. Every distinct client IP ever seen otherwise stays in the map forever, which is a real leak on a public per-IP endpoint like handleResolve.
const bucketStaleAfter = 10 * time.Minute

func (l *ipRateLimiter) sweepStale() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-bucketStaleAfter)
		l.mu.Lock()
		for ip, b := range l.buckets {
			if b.lastSeen.Before(cutoff) {
				delete(l.buckets, ip)
			}
		}
		l.mu.Unlock()
	}
}

func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b, ok := l.buckets[ip]
	if !ok {
		b = &bucket{tokens: float64(l.burst), lastSeen: now}
		l.buckets[ip] = b
	}

	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens = min(float64(l.burst), b.tokens+elapsed*l.rps)
	b.lastSeen = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
