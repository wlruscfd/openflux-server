package outbound

import (
	"context"
	"net"
	"strings"
	"sync"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

type Router struct {
	mu       sync.RWMutex
	pools    map[string]*Pool
	hosts    map[string][]string
	byHost   map[string]string
	exempt   map[string]bool
	fallback bool
}

func NewRouter() *Router {
	return &Router{
		pools:  map[string]*Pool{},
		hosts:  map[string][]string{},
		byHost: map[string]string{},
		exempt: map[string]bool{},
	}
}

func (r *Router) SetPool(carrierType string, pool *Pool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pools[carrierType] = pool
}

func (r *Router) Pool(carrierType string) *Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pools[carrierType]
}

func (r *Router) CarrierTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.pools))
	for name := range r.pools {
		out = append(out, name)
	}
	return out
}

func (r *Router) RegisterHost(carrierType, host string) {
	host = normalizeHost(host)
	if host == "" || carrierType == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, known := range r.hosts[carrierType] {
		if known == host {
			return
		}
	}
	r.hosts[carrierType] = append(r.hosts[carrierType], host)
	r.byHost[host] = carrierType
	for _, suffix := range r.hosts[carrierType] {
		r.byHost[suffix] = carrierType
	}
}

// Pin restores a remembered assignment. Subjects are "<carrier>|<address>", so the carrier decides
// which pool the pin belongs to; an unknown carrier is ignored rather than guessed.
func (r *Router) Pin(subject, keyID string) {
	carrierType, _, ok := strings.Cut(subject, "|")
	if !ok {
		return
	}
	pool := r.Pool(carrierType)
	if pool == nil {
		return
	}
	pool.Pin(subject, keyID)
}

func (r *Router) Assignments() map[string]string {
	r.mu.RLock()
	carriers := make([]string, 0, len(r.pools))
	for name := range r.pools {
		carriers = append(carriers, name)
	}
	r.mu.RUnlock()
	out := map[string]string{}
	for _, name := range carriers {
		pool := r.Pool(name)
		if pool == nil {
			continue
		}
		for subject, id := range pool.PinnedSubjects() {
			out[subject] = id
		}
	}
	return out
}

func (r *Router) Exempt(host string) {
	host = normalizeHost(host)
	if host == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exempt[host] = true
}

func normalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "wss://")
	host = strings.TrimPrefix(host, "ws://")
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if i := strings.IndexByte(host, '@'); i >= 0 {
		host = host[i+1:]
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.Trim(host, "[]")
}

func (r *Router) carrierFor(host string) string {
	host = normalizeHost(host)
	if host == "" {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.exempt[host] {
		return ""
	}
	if t := r.byHost[host]; t != "" {
		return t
	}
	labels := strings.Split(host, ".")
	for i := 1; i < len(labels)-1; i++ {
		suffix := strings.Join(labels[i:], ".")
		if t := r.byHost[suffix]; t != "" {
			return t
		}
	}
	return ""
}

func isLocalAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()
}

func (r *Router) Dial(ctx context.Context, network, address string, direct transport.DialFunc) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return direct(ctx, network, address)
	}
	if isLocalAddress(address) {
		return direct(ctx, network, address)
	}
	carrierType := r.carrierFor(address)
	if carrierType == "" {
		return direct(ctx, network, address)
	}
	pool := r.Pool(carrierType)
	if pool == nil || pool.Len() == 0 {
		return direct(ctx, network, address)
	}
	subject := carrierType + "|" + address
	if hint, ok := transport.DialHintFrom(ctx); ok {
		subject = carrierType + "|" + hint.Subject
		if hint.Spread {
			return pool.DialSpread(ctx, subject, network, address)
		}
	}
	conn, err := pool.DialContext(ctx, subject, network, address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *Router) Hook() transport.DialHook { return r.Dial }

func (r *Router) Feedback(subject string, err error) {
	carrierType, _, ok := strings.Cut(subject, "|")
	if !ok {
		return
	}
	r.mu.RLock()
	pool := r.pools[carrierType]
	r.mu.RUnlock()
	if pool != nil {
		pool.Penalize(subject, err)
	}
}

func (r *Router) HintFeedback() transport.DialFeedback {
	return func(subject string, err error) {
		r.mu.RLock()
		carriers := make([]string, 0, len(r.pools))
		for name := range r.pools {
			carriers = append(carriers, name)
		}
		r.mu.RUnlock()
		for _, name := range carriers {
			r.Feedback(name+"|"+subject, err)
		}
	}
}
