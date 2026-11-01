package outbound

import (
	"context"
	"sort"
	"time"
)

// verifiedCarriers are the carriers whose provider limits how often one address may ask. They do
// not use every key of their pool: only keys that passed a check against the provider lately are
// dialed through, and the check itself is slow and goes round the keys a few at a time.
var verifiedCarriers = map[string]bool{"avito": true}

const (
	// carouselEvery is how often a verified carrier's keys are looked at.
	carouselEvery = 5 * time.Minute
	// carouselBatch is how many keys one round checks. Every check is a request to the provider.
	carouselBatch = 12
	// carouselFresh: a key verified this recently is skipped by the round.
	carouselFresh = verifyTTL / 3
)

// probeCarousel checks the next few keys of a carrier against its provider. Keys that were verified
// lately are not asked again, so the provider sees a handful of requests per round and the pool
// keeps a set of keys that were looked at, balanced between them by the pool itself.
func (m *Manager) probeCarousel(ctx context.Context, carrierType string, pool *Pool, hosts []string) []KeyState {
	if len(hosts) == 0 {
		return nil
	}
	now := time.Now()
	m.mu.Lock()
	if m.slowAt == nil {
		m.slowAt = map[string]time.Time{}
		m.cursor = map[string]int{}
	}
	if now.Sub(m.slowAt[carrierType]) < carouselEvery {
		m.mu.Unlock()
		return nil
	}
	m.slowAt[carrierType] = now
	start := m.cursor[carrierType]
	m.mu.Unlock()

	snaps := pool.Snapshots()
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].ID < snaps[j].ID })
	n := len(snaps)
	if n == 0 {
		return nil
	}
	start %= n
	out := make([]KeyState, 0, carouselBatch)
	next := start
	for i := 0; i < n && len(out) < carouselBatch; i++ {
		idx := (start + i) % n
		next = idx + 1
		id := snaps[idx].ID
		if at, ok := pool.VerifiedAt(id, now); ok && now.Sub(at) < carouselFresh {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		took, err := pool.ProbeKeyHTTP(probeCtx, id, hosts[0]+":443", "/")
		cancel()
		state := KeyState{ID: id, Carrier: carrierType, OK: err == nil, Latency: took, CheckedAt: time.Now()}
		if err != nil {
			state.Error = err.Error()
		}
		out = append(out, state)
	}
	m.mu.Lock()
	m.cursor[carrierType] = next
	m.mu.Unlock()
	return out
}
