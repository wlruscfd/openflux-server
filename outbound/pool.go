package outbound

import (
	"crypto/tls"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoProxy     = errors.New("outbound: no proxy key available")
	ErrProxyBanned = errors.New("outbound: proxy key is held after repeated rebalances")
)

type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type keyState struct {
	id   string
	dial Dialer

	mu             sync.Mutex
	label          string
	priority       int
	seen           bool
	healthy        bool
	consecutiveErr int
	latency        time.Duration
	cooldownUntil  time.Time
	lastErr        error

	dials    atomic.Uint64
	failures atomic.Uint64
}

func (s *keyState) recordSuccess(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := !s.seen
	s.seen, s.healthy, s.consecutiveErr = true, true, 0
	s.cooldownUntil, s.lastErr = time.Time{}, nil
	if d <= 0 {
		return
	}
	if first || s.latency == 0 {
		s.latency = d
		return
	}
	s.latency = (s.latency*3 + d) / 4
}

func (s *keyState) recordFailure(err error, cooldown time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen, s.healthy = true, false
	s.consecutiveErr++
	s.lastErr = err
	if until := time.Now().Add(cooldown); until.After(s.cooldownUntil) {
		s.cooldownUntil = until
	}
}

func (s *keyState) available(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consecutiveErr == 0 && s.seen {
		return true
	}
	return now.After(s.cooldownUntil)
}

func (s *keyState) score() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.seen {
		return math.Inf(1)
	}
	weight := float64(s.priority)
	if weight <= 0 {
		weight = 1
	}
	return float64(s.latency) * (1 + 4*float64(s.consecutiveErr)) / weight
}

type Snapshot struct {
	ID        string        `json:"id"`
	Label     string        `json:"label"`
	Healthy   bool          `json:"healthy"`
	Probed    bool          `json:"probed"`
	Latency   time.Duration `json:"latency"`
	Dials     uint64        `json:"dials"`
	Failures  uint64        `json:"failures"`
	Cooldown  time.Time     `json:"cooldown,omitzero"`
	LastError string        `json:"last_error,omitempty"`
}

func (s *keyState) snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := Snapshot{
		ID:       s.id,
		Label:    s.label,
		Healthy:  s.healthy,
		Probed:   s.seen,
		Latency:  s.latency,
		Dials:    s.dials.Load(),
		Failures: s.failures.Load(),
	}
	if !s.cooldownUntil.IsZero() {
		snap.Cooldown = s.cooldownUntil
	}
	if s.lastErr != nil {
		snap.LastError = s.lastErr.Error()
	}
	return snap
}

type PoolConfig struct {
	Cooldown      time.Duration
	RebalanceHold time.Duration
	FlapThreshold int
}

func (c PoolConfig) withDefaults() PoolConfig {
	if c.Cooldown <= 0 {
		c.Cooldown = 90 * time.Second
	}
	if c.RebalanceHold <= 0 {
		c.RebalanceHold = 10 * time.Minute
	}
	if c.FlapThreshold <= 0 {
		c.FlapThreshold = 3
	}
	return c
}

type KeySpec struct {
	ID       string
	Label    string
	Priority int
	Dial     Dialer
}

type Pool struct {
	cfg PoolConfig

	mu      sync.Mutex
	keys    []*keyState
	byID    map[string]*keyState
	pinned  map[string]string
	movedAt map[string][]time.Time
	heldTil map[string]time.Time
}

func NewPool(cfg PoolConfig) *Pool {
	return &Pool{
		cfg:     cfg.withDefaults(),
		byID:    map[string]*keyState{},
		pinned:  map[string]string{},
		movedAt: map[string][]time.Time{},
		heldTil: map[string]time.Time{},
	}
}

func (p *Pool) SetKeys(specs []KeySpec) {
	p.mu.Lock()
	defer p.mu.Unlock()
	keys := make([]*keyState, 0, len(specs))
	byID := make(map[string]*keyState, len(specs))
	for _, spec := range specs {
		st := p.byID[spec.ID]
		if st == nil {
			st = &keyState{id: spec.ID}
		}
		st.dial, st.label, st.priority = spec.Dial, spec.Label, spec.Priority
		keys = append(keys, st)
		byID[spec.ID] = st
	}
	p.keys, p.byID = keys, byID
	p.forgetDroppedLocked()
}

func (p *Pool) forgetDroppedLocked() {
	for subject, id := range p.pinned {
		if p.byID[id] == nil {
			delete(p.pinned, subject)
		}
	}
	for subject := range p.movedAt {
		if p.pinned[subject] == "" {
			delete(p.movedAt, subject)
		}
	}
	for subject := range p.heldTil {
		if p.pinned[subject] == "" {
			delete(p.heldTil, subject)
		}
	}
}

func (p *Pool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.keys)
}

func (p *Pool) Snapshots() []Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Snapshot, 0, len(p.keys))
	for _, st := range p.keys {
		out = append(out, st.snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (p *Pool) Pinned(subject string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pinned[subject]
}

func (p *Pool) Pin(subject, keyID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.byID[keyID] == nil {
		return
	}
	p.pinned[subject] = keyID
	delete(p.heldTil, subject)
	p.movedAt[subject] = nil
}

func (p *Pool) PinnedSubjects() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]string, len(p.pinned))
	for subject, id := range p.pinned {
		out[subject] = id
	}
	return out
}

func (p *Pool) Unpin(subject string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropLocked(subject)
}

func (p *Pool) dropLocked(subject string) {
	delete(p.pinned, subject)
	delete(p.heldTil, subject)
	delete(p.movedAt, subject)
}

func (p *Pool) heldLocked(subject string, now time.Time) bool {
	return now.Before(p.heldTil[subject])
}

func (p *Pool) bestLocked(now time.Time) *keyState {
	var best *keyState
	bestScore := math.Inf(1)
	for _, st := range p.keys {
		if !st.available(now) {
			continue
		}
		s := st.score()
		if best == nil || s < bestScore {
			best, bestScore = st, s
		}
	}
	return best
}

func (p *Pool) pickLocked(subject string, now time.Time) *keyState {
	if id := p.pinned[subject]; id == "" && p.heldLocked(subject, now) {
		return nil
	}
	if id := p.pinned[subject]; id != "" {
		if st := p.byID[id]; st != nil && st.available(now) {
			return st
		}
		if p.heldLocked(subject, now) {
			return nil
		}
		p.noteMoveLocked(subject, now)
		delete(p.pinned, subject)
	}
	st := p.bestLocked(now)
	if st == nil {
		return nil
	}
	p.pinned[subject] = st.id
	return st
}

func (p *Pool) noteMoveLocked(subject string, now time.Time) {
	moves := append(p.movedAt[subject], now)
	kept := moves[:0]
	for _, t := range moves {
		if now.Sub(t) <= p.cfg.RebalanceHold {
			kept = append(kept, t)
		}
	}
	p.movedAt[subject] = kept
	if len(kept) >= p.cfg.FlapThreshold {
		p.heldTil[subject] = now.Add(p.cfg.RebalanceHold)
		p.movedAt[subject] = nil
	}
}

func (p *Pool) spreadLocked(subject string, now time.Time) *keyState {
	if id := p.pinned[subject]; id != "" {
		if st := p.byID[id]; st != nil && st.available(now) {
			return st
		}
		delete(p.pinned, subject)
	}
	load := make(map[string]int, len(p.keys))
	for _, id := range p.pinned {
		load[id]++
	}
	var best *keyState
	for _, st := range p.keys {
		if !st.available(now) {
			continue
		}
		if best == nil || load[st.id] < load[best.id] || (load[st.id] == load[best.id] && st.score() < best.score()) {
			best = st
		}
	}
	if best != nil {
		p.pinned[subject] = best.id
	}
	return best
}

func (p *Pool) DialSpread(ctx context.Context, subject, network, address string) (net.Conn, error) {
	p.mu.Lock()
	st := p.spreadLocked(subject, time.Now())
	err := error(nil)
	if st == nil {
		err = p.unavailableErrorLocked()
	}
	p.mu.Unlock()
	if st == nil {
		return nil, err
	}
	started := time.Now()
	conn, err := st.dial.DialContext(ctx, network, address)
	st.dials.Add(1)
	if err == nil {
		st.recordSuccess(time.Since(started))
		return conn, nil
	}
	st.failures.Add(1)
	st.recordFailure(err, p.cfg.Cooldown)
	p.mu.Lock()
	delete(p.pinned, subject)
	p.mu.Unlock()
	return nil, fmt.Errorf("proxy %s: %w", st.label, err)
}

func (p *Pool) Penalize(subject string, err error) {
	p.mu.Lock()
	id := p.pinned[subject]
	st := p.byID[id]
	delete(p.pinned, subject)
	p.mu.Unlock()
	if st != nil {
		st.recordFailure(err, p.cfg.Cooldown)
	}
}

func (p *Pool) Acquire(subject string) *keyState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pickLocked(subject, time.Now())
}

func (p *Pool) unavailableErrorLocked() error {
	if len(p.keys) == 0 {
		return ErrNoProxy
	}
	now := time.Now()
	for _, st := range p.keys {
		if st.available(now) {
			return ErrNoProxy
		}
	}
	return ErrProxyBanned
}

func (p *Pool) DialContext(ctx context.Context, subject, network, address string) (net.Conn, error) {
	p.mu.Lock()
	st := p.pickLocked(subject, time.Now())
	err := error(nil)
	if st == nil {
		err = p.unavailableErrorLocked()
	}
	p.mu.Unlock()
	if st == nil {
		return nil, err
	}
	started := time.Now()
	conn, err := st.dial.DialContext(ctx, network, address)
	st.dials.Add(1)
	if err == nil {
		st.recordSuccess(time.Since(started))
		return conn, nil
	}
	st.failures.Add(1)
	st.recordFailure(err, p.cfg.Cooldown)
	p.mu.Lock()
	if p.pinned[subject] == st.id {
		p.noteMoveLocked(subject, time.Now())
		delete(p.pinned, subject)
	}
	p.mu.Unlock()
	return nil, fmt.Errorf("proxy %s: %w", st.label, err)
}

// ProbeKey checks one key against one address and records the outcome for that key.
//
// The pool's own probe answers a different question: it asks whether the carrier can reach
// the provider at all, through whichever key it likes best. A list of a thousand public
// proxies is mostly dead, and the panel has to know which ones are alive, so every key is
// checked on its own and the result is kept with the key.
//
// The check is a TLS handshake and not a plain connection: a provider that does not like the
// address a request comes from lets the connection open and closes it during the handshake.
// A key that only opens a socket cannot fetch the document, and marking it as one that can
// sends the carrier to a key that will only ever fail.
func (p *Pool) ProbeKey(ctx context.Context, keyID, network, address string) (time.Duration, error) {
	p.mu.Lock()
	st := p.byID[keyID]
	p.mu.Unlock()
	if st == nil {
		return 0, ErrNoProxy
	}
	started := time.Now()
	conn, err := st.dial.DialContext(ctx, network, address)
	if err != nil {
		return time.Since(started), err
	}
	defer conn.Close()

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	tlsConn := tls.Client(conn, &tls.Config{ServerName: host})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return time.Since(started), fmt.Errorf("tls %s: %w", host, err)
	}
	// The check does not touch the key's score. A check is not use: penalising a key for
	// failing a probe puts every key of a carrier into a cooldown at once, and the
	// carrier then has nothing left to dial with. The panel is told the result and
	// decides what to hand out next; the score belongs to the traffic that failed.
	return time.Since(started), nil
}

func (p *Pool) Probe(ctx context.Context, subject, network, address string) error {
	p.mu.Lock()
	var st *keyState
	if id := p.pinned[subject]; id != "" {
		st = p.byID[id]
	}
	if st == nil {
		st = p.bestLocked(time.Now())
	}
	p.mu.Unlock()
	if st == nil {
		return ErrNoProxy
	}
	started := time.Now()
	conn, err := st.dial.DialContext(ctx, network, address)
	st.dials.Add(1)
	if err != nil {
		st.failures.Add(1)
		st.recordFailure(err, p.cfg.Cooldown)
		return err
	}
	conn.Close()
	st.recordSuccess(time.Since(started))
	return nil
}
