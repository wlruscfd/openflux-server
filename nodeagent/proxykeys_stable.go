package nodeagent

import (
	"sync"
	"time"

	"github.com/p1neappleXpress/OpenFlux/outbound"
)

// proxyRebuildMinInterval is how long a working key set is left alone. Every change to the set
// restarts sing-box, and a restart drops every open carrier connection at once - rooms, tunnels
// and websockets all go down together. The panel rotates its window of candidates every few
// minutes, so applying each window as it came made the whole node fall over on a timer.
const proxyRebuildMinInterval = 5 * time.Minute

// proxyRebuildUrgentInterval applies when a carrier has no working key in the set at all: waiting
// longer only keeps it down, so a rebuild is worth the cost, but not in a tight loop.
const proxyRebuildUrgentInterval = 2 * time.Minute

// proxyStabilizer keeps the set of keys handed to sing-box steady between rebuilds. A key that
// still works stays in the set even when the panel's window has moved past it, and only keys
// that failed their checks leave. New candidates are collected and applied together, rarely.
type proxyStabilizer struct {
	mu      sync.Mutex
	applied map[string][]outbound.Key // carrier -> keys last given to sing-box
	ok      map[string]bool           // key id -> passed its last check
	at      time.Time
}

// noteChecked records the outcome of a key's last check.
func (s *proxyStabilizer) noteChecked(id string, passed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ok == nil {
		s.ok = make(map[string]bool)
	}
	s.ok[id] = passed
}

// choose returns the groups to hand to sing-box now, and whether they differ from what runs.
// broken reports a key that failed enough checks in a row.
func (s *proxyStabilizer) choose(incoming map[string][]outbound.Key, broken func(id string) bool, now time.Time) (map[string][]outbound.Key, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applied == nil {
		return incoming, true
	}
	merged := make(map[string][]outbound.Key, len(incoming))
	urgent := false
	for carrier, in := range incoming {
		var keep []outbound.Key
		seen := map[string]bool{}
		working := 0
		for _, k := range s.applied[carrier] {
			if broken(k.ID) {
				continue
			}
			keep = append(keep, k)
			seen[k.ID] = true
			if s.ok[k.ID] {
				working++
			}
		}
		if working == 0 {
			urgent = true
		}
		for _, k := range in {
			if seen[k.ID] || broken(k.ID) {
				continue
			}
			seen[k.ID] = true
			keep = append(keep, k)
		}
		if len(keep) > outbound.MaxKeysPerCarrier {
			keep = keep[:outbound.MaxKeysPerCarrier]
		}
		merged[carrier] = keep
	}
	if sameGroups(merged, s.applied) {
		return s.applied, false
	}
	wait := proxyRebuildMinInterval
	if urgent {
		wait = proxyRebuildUrgentInterval
	}
	if now.Sub(s.at) < wait {
		return s.applied, false
	}
	return merged, true
}

// committed records the set that sing-box now runs.
func (s *proxyStabilizer) committed(groups map[string][]outbound.Key, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied, s.at = cloneGroups(groups), now
}

func (s *proxyStabilizer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = nil
}

func cloneGroups(g map[string][]outbound.Key) map[string][]outbound.Key {
	out := make(map[string][]outbound.Key, len(g))
	for c, keys := range g {
		out[c] = append([]outbound.Key(nil), keys...)
	}
	return out
}

func sameGroups(a, b map[string][]outbound.Key) bool {
	if len(a) != len(b) {
		return false
	}
	for c, ka := range a {
		kb, ok := b[c]
		if !ok || len(ka) != len(kb) {
			return false
		}
		ids := make(map[string]bool, len(kb))
		for _, k := range kb {
			ids[k.ID] = true
		}
		for _, k := range ka {
			if !ids[k.ID] {
				return false
			}
		}
	}
	return true
}
