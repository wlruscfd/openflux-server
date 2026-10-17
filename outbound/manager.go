package outbound

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

type sidecarRunner interface {
	Apply(keys []Key) error
	DialerFor(keyID string) Dialer
	Stop()
}

// MaxKeysPerCarrier bounds one pool. A node hands every carrier connection to one local port per key,
// so the cap is really a cap on how much a misconfigured panel can cost a node: sing-box opens one
// listener and one outbound per key, and a public key list has thousands of dead entries in it.
const MaxKeysPerCarrier = 64

func MaxKeysTotal() int { return MaxKeysPerCarrier * 8 }

type Manager struct {
	router  *Router
	sidecar sidecarRunner

	mu      sync.Mutex
	probeIn time.Duration
}

func NewManager(opts SidecarOptions) *Manager {
	return &Manager{router: NewRouter(), sidecar: NewSidecar(opts), probeIn: 60 * time.Second}
}

func (m *Manager) Router() *Router { return m.router }

func (m *Manager) LoadCarrierHosts() { m.router.LoadCarrierHosts() }

func (m *Manager) Install() {
	transport.SetDialHook(m.router.Hook())
	transport.SetDialFeedback(m.router.HintFeedback())
	utils.SafeGo("outbound.probe", m.probeLoop)
}

func (m *Manager) Uninstall() {
	transport.SetDialHook(nil)
	transport.SetDialFeedback(nil)
	m.sidecar.Stop()
}

func (m *Manager) Stop() {
	transport.SetDialHook(nil)
	transport.SetDialFeedback(nil)
	m.sidecar.Stop()
}

func (m *Manager) Apply(groups map[string][]Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	carriers := make([]string, 0, len(groups))
	for carrierType := range groups {
		carriers = append(carriers, carrierType)
	}
	sort.Strings(carriers)
	seen := map[string]bool{}
	all := []Key{}
	for _, carrierType := range carriers {
		keys := groups[carrierType]
		if len(keys) > MaxKeysPerCarrier {
			keys = keys[:MaxKeysPerCarrier]
		}
		for _, k := range keys {
			if seen[k.ID] || len(all) >= MaxKeysTotal() {
				continue
			}
			seen[k.ID] = true
			all = append(all, k)
		}
	}
	if err := m.sidecar.Apply(all); err != nil {
		return err
	}
	for carrierType, keys := range groups {
		specs := make([]KeySpec, 0, len(keys))
		for _, k := range keys {
			d := m.sidecar.DialerFor(k.ID)
			if d == nil {
				continue
			}
			specs = append(specs, KeySpec{ID: k.ID, Label: k.Name, Priority: k.Priority, Dial: d})
		}
		pool := m.router.Pool(carrierType)
		if pool == nil {
			pool = NewPool(PoolConfig{})
			m.router.SetPool(carrierType, pool)
		}
		pool.SetKeys(specs)
	}
	return nil
}

func (m *Manager) probeLoop() {
	for {
		time.Sleep(m.probeIn)
		for _, carrierType := range m.router.CarrierTypes() {
			pool := m.router.Pool(carrierType)
			if pool == nil {
				continue
			}
			for _, host := range carrierHostsOf(carrierType) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := pool.Probe(ctx, "health:"+carrierType+"|"+host, "tcp", host+":443"); err != nil {
					utils.Debugf("[OUTBOUND] %s probe via %s failed: %v", carrierType, host, err)
				}
				cancel()
			}
		}
	}
}

func carrierHostsOf(carrierType string) []string {
	return append([]string(nil), carrierHostTable[carrierType]...)
}
