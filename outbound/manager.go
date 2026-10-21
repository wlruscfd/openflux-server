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
	Settling() bool
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
	groups  map[string][]Key
	probeIn time.Duration
}

func NewManager(opts SidecarOptions) *Manager {
	return &Manager{router: NewRouter(), sidecar: NewSidecar(opts), probeIn: 60 * time.Second}
}

func (m *Manager) Router() *Router { return m.router }

// Settling reports whether the proxy was rebuilt so recently that a check would
// fail against listeners that are not up yet.
func (m *Manager) Settling() bool { return m.sidecar.Settling() }

func (m *Manager) LoadCarrierHosts() { m.router.LoadCarrierHosts() }

func (m *Manager) Install() {
	transport.SetDialHook(m.router.Hook())
	transport.SetDialFeedback(m.router.HintFeedback())
	transport.SetHostRegistrar(m.router.RegisterHost)
	utils.SafeGo("outbound.probe", m.probeLoop)
}

func (m *Manager) Uninstall() {
	transport.SetDialHook(nil)
	transport.SetDialFeedback(nil)
	transport.SetHostRegistrar(nil)
	m.sidecar.Stop()
}

func (m *Manager) Stop() {
	transport.SetDialHook(nil)
	transport.SetDialFeedback(nil)
	m.sidecar.Stop()
}

func (m *Manager) Apply(groups map[string][]Key) error {
	m.mu.Lock()
	m.groups = groups
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

// KeyState is what the node found when it last checked one key against the services that
// key is meant to reach. The panel keeps it, so the list can be sorted by what actually
// works instead of by the order the links were pasted in.
type KeyState struct {
	ID       string        `json:"id"`
	Carrier  string        `json:"carrier"`
	OK       bool          `json:"ok"`
	Latency  time.Duration `json:"latency"`
	Error    string        `json:"error,omitempty"`
	CheckedAt time.Time    `json:"checked_at"`
}

// ProbeAll checks every key of every carrier against the hosts that carrier has to reach.
//
// A public proxy list is mostly dead, so a key is only worth keeping if it opens a
// connection to the provider. The check is per key and per host, because a key that reaches
// one service is not necessarily the one that reaches the other.
func (m *Manager) ProbeAll(ctx context.Context) []KeyState {
	m.mu.Lock()
	groups := m.groups
	m.mu.Unlock()

	out := make([]KeyState, 0, 64)
	for carrierType := range groups {
		hosts := carrierHostsOf(carrierType)
		pool := m.router.Pool(carrierType)
		if pool == nil {
			continue
		}
		if len(hosts) == 0 {
			out = append(out, KeyState{ID: "-", Carrier: carrierType, Error: "no service host for this carrier",
				CheckedAt: time.Now()})
			continue
		}
		// Only the materialized keys are checked: they are the ones with a listener, and a
		// key nobody has tried yet must stay unknown rather than be written down as broken.
		for _, snap := range pool.Snapshots() {
			k := Key{ID: snap.ID, Name: snap.Label}
			ok := false
			var latency time.Duration
			var lastErr error
			for _, host := range hosts {
				probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
				took, err := pool.ProbeKey(probeCtx, k.ID, "tcp", host+":443")
				cancel()
				if err != nil {
					// Every host of the carrier has to answer. A key that reaches the
					// document but cannot open the channel is not a key this carrier
					// can work with, and the channel is where the traffic goes.
					ok = false
					latency = 0
					lastErr = err
					break
				}
				ok = true
				latency = took
			}
			state := KeyState{ID: k.ID, Carrier: carrierType, OK: ok, Latency: latency, CheckedAt: time.Now()}
			if !ok && lastErr != nil {
				state.Error = lastErr.Error()
			}
			out = append(out, state)
		}
	}
	return out
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
