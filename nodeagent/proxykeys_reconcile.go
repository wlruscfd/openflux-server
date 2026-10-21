package nodeagent

import (
	"context"
	"fmt"
	"log"
	"sort"
	"os"
	"strings"
	"time"

	"github.com/p1neappleXpress/OpenFlux/outbound"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

const proxyKeyPollInterval = 30 * time.Second

// proxyKeyProbeInterval is how often every key is checked against the services its carrier
// has to reach. A public proxy list is mostly dead links, and the pool can only pick a
// working one if it knows which ones work.
const proxyKeyProbeInterval = 2 * time.Minute

// proxyKeyFailuresBeforeBroken is how many checks in a row a key has to fail before it is
// written down as broken. One failure is often the moment and not the key.
const proxyKeyFailuresBeforeBroken = 3

func singboxOptionsFromEnv() outbound.SidecarOptions {
	opts := outbound.SidecarOptions{LogLevel: "error"}
	if v := os.Getenv("OPENFLUX_SINGBOX"); v != "" {
		opts.Binary = v
	}
	if v := os.Getenv("OPENFLUX_SINGBOX_WORKDIR"); v != "" {
		opts.WorkDir = v
	}
	return opts
}

// proxyLoop keeps the outbound proxy in step with the panel. A key pool that cannot be built is left
// as it was: carriers keep dialing straight out rather than losing the tunnel entirely.
func (o *Orchestrator) proxyLoop(ctx context.Context) {
	ticker := time.NewTicker(proxyKeyPollInterval)
	defer ticker.Stop()
	probe := time.NewTicker(proxyKeyProbeInterval)
	defer probe.Stop()
	for {
		o.reconcileProxyKeys(ctx)
		select {
		case <-ctx.Done():
			return
		case <-probe.C:
			o.probeProxyKeys(ctx)
		case <-ticker.C:
		}
	}
}

// probeProxyKeys checks every key against the services its carrier has to reach.
//
// The check is per key, not per carrier: a list of a thousand public proxies is mostly dead,
// and the pool balances over what it knows. A key that opens a connection to the provider
// earns a good score, one that does not goes into a cooldown and is left alone.
func (o *Orchestrator) proxyKeyFailuresLocked() map[string]int {
	o.proxyKeyFailuresMu.Lock()
	defer o.proxyKeyFailuresMu.Unlock()
	if o.proxyKeyFailures == nil {
		o.proxyKeyFailures = make(map[string]int)
	}
	return o.proxyKeyFailures
}

func (o *Orchestrator) probeProxyKeys(ctx context.Context) {
	o.proxyMu.Lock()
	p := o.proxy
	o.proxyMu.Unlock()
	if p == nil {
		return
	}
	if p.Settling() {
		// The sidecar was just rebuilt: its listeners are not up yet, and a check now would
		// fail every key at once and take the carrier's working set with it.
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, proxyKeyProbeInterval)
	defer cancel()
	states := p.ProbeAll(probeCtx)
	if len(states) == 0 {
		return
	}
	alive := 0
	byCarrier := map[string]int{}
	carrierKeys := map[string]int{}
	lastError := ""
	// A key is checked once per carrier it serves, and the carriers reach different
	// services, so the same key can pass one and fail the next. It is one key, and it is
	// worth keeping if it reaches any of the services it is meant to reach.
	byKey := make(map[string]RemoteProxyKeyState, len(states))
	order := make([]string, 0, len(states))
	for _, s := range states {
		carrierKeys[s.Carrier]++
		state := "failed"
		if s.OK {
			state = "ok"
		}
		latency := int(s.Latency / time.Millisecond)
		entry := RemoteProxyKeyState{ID: s.ID, State: state, LatencyMS: &latency}
		if !s.OK {
			entry.Error = s.Error
			if lastError == "" {
				lastError = s.Error
			}
		}
		prev, seen := byKey[s.ID]
		if !seen {
			order = append(order, s.ID)
			byKey[s.ID] = entry
			continue
		}
		if prev.State != "ok" && s.OK {
			entry.Error = ""
			byKey[s.ID] = entry
		}
	}
	// A key that was working is only written down as broken after it failed a few checks
	// in a row. One failed check is often the network, not the key: a proxy that was busy
	// for a moment, a sidecar that was rebuilt, a provider that refused once. Demoting a
	// working key on the first failure is what makes a carrier work for a while and then
	// stop, because the key it depended on is dropped and never comes back.
	//
	// A key that has not failed enough times yet is not reported at all: the panel keeps
	// the state it had, which is the point of not throwing it away.
	fails := o.proxyKeyFailuresLocked()
	for id, entry := range byKey {
		if entry.State == "ok" {
			delete(fails, id)
			continue
		}
		// A refused connection to the local listener is not the key failing: the sidecar
		// was rebuilt and the key was not tested at all. Counting it would condemn a
		// working key every time the rotation rebuilt the sidecar.
		if strings.Contains(entry.Error, "connection refused") {
			continue
		}
		fails[id]++
	}
	report := make([]RemoteProxyKeyState, 0, len(order))
	for _, id := range order {
		entry := byKey[id]
		if entry.State == "ok" {
			alive++
			report = append(report, entry)
			continue
		}
		if fails[id] < proxyKeyFailuresBeforeBroken {
			continue
		}
		if strings.Contains(entry.Error, "connection refused") {
			continue
		}
		report = append(report, entry)
	}
	for _, s := range states {
		if s.OK {
			byCarrier[s.Carrier]++
		}
	}
	parts := make([]string, 0, len(carrierKeys))
	for carrier, n := range carrierKeys {
		parts = append(parts, fmt.Sprintf("%s=%d/%d", carrier, byCarrier[carrier], n))
	}
	sort.Strings(parts)
	log.Printf("[NODEAGENT] proxy keys checked: %d of %d reach their provider (%s); last failure: %s",
		alive, len(order), strings.Join(parts, " "), lastError)

	reportCtx, reportCancel := context.WithTimeout(ctx, 20*time.Second)
	defer reportCancel()
	if err := o.client.ReportProxyKeyStates(reportCtx, report); err != nil {
		utils.Debugf("[NODEAGENT] could not report proxy key states: %v", err)
	}
}

func (o *Orchestrator) reconcileProxyKeys(ctx context.Context) {
	resp, err := o.client.ListProxyKeys(ctx)
	if err != nil {
		utils.Debugf("[NODEAGENT] list proxy keys failed: %v", err)
		return
	}
	if len(resp.Keys) == 0 {
		o.teardownProxy()
		return
	}
	groups := map[string][]outbound.Key{}
	for _, rk := range resp.Keys {
		k, err := outbound.ParseLink(rk.Link)
		if err != nil {
			utils.Debugf("[NODEAGENT] proxy key %s (%s) is unusable: %v", rk.Name, rk.ID, err)
			continue
		}
		k.ID, k.Priority = rk.ID, rk.Priority
		if rk.Name != "" {
			k.Name = rk.Name
		}
		for _, carrier := range rk.Carriers {
			groups[carrier] = append(groups[carrier], k)
		}
	}
	if len(groups) == 0 {
		utils.Debugf("[NODEAGENT] no usable proxy key, carriers keep dialing directly")
		return
	}
	if o.proxy == nil {
		o.proxy = outbound.NewManager(singboxOptionsFromEnv())
	}
	if err := o.proxy.Apply(groups); err != nil {
		// A proxy that did not come up leaves the carriers dialing straight out, and the
		// provider that blocks this node's address is exactly why the proxy is there.
		utils.Infof("[NODEAGENT] the proxy pools did not come up: %v", err)
		utils.Debugf("[NODEAGENT] could not bring the proxy pools up: %v", err)
		return
	}
	if !o.proxyInstalled {
		o.proxy.LoadCarrierHosts()
		o.proxy.Install()
		o.proxyInstalled = true
	}
	o.restoreProxyAssignments(resp.Assignments)
	o.reportProxyAssignments(ctx)
}

func (o *Orchestrator) restoreProxyAssignments(assignments map[string]string) {
	for subject, proxyKeyID := range assignments {
		o.proxy.Router().Pin(subject, proxyKeyID)
	}
}

// reportProxyAssignments tells the controlplane where this node put each subject, so the next start
// comes back from the same proxy address and a provider check stays bound to it.
func (o *Orchestrator) reportProxyAssignments(ctx context.Context) {
	if !o.proxyInstalled {
		return
	}
	pins := o.proxy.Router().Assignments()
	if len(pins) == 0 {
		return
	}
	saveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := o.client.SaveProxyAssignments(saveCtx, pins); err != nil {
		utils.Debugf("[NODEAGENT] could not report proxy assignments: %v", err)
	}
}

func (o *Orchestrator) teardownProxy() {
	o.proxyMu.Lock()
	p := o.proxy
	installed := o.proxyInstalled
	o.proxy, o.proxyInstalled = nil, false
	o.proxyMu.Unlock()
	if p != nil {
		p.Stop()
	}
	if installed {
		utils.Debugf("[NODEAGENT] the panel has no proxy keys left, carriers dial directly again")
	}
}