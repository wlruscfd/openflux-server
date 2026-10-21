package nodeagent

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/p1neappleXpress/OpenFlux/outbound"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

const proxyKeyPollInterval = 30 * time.Second

// proxyKeyProbeInterval is how often every key is checked against the services its carrier
// has to reach. A public proxy list is mostly dead links, and the pool can only pick a
// working one if it knows which ones work.
const proxyKeyProbeInterval = 2 * time.Minute

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
func (o *Orchestrator) probeProxyKeys(ctx context.Context) {
	o.proxyMu.Lock()
	p := o.proxy
	o.proxyMu.Unlock()
	if p == nil {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, proxyKeyProbeInterval)
	defer cancel()
	states := p.ProbeAll(probeCtx)
	if len(states) == 0 {
		return
	}
	alive := 0
	for _, s := range states {
		if s.OK {
			alive++
		}
	}
	log.Printf("[NODEAGENT] proxy keys checked: %d of %d reach their provider", alive, len(states))
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