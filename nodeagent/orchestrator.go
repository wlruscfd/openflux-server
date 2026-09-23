// Package nodeagent lets an exit-node process serve many keys at once, picking up newly added/disabled keys from openflux-control instead of one fixed --url per process.
package nodeagent

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

type Config struct {
	ControlURL      string
	NodeToken       string
	PollInterval    time.Duration
	UsageInterval   time.Duration
	HeartbeatPeriod time.Duration
	ExitMode        tunnel.ExitMode
	// PortRangeSize: raw mode's outbound ports per key - trades keys-per-node against connections-per-key; 0 = DefaultPortRangeSize.
	PortRangeSize int
	// CaptchaSolveMode: see yandex.CaptchaSolveMode. Applied to every yandex transport this orchestrator starts.
	CaptchaSolveMode yandex.CaptchaSolveMode
}

const DefaultPortRangeSize = 96

func DefaultConfig(controlURL, nodeToken string) Config {
	return Config{
		ControlURL:       controlURL,
		NodeToken:        nodeToken,
		PollInterval:     20 * time.Second,
		UsageInterval:    20 * time.Second,
		HeartbeatPeriod:  60 * time.Second,
		ExitMode:         tunnel.ExitModeRaw,
		PortRangeSize:    DefaultPortRangeSize,
		CaptchaSolveMode: yandex.CaptchaSolveModeHeadlessBrowser,
	}
}

// worker is normally a client-facing (Yandex) transport paired with its own raw/proxy exit tunnel.
// For a cascaded key, tun is nil and relayTo holds the UDP link to the final-exit node instead - trans
// still means the Yandex side, so usage accounting (based on trans.Stats()) is unaffected either way.
type worker struct {
	trans   transport.Transport
	tun     *tunnel.TCPTunnel
	relayTo transport.Transport
	docURL  string
	portIdx int

	lastSent uint64
	lastRecv uint64
}

// relayExitWorker is the other half of a cascade: it never talks to Yandex at all, just accepts the entry node's relayed traffic and runs a normal raw/proxy exit for it.
type relayExitWorker struct {
	trans   transport.Transport
	tun     *tunnel.TCPTunnel
	portIdx int
}

type Orchestrator struct {
	client *ControlClient
	cfg    Config

	mu               sync.Mutex
	workers          map[string]*worker
	relayExitWorkers map[string]*relayExitWorker
	ports            portAllocator
}

func NewOrchestrator(cfg Config) *Orchestrator {
	rangeSize := cfg.PortRangeSize
	if rangeSize <= 0 {
		rangeSize = DefaultPortRangeSize
	}
	return &Orchestrator{
		client:           NewControlClient(cfg.ControlURL, cfg.NodeToken),
		cfg:              cfg,
		workers:          make(map[string]*worker),
		relayExitWorkers: make(map[string]*relayExitWorker),
		ports:            portAllocator{rangeSize: rangeSize},
	}
}

const (
	portRangeBase = 1025
	portRangeMax  = 65535
)

type portAllocator struct {
	mu        sync.Mutex
	rangeSize int
	next      int
	free      []int
}

func (p *portAllocator) alloc() (idx int, start, end uint16, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fromFree := false
	if n := len(p.free); n > 0 {
		idx = p.free[n-1]
		p.free = p.free[:n-1]
		fromFree = true
	} else {
		idx = p.next
		p.next++
	}

	rangeStart := portRangeBase + idx*p.rangeSize
	rangeEnd := rangeStart + p.rangeSize - 1
	if rangeEnd > portRangeMax {
		if fromFree {
			p.free = append(p.free, idx)
		} else {
			p.next--
		}
		return 0, 0, 0, false
	}
	return idx, uint16(rangeStart), uint16(rangeEnd), true
}

func (p *portAllocator) release(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.free = append(p.free, idx)
}

func (o *Orchestrator) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); o.pollLoop(ctx) }()
	go func() { defer wg.Done(); o.usageLoop(ctx) }()
	go func() { defer wg.Done(); o.heartbeatLoop(ctx) }()

	wg.Wait()

	o.mu.Lock()
	defer o.mu.Unlock()
	for id, w := range o.workers {
		o.stopWorker(w)
		delete(o.workers, id)
	}
	for id, w := range o.relayExitWorkers {
		o.stopRelayExitWorker(w)
		delete(o.relayExitWorkers, id)
	}
}

func (o *Orchestrator) pollLoop(ctx context.Context) {
	o.reconcile(ctx)

	ticker := time.NewTicker(o.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.reconcile(ctx)
		}
	}
}

const workerStartStagger = 150 * time.Millisecond

func (o *Orchestrator) reconcile(ctx context.Context) {
	o.reconcileRelayExits(ctx)

	keys, err := o.client.ListKeys(ctx)
	if err != nil {
		utils.Debugf("[NODEAGENT] list keys failed: %v", err)
		return
	}

	active := make(map[string]RemoteKey, len(keys))
	for _, k := range keys {
		active[k.ID] = k
	}

	o.mu.Lock()
	for id, w := range o.workers {
		if _, stillActive := active[id]; !stillActive {
			utils.Debugf("[NODEAGENT] stopping worker for key %s (no longer active)", id)
			o.stopWorker(w)
			delete(o.workers, id)
		}
	}

	var toStart []RemoteKey
	for id, k := range active {
		if _, exists := o.workers[id]; exists {
			continue
		}
		if k.Transport != "yandex" && k.Transport != "yandex_multistream" {
			utils.Debugf("[NODEAGENT] skipping key %s: managed mode only supports the yandex/yandex_multistream transports today", id)
			continue
		}
		if k.Transport == "yandex_multistream" && len(k.DocURLs) < 2 {
			utils.Debugf("[NODEAGENT] skipping key %s: yandex_multistream needs 2+ doc_urls, got %d", id, len(k.DocURLs))
			continue
		}
		toStart = append(toStart, k)
	}
	o.mu.Unlock()

	// Worker starts are staggered outside o.mu so a cold start doesn't burst dozens of near-simultaneous connections from one exit-node IP, which looks like bot traffic to the doc providers.
	for i, k := range toStart {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(workerStartStagger):
			}
		}

		w, err := o.startWorker(k)
		if err != nil {
			// Port-range exhaustion is logged unconditionally (not gated behind --debug) since it otherwise fails silently, the same way, every poll cycle.
			if o.cfg.ExitMode == tunnel.ExitModeRaw && strings.Contains(err.Error(), "no port range capacity") {
				log.Printf("[NODEAGENT] key %s not started: %v - this node has reached its concurrent-key ceiling (raise it with --port-range-size, or run another node)", k.ID, err)
			} else {
				utils.Debugf("[NODEAGENT] failed to start worker for key %s: %v", k.ID, err)
			}
			continue
		}
		utils.Debugf("[NODEAGENT] started worker for key %s", k.ID)

		o.mu.Lock()
		o.workers[k.ID] = w
		o.mu.Unlock()
	}
}

// reconcileRelayExits is the final-exit half of a cascade, polled on the same tick as the regular
// entry-role reconcile above - a physical node can serve either role, or both, for different keys.
func (o *Orchestrator) reconcileRelayExits(ctx context.Context) {
	keys, err := o.client.ListRelayExitKeys(ctx)
	if err != nil {
		utils.Debugf("[NODEAGENT] list relay exit keys failed: %v", err)
		return
	}

	active := make(map[string]RelayExitKey, len(keys))
	for _, k := range keys {
		active[k.ID] = k
	}

	o.mu.Lock()
	for id, w := range o.relayExitWorkers {
		if _, stillActive := active[id]; !stillActive {
			utils.Debugf("[NODEAGENT] stopping relay-exit worker for key %s (no longer active)", id)
			o.stopRelayExitWorker(w)
			delete(o.relayExitWorkers, id)
		}
	}
	var toStart []RelayExitKey
	for id, k := range active {
		if _, exists := o.relayExitWorkers[id]; !exists {
			toStart = append(toStart, k)
		}
	}
	o.mu.Unlock()

	for _, k := range toStart {
		w, err := o.startRelayExitWorker(k)
		if err != nil {
			utils.Debugf("[NODEAGENT] failed to start relay-exit worker for key %s: %v", k.ID, err)
			continue
		}
		utils.Debugf("[NODEAGENT] started relay-exit worker for key %s", k.ID)
		o.mu.Lock()
		o.relayExitWorkers[k.ID] = w
		o.mu.Unlock()
	}
}

func (o *Orchestrator) startRelayExitWorker(k RelayExitKey) (*relayExitWorker, error) {
	portIdx := -1
	var portStart, portEnd uint16
	if o.cfg.ExitMode == tunnel.ExitModeRaw {
		var ok bool
		portIdx, portStart, portEnd, ok = o.ports.alloc()
		if !ok {
			return nil, fmt.Errorf("no port range capacity left on this node")
		}
	}

	relay := transport.NewUDPRelayTransport(net.JoinHostPort("0.0.0.0", strconv.Itoa(k.RelayPort)), "", k.Token, false, transport.DefaultConfig())
	if err := relay.Start(); err != nil {
		if portIdx >= 0 {
			o.ports.release(portIdx)
		}
		return nil, fmt.Errorf("start relay listener: %w", err)
	}

	tun := tunnel.NewTCPTunnelMode(relay, true, o.cfg.ExitMode)
	if portIdx >= 0 {
		tun.SetPortRange(portStart, portEnd)
	}
	return &relayExitWorker{trans: relay, tun: tun, portIdx: portIdx}, nil
}

func (o *Orchestrator) stopRelayExitWorker(w *relayExitWorker) {
	w.trans.Stop()
	w.tun.Close()
	if w.portIdx >= 0 {
		o.ports.release(w.portIdx)
	}
}

// startRelayBridgeWorker is the entry half of a cascade: it never touches raw sockets or gvisor at
// all, just pipes decoded bytes between the client-facing Yandex transport and the UDP link to the
// final-exit node, which is the one that actually dials the real internet.
func (o *Orchestrator) startRelayBridgeWorker(k RemoteKey) (*worker, error) {
	if k.Transport != "yandex" {
		return nil, fmt.Errorf("key %s: cascade only supports the yandex transport today", k.ID)
	}
	// E2E is meant to hide payload from anyone but the client and the final exit - an entry node that unwrapped it here would defeat that, so cascade+E2E is refused rather than silently getting the boundary wrong.
	if k.E2EEncryption {
		return nil, fmt.Errorf("key %s: e2e_encryption + cascade isn't supported yet (the final-exit node would need to own the E2E unwrap, not this one)", k.ID)
	}

	yd := yandex.NewYandexDocsTransport(k.DocURL, transport.DefaultConfig())
	yd.EnableSelfCompression()
	yd.SetCaptchaSolveMode(o.cfg.CaptchaSolveMode)
	if err := yd.Start(); err != nil {
		return nil, err
	}

	relay := transport.NewUDPRelayTransport(":0", net.JoinHostPort(*k.RelayHost, strconv.Itoa(*k.RelayPort)), k.Token, true, transport.DefaultConfig())
	if err := relay.Start(); err != nil {
		yd.Stop()
		return nil, fmt.Errorf("start relay link: %w", err)
	}

	yd.Receive(func(data []byte) {
		if err := relay.Send(data); err != nil {
			utils.Debugf("[NODEAGENT] relay send failed for key %s: %v", k.ID, err)
		}
	})
	relay.Receive(func(data []byte) {
		if err := yd.Send(data); err != nil {
			utils.Debugf("[NODEAGENT] yandex send failed for key %s: %v", k.ID, err)
		}
	})

	return &worker{trans: yd, relayTo: relay, docURL: k.DocURL, portIdx: -1}, nil
}

func (o *Orchestrator) startWorker(k RemoteKey) (*worker, error) {
	if k.IsRelayed() {
		return o.startRelayBridgeWorker(k)
	}

	// Raw mode needs a disjoint port range per worker (see TCPTunnel.SetPortRange); proxy mode's plain net.Dial needs no such reservation.
	portIdx := -1
	var portStart, portEnd uint16
	if o.cfg.ExitMode == tunnel.ExitModeRaw {
		var ok bool
		portIdx, portStart, portEnd, ok = o.ports.alloc()
		if !ok {
			return nil, fmt.Errorf("no port range capacity left on this node")
		}
	}

	// e2e_encryption is binding: if it's on for a key but there's no token to derive a matching key, refuse to start the worker rather than silently running unencrypted.
	if k.E2EEncryption && k.Token == "" {
		if portIdx >= 0 {
			o.ports.release(portIdx)
		}
		return nil, fmt.Errorf("key %s has e2e_encryption on but no usable token", k.ID)
	}

	// For yandex_multistream, each stream must compress/encrypt itself before MultiStreamTransport sees the data, since Send() reads streamIndex off what it assumes is a raw header.
	wrapStream := func(yd *yandex.YandexDocsTransport, idx int, perStreamKey bool) transport.Transport {
		yd.SetCaptchaSolveMode(o.cfg.CaptchaSolveMode)
		if !k.E2EEncryption {
			yd.EnableSelfCompression()
			return yd
		}
		if perStreamKey {
			yd.EnableEncryptedSelfCompressionForStream(k.Token, true, idx)
		} else {
			yd.EnableEncryptedSelfCompression(k.Token, true)
		}
		return yd
	}

	var trans transport.Transport
	label := k.DocURL
	if k.Transport == "yandex_multistream" {
		streams := make([]transport.Transport, len(k.DocURLs))
		for i, url := range k.DocURLs {
			streams[i] = wrapStream(yandex.NewYandexDocsTransport(url, transport.DefaultConfig()), i, true)
		}
		trans = transport.NewMultiStreamTransport(streams)
		label = strings.Join(k.DocURLs, ",")
	} else {
		trans = wrapStream(yandex.NewYandexDocsTransport(k.DocURL, transport.DefaultConfig()), 0, false)
	}
	if err := trans.Start(); err != nil {
		if portIdx >= 0 {
			o.ports.release(portIdx)
		}
		return nil, err
	}
	tun := tunnel.NewTCPTunnelMode(trans, true, o.cfg.ExitMode)
	if portIdx >= 0 {
		tun.SetPortRange(portStart, portEnd)
	}
	return &worker{trans: trans, tun: tun, docURL: label, portIdx: portIdx}, nil
}

func (o *Orchestrator) stopWorker(w *worker) {
	w.trans.Stop()
	if w.tun != nil {
		w.tun.Close()
	}
	if w.relayTo != nil {
		w.relayTo.Stop()
	}
	if w.portIdx >= 0 {
		o.ports.release(w.portIdx)
	}
}

func (o *Orchestrator) usageLoop(ctx context.Context) {
	ticker := time.NewTicker(o.cfg.UsageInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.reportUsage(ctx)
		}
	}
}

func (o *Orchestrator) reportUsage(ctx context.Context) {
	o.mu.Lock()
	deltas := make([]UsageDelta, 0, len(o.workers))
	for id, w := range o.workers {
		stats := w.trans.Stats()
		sentDelta := diffCounter(w.lastSent, stats.BytesSent)
		recvDelta := diffCounter(w.lastRecv, stats.BytesReceived)
		w.lastSent = stats.BytesSent
		w.lastRecv = stats.BytesReceived

		if sentDelta == 0 && recvDelta == 0 {
			continue
		}
		deltas = append(deltas, UsageDelta{
			KeyID:              id,
			BytesSentDelta:     int64(sentDelta),
			BytesReceivedDelta: int64(recvDelta),
		})
	}
	o.mu.Unlock()

	if len(deltas) == 0 {
		return
	}

	// The control plane rejects a usage request over 1000 deltas outright rather than partially applying it, so this chunks reports to keep enforcement correct past that scale.
	const usageChunkSize = 1000
	var disabledNow []string
	for start := 0; start < len(deltas); start += usageChunkSize {
		end := start + usageChunkSize
		if end > len(deltas) {
			end = len(deltas)
		}
		chunkDisabled, err := o.client.ReportUsage(ctx, deltas[start:end])
		if err != nil {
			utils.Debugf("[NODEAGENT] report usage failed: %v", err)
			continue
		}
		disabledNow = append(disabledNow, chunkDisabled...)
	}

	if len(disabledNow) == 0 {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range disabledNow {
		if w, ok := o.workers[id]; ok {
			utils.Debugf("[NODEAGENT] key %s went over quota, stopping worker", id)
			o.stopWorker(w)
			delete(o.workers, id)
		}
	}
}

// diffCounter treats a negative-looking diff (from a transport reconnect resetting counters) as "count from zero again" rather than underflowing a uint64 subtraction.
func diffCounter(previous, current uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}

func (o *Orchestrator) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(o.cfg.HeartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := o.client.Heartbeat(ctx); err != nil {
				utils.Debugf("[NODEAGENT] heartbeat failed: %v", err)
			}
		}
	}
}
