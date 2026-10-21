// Package nodeagent lets an exit-node process serve many keys at once, picking up newly added/disabled keys from openflux-control instead of one fixed --url per process.
package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/p1neappleXpress/OpenFlux/outbound"
	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/transport/avito"
	"github.com/p1neappleXpress/OpenFlux/transport/cupsonline"
	"github.com/p1neappleXpress/OpenFlux/transport/mailru"
	"github.com/p1neappleXpress/OpenFlux/transport/mts"
	"github.com/p1neappleXpress/OpenFlux/transport/yandex"
	"github.com/p1neappleXpress/OpenFlux/warp"
	"github.com/p1neappleXpress/OpenFlux/nodetunnel"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

type Config struct {
	ControlURL      string
	NodeToken       string
	PollInterval    time.Duration
	UsageInterval   time.Duration
	HeartbeatPeriod time.Duration
	ExitMode        nodetunnel.ExitMode
	// PortRangeSize: raw mode's outbound ports per key - trades keys-per-node against connections-per-key; 0 = DefaultPortRangeSize.
	PortRangeSize    int
	CaptchaSolveMode yandex.CaptchaSolveMode
	AvitoCookies     string
}

const DefaultPortRangeSize = 96

func DefaultConfig(controlURL, nodeToken string) Config {
	return Config{
		ControlURL:       controlURL,
		NodeToken:        nodeToken,
		PollInterval:     20 * time.Second,
		UsageInterval:    20 * time.Second,
		HeartbeatPeriod:  60 * time.Second,
		ExitMode:         nodetunnel.ExitModeRaw,
		PortRangeSize:    DefaultPortRangeSize,
		CaptchaSolveMode: yandex.CaptchaSolveModeOff,
	}
}

// worker is normally a client-facing (Yandex) transport paired with its own raw/proxy exit nodetunnel.
// For a cascaded key, tun is nil and relayTo holds the UDP link to the final-exit node instead - trans
// still means the Yandex side, so usage accounting (based on trans.Stats()) is unaffected either way.
type worker struct {
	trans   transport.Transport
	tun     *nodetunnel.TCPTunnel
	relayTo transport.Transport
	docURL  string
	portIdx int

	transportName string
	docURLs       []string
	e2e           bool
	token         string
	cookieHash    string
	warpFP        string

	lastSent uint64
	lastRecv uint64
}

// driftedFrom reports whether the panel changed something about this key that the running
// transport was built from. Without this an edited doc_url or transport would keep using the old
// one until the node itself restarted, which looks like the edit was silently ignored.
func (w *worker) driftedFrom(k RemoteKey) bool {
	if w.transportName != k.Transport || w.docURL != k.DocURL || w.e2e != k.E2EEncryption {
		return true
	}
	if w.e2e && w.token != k.Token {
		return true
	}
	if w.warpFP != warpFingerprint(k) {
		return true
	}
	if len(w.docURLs) != len(k.DocURLs) {
		return true
	}
	for i := range k.DocURLs {
		if w.docURLs[i] != k.DocURLs[i] {
			return true
		}
	}
	return false
}

// relayExitWorker is the other half of a cascade: it never talks to Yandex at all, just accepts the entry node's relayed traffic and runs a normal raw/proxy exit for it.
type relayExitWorker struct {
	trans   transport.Transport
	tun     *nodetunnel.TCPTunnel
	portIdx int
}

type Orchestrator struct {
	client *ControlClient
	cfg    Config

	mu               sync.Mutex
	workers          map[string]*worker
	relayExitWorkers map[string]*relayExitWorker
	ports            portAllocator
	lastSummaryAt    time.Time
	warnedAt         map[string]time.Time

	startMu      sync.Mutex
	startRetryAt map[string]time.Time

	proxyKeyFailuresMu sync.Mutex
	proxyKeyFailures   map[string]int

	warpMu sync.Mutex
	warps  map[string]*warp.Tunnel

	lastPollOK atomic.Int64

	proxyMu        sync.Mutex
	proxy          *outbound.Manager
	proxyInstalled bool
}

// Healthy says whether this process has reached the control plane and fetched its keys at least once.
func (o *Orchestrator) Healthy() bool { return o.lastPollOK.Load() != 0 }

func NewOrchestrator(cfg Config) *Orchestrator {
	rangeSize := cfg.PortRangeSize
	if rangeSize <= 0 {
		rangeSize = DefaultPortRangeSize
	}
	return &Orchestrator{
		client:           NewControlClient(cfg.ControlURL, cfg.NodeToken),
		cfg:              cfg,
		workers:          make(map[string]*worker),
		warps:            make(map[string]*warp.Tunnel),
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
	wg.Add(4)

	go func() { defer wg.Done(); o.pollLoop(ctx) }()
	go func() { defer wg.Done(); o.usageLoop(ctx) }()
	go func() { defer wg.Done(); o.heartbeatLoop(ctx) }()
	go func() { defer wg.Done(); o.proxyLoop(ctx) }()

	wg.Wait()

	o.teardownProxy()

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
	o.closeWarps(nil)
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

const maxWorkerStartsPerReconcile = 100

// startRetryBackoff is how long a key waits after a failed start before it is tried again.
// A provider that throttles this node answers every attempt with an error, and hammering
// it keeps the block alive, so the wait has to outlast the provider's window.
const startRetryBackoff = 5 * time.Minute

type cookieProvider interface {
	ProvideCookies(cookieStr string)
}

type docURLProvider interface {
	ProvideDocURL(raw string)
}

// applyCookies pushes freshly uploaded jars into the transports already running here and drops
// the session so the next attempt presents them. A key the client can pass a provider check on
// but this node cannot is the whole point: the jar arrives from outside, not from a solver.
func (o *Orchestrator) applyCookies(ctx context.Context) {
	jars, err := o.client.ListKeyCookies(ctx)
	if err != nil {
		utils.Debugf("[NODEAGENT] list cookies failed: %v", err)
		return
	}
	byKey := make(map[string]RemoteKeyCookie, len(jars))
	for _, j := range jars {
		if j.KeyID != "" && j.Cookies != "" {
			byKey[j.KeyID] = j
		}
	}
	o.applyCookiesWith(byKey)
}

func (o *Orchestrator) applyCookiesWith(byKey map[string]RemoteKeyCookie) {
	if len(byKey) == 0 {
		return
	}

	o.mu.Lock()
	targets := make([]struct {
		id   string
		w    *worker
		hash string
		jar  string
		url  string
	}, 0, len(byKey))
	for id, j := range byKey {
		w, ok := o.workers[id]
		if !ok {
			continue
		}
		hash := cookieFingerprint(j.Cookies)
		if j.ResolvedURL != "" {
			hash = cookieFingerprint(j.Cookies + "|" + j.ResolvedURL)
		}
		if hash == "" || hash == w.cookieHash {
			continue
		}
		w.cookieHash = hash
		targets = append(targets, struct {
			id   string
			w    *worker
			hash string
			jar  string
			url  string
		}{id: id, w: w, hash: hash, jar: j.Cookies, url: j.ResolvedURL})
	}
	o.mu.Unlock()

	for _, t := range targets {
		o.mu.Lock()
		current, stillActive := o.workers[t.id]
		o.mu.Unlock()
		if !stillActive || current != t.w {
			utils.Debugf("[NODEAGENT] worker for key %s no longer active, dropping uploaded cookies", t.id)
			continue
		}
		provider, ok := t.w.trans.(cookieProvider)
		if !ok {
			log.Printf("[NODEAGENT] key %s (%s): a cookie jar was uploaded but this transport cannot use it", t.id, t.w.transportName)
			continue
		}
		log.Printf("[NODEAGENT] key %s (%s): applying the uploaded cookie jar (%d bytes) and reconnecting", t.id, t.w.transportName, len(t.jar))
		provider.ProvideCookies(t.jar)
		if t.url != "" {
			if dprov, ok := t.w.trans.(docURLProvider); ok {
				dprov.ProvideDocURL(t.url)
			} else {
				log.Printf("[NODEAGENT] key %s (%s): resolved document url ignored, this transport does not take one", t.id, t.w.transportName)
			}
		}
		transport.ForceReconnect(t.w.trans)
	}
}

func cookieFingerprint(jar string) string {
	sum := sha256.Sum256([]byte(jar))
	return hex.EncodeToString(sum[:8])
}

// startBackoffActive reports whether a key that failed to start recently still has to wait.
// A provider that is throttling this node answers every attempt with an error, and trying
// again straight away only keeps the block alive, so a failed key waits a few minutes.
func (o *Orchestrator) startBackoffActive(keyID string) bool {
	o.startMu.Lock()
	defer o.startMu.Unlock()
	until, ok := o.startRetryAt[keyID]
	return ok && time.Now().Before(until)
}

func (o *Orchestrator) noteStartFailure(keyID string) {
	o.startMu.Lock()
	defer o.startMu.Unlock()
	if o.startRetryAt == nil {
		o.startRetryAt = make(map[string]time.Time)
	}
	o.startRetryAt[keyID] = time.Now().Add(startRetryBackoff)
}

func (o *Orchestrator) clearStartFailure(keyID string) {
	o.startMu.Lock()
	defer o.startMu.Unlock()
	delete(o.startRetryAt, keyID)
}

func (o *Orchestrator) avitoCookiesFor(keyID string) string {	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	jars, err := o.client.ListKeyCookies(ctx)
	if err != nil {
		utils.Debugf("[NODEAGENT] key %s: cookies lookup failed: %v", keyID, err)
	}
	for _, j := range jars {
		if j.KeyID == keyID && j.Cookies != "" {
			return j.Cookies
		}
	}
	return o.cfg.AvitoCookies
}

func (o *Orchestrator) publishRoomLists(trans transport.Transport, k RemoteKey) {
	rl, ok := trans.(interface {
		OnRoomList(func(string))
		RoomList() string
	})
	if !ok {
		return
	}
	var last atomic.Pointer[string]
	rl.OnRoomList(func(list string) {
		if list == "" || last.Load() == &list {
			return
		}
		published := list
		last.Store(&published)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := o.client.ReportRoomList(ctx, k.ID, published); err != nil {
				utils.Debugf("[NODEAGENT] key %s (%s): room list report failed: %v", k.ID, k.Transport, err)
			}
		}()
	})
}

func (o *Orchestrator) reconcile(ctx context.Context) {
	o.reconcileRelayExits(ctx)
	o.applyCookies(ctx)

	keys, err := o.client.ListKeys(ctx)
	if err != nil {
		o.warnThrottled("list-keys", "[NODEAGENT] cannot fetch keys from the controlplane (%s): %v", o.cfg.ControlURL, err)
		return
	}
	o.lastPollOK.Store(time.Now().UnixNano())

	active := make(map[string]RemoteKey, len(keys))
	avitoKeys := 0
	for _, k := range keys {
		active[k.ID] = k
		if k.Transport == "avito" {
			avitoKeys++
		}
	}
	avito.SetFleetSize(avitoKeys)

	var toStart []RemoteKey
	var toStop []*worker
	o.mu.Lock()
	for id, w := range o.workers {
		k, stillActive := active[id]
		if !stillActive {
			utils.Debugf("[NODEAGENT] stopping worker for key %s (no longer active)", id)
			toStop = append(toStop, w)
			delete(o.workers, id)
			continue
		}
		if w.driftedFrom(k) {
			utils.Debugf("[NODEAGENT] restarting worker for key %s (doc/transport/e2e changed in the panel)", id)
			toStop = append(toStop, w)
			delete(o.workers, id)
			toStart = append(toStart, k)
		}
	}

	for id, k := range active {
		if _, exists := o.workers[id]; exists {
			continue
		}
		if o.startBackoffActive(id) {
			continue
		}
		if k.Transport != "yandex" && k.Transport != "yandex_multistream" && k.Transport != "boards" && k.Transport != "mailru" && k.Transport != "mts" && k.Transport != "telemost" && k.Transport != "cupsonline" && k.Transport != "avito" {
			utils.Debugf("[NODEAGENT] skipping key %s: managed mode only supports the yandex/yandex_multistream/boards/mailru/mts/telemost/cupsonline/avito transports today", id)
			continue
		}
		if k.Transport == "yandex_multistream" && len(k.DocURLs) < 2 {
			utils.Debugf("[NODEAGENT] skipping key %s: yandex_multistream needs 2+ doc_urls, got %d", id, len(k.DocURLs))
			continue
		}
		toStart = append(toStart, k)
	}
	o.mu.Unlock()

	for _, w := range toStop {
		o.stopWorker(w)
	}
	if len(toStart) > maxWorkerStartsPerReconcile {
		toStart = toStart[:maxWorkerStartsPerReconcile]
	}

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
			o.noteStartFailure(k.ID)
			// Port-range exhaustion is logged unconditionally (not gated behind --debug) since it otherwise fails silently, the same way, every poll cycle.
			if o.cfg.ExitMode == nodetunnel.ExitModeRaw && strings.Contains(err.Error(), "no port range capacity") {
				log.Printf("[NODEAGENT] key %s not started: %v - this node has reached its concurrent-key ceiling (raise it with --port-range-size, or run another node)", k.ID, err)
			} else {
				// A key that cannot start is the one thing an operator needs to see: without
				// this line a blocked provider is indistinguishable from a key that is simply
				// still waiting, and the panel shows an empty document for good.
				o.warnThrottled("start-"+k.ID, "[NODEAGENT] key %s (%s) not started: %v", k.ID, k.Transport, err)
				utils.Debugf("[NODEAGENT] failed to start worker for key %s: %v", k.ID, err)
			}
			continue
		}
		o.clearStartFailure(k.ID)
		log.Printf("[NODEAGENT] key %s (%s): worker started", k.ID, k.Transport)

		o.mu.Lock()
		o.workers[k.ID] = w
		o.mu.Unlock()
	}
	o.sweepWarps()
	o.logSummary()
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
	if o.cfg.ExitMode == nodetunnel.ExitModeRaw {
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

	tun := nodetunnel.NewTCPTunnelMode(relay, true, o.cfg.ExitMode)
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

	return &worker{
		trans:         yd,
		relayTo:       relay,
		docURL:        k.DocURL,
		token:         k.Token,
		portIdx:       -1,
		transportName: k.Transport,
		docURLs:       append([]string(nil), k.DocURLs...),
		e2e:           k.E2EEncryption,
	}, nil
}

func (o *Orchestrator) startWorker(k RemoteKey) (*worker, error) {
	if k.IsRelayed() {
		return o.startRelayBridgeWorker(k)
	}

	// Raw mode needs a disjoint port range per worker (see TCPTunnel.SetPortRange); proxy mode's plain net.Dial needs no such reservation.
	portIdx := -1
	var portStart, portEnd uint16
	if o.cfg.ExitMode == nodetunnel.ExitModeRaw {
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
	switch k.Transport {
	case "boards":
		if k.E2EEncryption {
			if portIdx >= 0 {
				o.ports.release(portIdx)
			}
			return nil, fmt.Errorf("key %s: boards transport doesn't support e2e_encryption yet", k.ID)
		}
		trans = yandex.NewBoardsBatched(yandex.NewBoardsTransport(k.DocURL, transport.DefaultConfig()))
	case "mailru":
		if k.E2EEncryption {
			if portIdx >= 0 {
				o.ports.release(portIdx)
			}
			return nil, fmt.Errorf("key %s: mailru transport doesn't support e2e_encryption yet", k.ID)
		}
		trans = transport.NewCompressedTransport(mailru.NewMailruDocsTransport(k.DocURL, transport.DefaultConfig()))
	case "mts":
		if k.E2EEncryption {
			if portIdx >= 0 {
				o.ports.release(portIdx)
			}
			return nil, fmt.Errorf("key %s: mts transport doesn't support e2e_encryption yet", k.ID)
		}
		trans = newMTSExitTransport(k.DocURL)
	case "telemost":
		if k.E2EEncryption {
			if portIdx >= 0 {
				o.ports.release(portIdx)
			}
			return nil, fmt.Errorf("key %s: telemost transport doesn't support e2e_encryption yet", k.ID)
		}
		trans = yandex.NewTelemostBatched(k.DocURL, true, transport.DefaultConfig())
	case "cupsonline":
		if k.E2EEncryption {
			if portIdx >= 0 {
				o.ports.release(portIdx)
			}
			return nil, fmt.Errorf("key %s: cupsonline transport doesn't support e2e_encryption yet", k.ID)
		}
		trans = transport.NewBatchedTransport(cupsonline.NewCupsonlineTransport(k.DocURL, transport.DefaultConfig(), false))
		o.publishRoomLists(trans, k)
	case "avito":
		if k.E2EEncryption {
			if portIdx >= 0 {
				o.ports.release(portIdx)
			}
			return nil, fmt.Errorf("key %s: avito transport doesn't support e2e_encryption yet", k.ID)
		}
		trans = transport.NewStripedRoom(avito.NewAvitoTransport(k.DocURL, transport.DefaultConfig(), o.avitoCookiesFor(k.ID), false))
		o.publishRoomLists(trans, k)
	case "yandex_multistream":
		streams := make([]transport.Transport, len(k.DocURLs))
		for i, url := range k.DocURLs {
			streams[i] = wrapStream(yandex.NewYandexDocsTransport(url, transport.DefaultConfig()), i, true)
		}
		trans = transport.NewMultiStreamTransport(streams)
		label = strings.Join(k.DocURLs, ",")
	default:
		trans = wrapStream(yandex.NewYandexDocsTransport(k.DocURL, transport.DefaultConfig()), 0, false)
	}
	transport.SetEventCallback(trans, keyEventLogger(k.ID, k.Transport))
	if err := trans.Start(); err != nil {
		if portIdx >= 0 {
			o.ports.release(portIdx)
		}
		return nil, err
	}
	tun := nodetunnel.NewTCPTunnelMode(trans, true, o.cfg.ExitMode)
	tun.TuneFor(k.Transport)
	warpFP := warpFingerprint(k)
	if k.Warp != nil {
		wt, werr := o.warpTunnel(k.Warp)
		if werr != nil {
			trans.Stop()
			tun.Close()
			if portIdx >= 0 {
				o.ports.release(portIdx)
			}
			return nil, fmt.Errorf("key %s: warp: %w", k.ID, werr)
		}
		tun.SetOutboundDialer(wt.DialContext)
	}
	if portIdx >= 0 {
		tun.SetPortRange(portStart, portEnd)
	}
	return &worker{
		warpFP:        warpFP,
		trans:         trans,
		tun:           tun,
		docURL:        label,
		portIdx:       portIdx,
		transportName: k.Transport,
		docURLs:       append([]string(nil), k.DocURLs...),
		e2e:           k.E2EEncryption,
		token:         k.Token,
	}, nil
}

// newMTSExitTransport must mirror how the client builds its mts transport in mobile.wrapGeneric.
// The client wraps mts in a CompressedTransport, which prefixes each packet with a marker byte;
// an exit that skips the wrapper receives every packet with that marker still attached, so the
// IPv4 header lands at offset 1 and the tunnel discards it without an error. mts was silently
// one-way until this was matched.
func newMTSExitTransport(docURL string) transport.Transport {
	return transport.NewCompressedTransport(mts.NewTransport(docURL, transport.DefaultConfig()))
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

	var toStop []*worker
	o.mu.Lock()
	for _, id := range disabledNow {
		if w, ok := o.workers[id]; ok {
			utils.Debugf("[NODEAGENT] key %s went over quota, stopping worker", id)
			toStop = append(toStop, w)
			delete(o.workers, id)
		}
	}
	o.mu.Unlock()
	for _, w := range toStop {
		o.stopWorker(w)
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

func warpConfig(w *RemoteWarp) warp.Config {
	return warp.Config{ProfileID: w.ProfileID, PrivateKey: w.PrivateKey, PeerPublicKey: w.PeerPublicKey,
		Endpoint: w.Endpoint, AddressV4: w.AddressV4, AddressV6: w.AddressV6}
}

func warpFingerprint(k RemoteKey) string {
	if k.Warp == nil || k.IsRelayed() {
		return ""
	}
	return warpConfig(k.Warp).Fingerprint()
}

// warpTunnel shares one WireGuard link per WARP profile across every key that uses it; a key never falls back to the node's own address when the link is down.
func (o *Orchestrator) warpTunnel(w *RemoteWarp) (*warp.Tunnel, error) {
	cfg := warpConfig(w)
	fp := cfg.Fingerprint()
	o.warpMu.Lock()
	defer o.warpMu.Unlock()
	if t, ok := o.warps[fp]; ok {
		return t, nil
	}
	t, err := warp.New(cfg)
	if err != nil {
		return nil, err
	}
	o.warps[fp] = t
	go o.watchWarp(fp, t)
	return t, nil
}

// watchWarp says in the log, once and plainly, whether the link to Cloudflare carries traffic - a blocked UDP port shows up as slow or missing sites, not as an error.
func (o *Orchestrator) watchWarp(fp string, t *warp.Tunnel) {
	for attempt := 1; attempt <= 6; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		ok, rtt, err := t.Probe(ctx)
		cancel()
		switch {
		case err == nil && ok:
			log.Printf("[WARP] link is up: traffic leaves as WARP, probe took %dms", rtt.Milliseconds())
			return
		case err == nil:
			log.Printf("[WARP] link answers but the traffic is not WARP (attempt %d)", attempt)
		default:
			log.Printf("[WARP] no answer through the link (attempt %d): %v - check that this node can send UDP to Cloudflare on port 2408", attempt, err)
		}
		o.warpMu.Lock()
		_, alive := o.warps[fp]
		o.warpMu.Unlock()
		if !alive {
			return
		}
		time.Sleep(time.Duration(attempt) * 10 * time.Second)
	}
}

func (o *Orchestrator) sweepWarps() {
	o.mu.Lock()
	inUse := make(map[string]bool, len(o.workers))
	for _, w := range o.workers {
		if w.warpFP != "" {
			inUse[w.warpFP] = true
		}
	}
	o.mu.Unlock()
	o.closeWarps(inUse)
}

func (o *Orchestrator) closeWarps(keep map[string]bool) {
	o.warpMu.Lock()
	defer o.warpMu.Unlock()
	for fp, t := range o.warps {
		if !keep[fp] {
			t.Close()
			delete(o.warps, fp)
		}
	}
}
