package outbound

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

type directDialer struct{}

func (directDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

type SidecarOptions struct {
	Binary    string
	WorkDir   string
	PortLow   int
	PortHigh  int
	LogLevel  string
	StartWait time.Duration
	StopGrace time.Duration
}

func (o SidecarOptions) withDefaults() SidecarOptions {
	if o.Binary == "" {
		o.Binary = "sing-box"
	}
	if o.WorkDir == "" {
		o.WorkDir = filepath.Join(os.TempDir(), "openflux-singbox")
	}
	if o.PortLow == 0 {
		o.PortLow = 41000
	}
	if o.PortHigh == 0 {
		// The node opens one local listener per key it uses, so the range has to hold the
		// whole set the node materializes. It was a thousand ports, which a handful of
		// carriers with a few hundred keys each no longer fits in.
		o.PortHigh = 42999
	}
	if o.LogLevel == "" {
		o.LogLevel = "error"
	}
	if o.StartWait == 0 {
		o.StartWait = 15 * time.Second
	}
	if o.StopGrace == 0 {
		o.StopGrace = 5 * time.Second
	}
	return o
}

type Sidecar struct {
	opts SidecarOptions

	mu        sync.Mutex
	cmd       *exec.Cmd
	keys      []Key
	dialers   map[string]Dialer
	finger    string
	stopped   bool
	startedAt time.Time
}

func NewSidecar(opts SidecarOptions) *Sidecar {
	return &Sidecar{opts: opts.withDefaults(), dialers: map[string]Dialer{}}
}

// Settling reports whether the sidecar was started so recently that its listeners
// may not answer yet.
//
// The sidecar is rebuilt whenever the set of keys changes, and a check that runs
// against a half-started sidecar fails every key at once. Skipping the check until
// the listeners are up keeps one rebuild from condemning the whole set.
func (s *Sidecar) Settling() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startedAt.IsZero() {
		return false
	}
	return time.Since(s.startedAt) < 15*time.Second
}

func (s *Sidecar) DialerFor(keyID string) Dialer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dialers[keyID]
}

func (s *Sidecar) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cmd != nil
}

func fingerprint(keys []Key) string {
	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s|%s|%s|%d|%s|%s|%s|%s|%s|%v|%s|%s\n",
			k.ID, k.Kind, k.Server, k.Port, k.Method, k.Password, k.UUID,
			k.SNI, k.Flow, k.Reality, k.PublicKey, k.ShortID)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Sidecar) Apply(keys []Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return fmt.Errorf("sing-box sidecar is stopped")
	}
	// A public key list is thousands of entries and some are broken. Refusing the whole batch over
	// one bad link takes every good key down with it, so unusable ones are dropped here and the
	// rest are still served.
	usable := keys[:0]
	var dropped []string
	for _, k := range keys {
		if err := k.Valid(); err != nil {
			dropped = append(dropped, fmt.Sprintf("%s: %v", k.Label(), err))
			continue
		}
		usable = append(usable, k)
	}
	keys = usable
	for _, d := range dropped {
		if skippedInLog < 20 {
			utils.Debugf("[OUTBOUND] proxy key dropped: %s", d)
		}
		skippedInLog++
	}
	if len(keys) == 0 {
		return fmt.Errorf("none of the %d proxy keys are usable", len(dropped))
	}
	fp := fingerprint(keys)
	if fp == s.finger && s.cmd != nil {
		return nil
	}
	// The old process is stopped before the ports are counted: it is holding the very ports
	// the new one has to bind, and while it runs they look taken, so a set of keys that
	// fits the range does not look like it fits.
	s.stopProcessLocked()
	ports, err := s.allocatePorts(len(keys))
	if err != nil {
		utils.Infof("[OUTBOUND] sing-box got no ports for %d keys: %v", len(keys), err)
		return err
	}
	byID := make(map[string]int, len(keys))
	for _, k := range keys {
		byID[k.ID] = ports[len(byID)]
	}
	config, err := buildSingboxConfig(keys, byID, s.opts.LogLevel)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.opts.WorkDir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(s.opts.WorkDir, "config.json")
	if err := os.WriteFile(path, config, 0o600); err != nil {
		return err
	}
	// sing-box validates a whole config or nothing: one link carrying a field this build has never
	// heard of - a flow variant, an unknown transport - would take every other key down with it.
	// Checking first keeps one odd entry from costing the node its tunnel.
	keys, err = s.dropKeysRefusedBySingbox(keys, path)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("sing-box refused every proxy key in this batch")
	}
	keptPorts := make(map[string]int, len(keys))
	for _, k := range keys {
		keptPorts[k.ID] = byID[k.ID]
	}
	byID = keptPorts
	config, err = buildSingboxConfig(keys, byID, s.opts.LogLevel)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, config, 0o600); err != nil {
		return err
	}
	dialers := make(map[string]Dialer, len(keys))
	for _, k := range keys {
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(byID[k.ID]))
		dialers[k.ID] = newSocksDialer(addr, directDialer{}, 15*time.Second)
	}
	s.stopProcessLocked()
	s.killOrphan()
	cmd := exec.Command(s.opts.Binary, "run", "-c", path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = processAttributes()
	if err := cmd.Start(); err != nil {
		utils.Infof("[OUTBOUND] sing-box did not start: %v", err)
		return fmt.Errorf("start sing-box: %w", err)
	}
	s.startedAt = time.Now()
	s.writePIDFileLocked(cmd.Process.Pid)
	// sing-box opens one listener per key, and the listeners are checked one after another.
	// A fixed wait is not enough for a few hundred keys: the process is killed while it is
	// still coming up, and the node is left with no proxy at all and no way to see why.
	wait := s.opts.StartWait + time.Duration(len(byID))*150*time.Millisecond
	if err := waitPorts(byID, wait); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		s.clearPIDFileLocked()
		utils.Infof("[OUTBOUND] sing-box did not come up with %d keys: %v", len(byID), err)
		return err
	}
	s.cmd, s.keys, s.dialers, s.finger = cmd, keys, dialers, fp
	return nil
}

// dropKeysRefusedBySingbox asks sing-box itself which keys it will accept. The full config is tried
// first, so the common case costs one invocation; only when it refuses does each key get its own
// check, which names the offender instead of guessing from a message about a numbered outbound.
func (s *Sidecar) dropKeysRefusedBySingbox(keys []Key, path string) ([]Key, error) {
	probe := filepath.Join(filepath.Dir(path), "probe.json")
	if err := s.writeConfig(keys, probe); err != nil {
		return keys, err
	}
	if s.checkConfig(probe) == nil {
		return keys, nil
	}
	var kept []Key
	for _, k := range keys {
		if err := s.writeConfig([]Key{k}, probe); err != nil {
			continue
		}
		if err := s.checkConfig(probe); err != nil {
			if skippedInLog < 20 {
				utils.Debugf("[OUTBOUND] sing-box refused key %s (%s): %v", k.Label(), k.Endpoint(), err)
			}
			skippedInLog++
			continue
		}
		kept = append(kept, k)
	}
	return kept, nil
}

func (s *Sidecar) writeConfig(keys []Key, path string) error {
	cfg, err := buildSingboxConfig(keys, nil, s.opts.LogLevel)
	if err != nil {
		return err
	}
	return os.WriteFile(path, cfg, 0o600)
}

func (s *Sidecar) checkConfig(path string) error {
	out, err := exec.Command(s.opts.Binary, "check", "-c", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s", strings.TrimSpace(string(out)))
	}
	return nil
}

func (s *Sidecar) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.stopProcessLocked()
	s.finger = ""
}

func (s *Sidecar) stopProcessLocked() {
	cmd := s.cmd
	s.cmd = nil
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(terminateSignal())
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(s.opts.StopGrace):
		_ = cmd.Process.Kill()
		<-done
	}
	s.clearPIDFileLocked()
}

func (s *Sidecar) pidFile() string { return filepath.Join(s.opts.WorkDir, "sing-box.pid") }

func (s *Sidecar) writePIDFileLocked(pid int) {
	_ = os.MkdirAll(s.opts.WorkDir, 0o700)
	_ = os.WriteFile(s.pidFile(), []byte(strconv.Itoa(pid)), 0o600)
}

func (s *Sidecar) clearPIDFileLocked() {
	if s.opts.WorkDir != "" {
		_ = os.Remove(s.pidFile())
	}
}

// killOrphan finds the sidecar a previous run left behind. A node restart never calls Stop on the
// old process, and it keeps holding the very ports the new one has to bind: the new sing-box then
// cannot open a single listener and the whole proxy pool silently never comes up.
func (s *Sidecar) killOrphan() {
	raw, err := os.ReadFile(s.pidFile())
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 1 || pid == os.Getpid() {
		s.clearPIDFileLocked()
		return
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		s.clearPIDFileLocked()
		return
	}
	if !alive(proc) {
		s.clearPIDFileLocked()
		return
	}
	_ = proc.Signal(terminateSignal())
	for i := 0; i < 40 && alive(proc); i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if alive(proc) {
		_ = proc.Kill()
	}
	utils.Debugf("[OUTBOUND] removed a sing-box left over from pid %d", pid)
	s.clearPIDFileLocked()
}

func alive(proc *os.Process) bool {
	if proc == nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func (s *Sidecar) allocatePorts(count int) ([]int, error) {
	ports := make([]int, 0, count)
	taken := make(map[int]bool, count)
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	for next := s.opts.PortLow; len(ports) < count && next <= s.opts.PortHigh; next++ {
		if taken[next] {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(next)))
		if err != nil {
			continue
		}
		listeners = append(listeners, l)
		taken[next] = true
		ports = append(ports, next)
	}
	if len(ports) < count {
		return nil, fmt.Errorf("no free port for sing-box between %d and %d", s.opts.PortLow, s.opts.PortHigh)
	}
	return ports, nil
}

func waitPorts(byID map[string]int, within time.Duration) error {
	deadline := time.Now().Add(within)
	for id, port := range byID {
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		for {
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err == nil {
				conn.Close()
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("sing-box did not open local port %d for key %s within %s", port, id, within)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return nil
}

type singboxConfig struct {
	Log       singboxLog        `json:"log"`
	Inbounds  []json.RawMessage `json:"inbounds"`
	Outbounds []json.RawMessage `json:"outbounds"`
	Route     *singboxRoute     `json:"route,omitempty"`
}

type singboxRoute struct {
	Rules []singboxRouteRule `json:"rules"`
	Final string             `json:"final"`
}

type singboxRouteRule struct {
	Inbound  []string `json:"inbound"`
	Outbound string   `json:"outbound"`
}

type singboxLog struct {
	Level     string `json:"level"`
	Timestamp bool   `json:"timestamp"`
}

// singboxTransport builds the transport block a link asks for. Every published vless carries a
// type=, and the server speaks that framing only: a ws link answered with plain TCP reads back as
// noise and is indistinguishable from a dead server.
func singboxTransport(k Key) (map[string]any, bool) {
	path := k.Path
	if path == "" {
		path = "/"
	}
	switch k.TransportType {
	case "", "tcp":
		if k.HeaderType == "http" {
			tr := map[string]any{"type": "http", "path": path}
			if k.Host != "" {
				tr["host"] = []string{k.Host}
			}
			return tr, true
		}
		return nil, false
	case "ws", "websocket":
		tr := map[string]any{"type": "ws", "path": path}
		if k.Host != "" {
			tr["headers"] = map[string]any{"Host": k.Host}
		}
		return tr, true
	case "httpupgrade":
		tr := map[string]any{"type": "httpupgrade", "path": path}
		if k.Host != "" {
			tr["host"] = k.Host
		}
		return tr, true
	case "grpc":
		tr := map[string]any{"type": "grpc"}
		if k.ServiceName != "" {
			tr["service_name"] = k.ServiceName
		}
		if k.Mode != "" {
			tr["idle_timeout"] = "15s"
			tr["ping_timeout"] = "15s"
			tr["permit_without_stream"] = false
		}
		return tr, true
	case "http", "xhttp", "splithttp":
		tr := map[string]any{"type": "http", "path": path}
		if k.Host != "" {
			tr["host"] = []string{k.Host}
		}
		return tr, true
	default:
		return nil, false
	}
}

func singboxOutbound(k Key, tag string) (json.RawMessage, error) {
	switch k.Kind {
	case KindShadowsocks:
		return json.Marshal(map[string]any{
			"type": "shadowsocks", "tag": tag,
			"server": k.Server, "server_port": k.Port,
			"method": k.Method, "password": k.Password,
		})
	case KindVLESS:
		entry := map[string]any{
			"type": "vless", "tag": tag,
			"server": k.Server, "server_port": k.Port,
			"uuid": k.UUID,
		}
		if k.Flow != "" {
			entry["flow"] = k.Flow
		}
		if tr, ok := singboxTransport(k); ok {
			entry["transport"] = tr
		}
		if k.NoTLS {
			return json.Marshal(entry)
		}
		tls := map[string]any{"enabled": true}
		if k.SNI != "" {
			tls["server_name"] = k.SNI
		}
		if len(k.ALPN) > 0 {
			tls["alpn"] = k.ALPN
		}
		if k.Insecure {
			tls["insecure"] = true
		}
		// A fingerprint is requested by the link, not only by reality: the ordinary tls configs
		// that dominate public lists all carry fp=chrome.
		if k.Fingerprint != "" && !k.Reality {
			tls["utls"] = map[string]any{"enabled": true, "fingerprint": k.Fingerprint}
		}
		if k.Reality {
			tls["utls"] = map[string]any{"enabled": true, "fingerprint": k.Fingerprint}
			tls["reality"] = map[string]any{"enabled": true, "public_key": k.PublicKey, "short_id": k.ShortID}
		}
		entry["tls"] = tls
		return json.Marshal(entry)
	case KindHysteria2:
		tls := map[string]any{"enabled": true}
		if k.SNI != "" {
			tls["server_name"] = k.SNI
		}
		if k.Insecure {
			tls["insecure"] = true
		}
		return json.Marshal(map[string]any{
			"type": "hysteria2", "tag": tag,
			"server": k.Server, "server_port": k.Port,
			"password": k.Password, "tls": tls,
		})
	case KindVMess:
		entry := map[string]any{
			"type": "vmess", "tag": tag,
			"server": k.Server, "server_port": k.Port,
			"uuid": k.UUID, "alter_id": 0, "security": firstNonEmpty(k.Mode, "auto"),
		}
		if tr, ok := singboxTransport(k); ok {
			entry["transport"] = tr
		}
		if k.NoTLS {
			return json.Marshal(entry)
		}
		tls := map[string]any{"enabled": true}
		if k.SNI != "" {
			tls["server_name"] = k.SNI
		}
		if len(k.ALPN) > 0 {
			tls["alpn"] = k.ALPN
		}
		if k.Insecure {
			tls["insecure"] = true
		}
		if k.Fingerprint != "" {
			tls["utls"] = map[string]any{"enabled": true, "fingerprint": k.Fingerprint}
		}
		entry["tls"] = tls
		return json.Marshal(entry)
	case KindTrojan:
		entry := map[string]any{
			"type": "trojan", "tag": tag,
			"server": k.Server, "server_port": k.Port,
			"password": k.Password,
		}
		if tr, ok := singboxTransport(k); ok {
			entry["transport"] = tr
		}
		tls := map[string]any{"enabled": true}
		if k.SNI != "" {
			tls["server_name"] = k.SNI
		}
		if len(k.ALPN) > 0 {
			tls["alpn"] = k.ALPN
		}
		if k.Insecure {
			tls["insecure"] = true
		}
		if k.Fingerprint != "" {
			tls["utls"] = map[string]any{"enabled": true, "fingerprint": k.Fingerprint}
		}
		entry["tls"] = tls
		return json.Marshal(entry)
	default:
		return nil, fmt.Errorf("sing-box: unsupported key kind %q", k.Kind)
	}
}

func buildSingboxConfig(keys []Key, ports map[string]int, logLevel string) ([]byte, error) {
	cfg := singboxConfig{Log: singboxLog{Level: logLevel, Timestamp: true}}
	route := &singboxRoute{Final: "direct"}
	for i, k := range keys {
		tag := keyTag(i)
		out, err := singboxOutbound(k, tag)
		if err != nil {
			return nil, err
		}
		port, ok := ports[k.ID]
		if !ok {
			// A validation-only config has no ports yet; the listener address is not what is
			// being checked here, and sing-box still needs a number to accept.
			port = 1080 + i
		}
		in, err := json.Marshal(map[string]any{
			"type": "socks", "tag": tag + "-in",
			"listen": "127.0.0.1", "listen_port": port,
		})
		if err != nil {
			return nil, err
		}
		cfg.Outbounds = append(cfg.Outbounds, out)
		cfg.Inbounds = append(cfg.Inbounds, in)
		// Each listener has to be bound to its own outbound. Without the rule sing-box sends
		// everything that arrives on any of them out through the first outbound in the list,
		// so every key looked like the first one: the fleet appeared to be a single dead
		// address, and the rest of the keys were never used at all.
		route.Rules = append(route.Rules, singboxRouteRule{
			Inbound:  []string{tag + "-in"},
			Outbound: tag,
		})
	}
	direct, err := json.Marshal(map[string]any{"type": "direct", "tag": "direct"})
	if err != nil {
		return nil, err
	}
	cfg.Outbounds = append(cfg.Outbounds, direct)
	cfg.Route = route
	return json.MarshalIndent(cfg, "", "  ")
}

func keyTag(i int) string { return "ofx-key-" + strconv.Itoa(i) }

var skippedInLog int

func (k Key) Label() string {
	if k.Name != "" {
		return k.Name
	}
	return k.ID
}
