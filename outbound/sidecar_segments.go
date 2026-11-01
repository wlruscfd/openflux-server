package outbound

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

// A sidecar is not one sing-box process but a few of them, one per batch of keys. Restarting a single
// process for every change of the key set cut every open carrier connection at once - rooms, tunnels,
// websockets - which is what made the whole node fall over each time the rotation moved. Now a key
// that already runs is never touched: keys that are new start in a process of their own, and a process
// whose keys have all left the set is kept alive for a while so the connections still on it can end.
const (
	// segmentDrain is how long a process nobody is assigned to is left running.
	segmentDrain = 10 * time.Minute
	// maxSegments bounds the processes the sidecar keeps; each one costs memory.
	maxSegments = 12
)

type segment struct {
	id       int
	cmd      *exec.Cmd
	keys     map[string]string // key id -> fingerprint of the key as it runs here
	dialers  map[string]Dialer
	retireAt time.Time // zero while some key of the segment is wanted
	exited   chan struct{}
}

func (g *segment) dead() bool {
	select {
	case <-g.exited:
		return true
	default:
		return false
	}
}

func keyFingerprint(k Key) string { return fingerprint([]Key{k}) }

func (s *Sidecar) segmentPID(id int) string {
	return filepath.Join(s.opts.WorkDir, "sing-box-"+strconv.Itoa(id)+".pid")
}

func (s *Sidecar) segmentConfig(id int) string {
	return filepath.Join(s.opts.WorkDir, "config-"+strconv.Itoa(id)+".json")
}

// Apply makes the keys given the set that is served. Keys that already run are left alone; only
// the ones that are new cost a process, and only that process is started.
func (s *Sidecar) Apply(keys []Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return fmt.Errorf("sing-box sidecar is stopped")
	}
	// A public key list is thousands of entries and some are broken. Refusing the whole batch over
	// one bad link takes every good key down with it, so unusable ones are dropped here and the
	// rest are still served.
	usable := make([]Key, 0, len(keys))
	var dropped []string
	for _, k := range keys {
		if err := k.Valid(); err != nil {
			dropped = append(dropped, fmt.Sprintf("%s: %v", k.Label(), err))
			continue
		}
		usable = append(usable, k)
	}
	for _, d := range dropped {
		if skippedInLog < 20 {
			utils.Debugf("[OUTBOUND] proxy key dropped: %s", d)
		}
		skippedInLog++
	}
	if len(usable) == 0 {
		return fmt.Errorf("none of the %d proxy keys are usable", len(dropped))
	}
	if !s.orphansKilled {
		s.killAllOrphans()
		s.orphansKilled = true
	}
	s.pruneDeadLocked()

	wanted := make(map[string]string, len(usable))
	var ordered []Key
	for _, k := range usable {
		if _, dup := wanted[k.ID]; dup {
			continue
		}
		wanted[k.ID] = keyFingerprint(k)
		ordered = append(ordered, k)
	}
	have := make(map[string]Dialer, len(wanted))
	for _, seg := range s.segments {
		inUse := false
		for id, fp := range seg.keys {
			if wanted[id] == fp {
				have[id] = seg.dialers[id]
				inUse = true
			}
		}
		if inUse {
			seg.retireAt = time.Time{}
		} else if seg.retireAt.IsZero() {
			seg.retireAt = time.Now()
		}
	}
	var fresh []Key
	for _, k := range ordered {
		if have[k.ID] == nil {
			fresh = append(fresh, k)
		}
	}
	s.reapLocked(time.Now(), false)
	if len(fresh) > 0 {
		started, err := s.startSegmentLocked(fresh)
		if err != nil {
			s.dialers = have
			return err
		}
		for id, d := range started {
			have[id] = d
		}
	}
	s.dialers = have
	s.startReaperLocked()
	return nil
}

func (s *Sidecar) startSegmentLocked(keys []Key) (map[string]Dialer, error) {
	if len(s.segments) >= maxSegments {
		s.reapLocked(time.Now(), true)
	}
	if len(s.segments) >= maxSegments {
		return nil, fmt.Errorf("sing-box already runs %d processes and none can be retired yet", len(s.segments))
	}
	ports, err := s.allocatePorts(len(keys))
	if err != nil {
		utils.Infof("[OUTBOUND] sing-box got no ports for %d keys: %v", len(keys), err)
		return nil, err
	}
	byID := make(map[string]int, len(keys))
	for _, k := range keys {
		byID[k.ID] = ports[len(byID)]
	}
	if err := os.MkdirAll(s.opts.WorkDir, 0o700); err != nil {
		return nil, err
	}
	id := s.nextSeg
	s.nextSeg++
	path := s.segmentConfig(id)
	config, err := buildSingboxConfig(keys, byID, s.opts.LogLevel)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, config, 0o600); err != nil {
		return nil, err
	}
	// sing-box validates a whole config or nothing: one link carrying a field this build has never
	// heard of would take every other key of the batch down with it.
	keys, err = s.dropKeysRefusedBySingbox(keys, path)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("sing-box refused every proxy key in this batch")
	}
	kept := make(map[string]int, len(keys))
	for _, k := range keys {
		kept[k.ID] = byID[k.ID]
	}
	byID = kept
	config, err = buildSingboxConfig(keys, byID, s.opts.LogLevel)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, config, 0o600); err != nil {
		return nil, err
	}
	cmd := exec.Command(s.opts.Binary, "run", "-c", path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = processAttributes()
	if err := cmd.Start(); err != nil {
		utils.Infof("[OUTBOUND] sing-box did not start: %v", err)
		return nil, fmt.Errorf("start sing-box: %w", err)
	}
	seg := &segment{
		id: id, cmd: cmd, keys: make(map[string]string, len(keys)),
		dialers: make(map[string]Dialer, len(keys)), exited: make(chan struct{}),
	}
	go func() { _ = cmd.Wait(); close(seg.exited) }()
	_ = os.WriteFile(s.segmentPID(id), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
	s.startedAt = time.Now()
	// One listener per key, checked one after another: a fixed wait is not enough for a few hundred.
	wait := s.opts.StartWait + time.Duration(len(byID))*150*time.Millisecond
	if err := waitPorts(byID, wait); err != nil {
		s.killSegment(seg)
		utils.Infof("[OUTBOUND] sing-box did not come up with %d keys: %v", len(byID), err)
		return nil, err
	}
	out := make(map[string]Dialer, len(keys))
	for _, k := range keys {
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(byID[k.ID]))
		d := newSocksDialer(addr, directDialer{}, 15*time.Second)
		seg.keys[k.ID] = keyFingerprint(k)
		seg.dialers[k.ID] = d
		out[k.ID] = d
	}
	s.segments = append(s.segments, seg)
	return out, nil
}

// killSegment ends the process and removes its files. It does not touch s.segments.
func (s *Sidecar) killSegment(seg *segment) {
	if seg.cmd != nil && seg.cmd.Process != nil && !seg.dead() {
		_ = seg.cmd.Process.Signal(terminateSignal())
		select {
		case <-seg.exited:
		case <-time.After(s.opts.StopGrace):
			_ = seg.cmd.Process.Kill()
			<-seg.exited
		}
	}
	_ = os.Remove(s.segmentPID(seg.id))
	_ = os.Remove(s.segmentConfig(seg.id))
}

// pruneDeadLocked forgets processes that died by themselves; their keys count as not running, so
// the next Apply starts them again.
func (s *Sidecar) pruneDeadLocked() {
	live := s.segments[:0]
	for _, seg := range s.segments {
		if seg.dead() {
			utils.Infof("[OUTBOUND] a sing-box process (segment %d, %d keys) exited on its own", seg.id, len(seg.keys))
			_ = os.Remove(s.segmentPID(seg.id))
			_ = os.Remove(s.segmentConfig(seg.id))
			continue
		}
		live = append(live, seg)
	}
	s.segments = live
}

// reapLocked stops processes that have had no wanted key for the drain period, or one at once when
// force is set and the process limit is reached.
func (s *Sidecar) reapLocked(now time.Time, force bool) {
	live := s.segments[:0]
	for _, seg := range s.segments {
		if !seg.retireAt.IsZero() && (force || now.Sub(seg.retireAt) >= segmentDrain) {
			s.killSegment(seg)
			force = false
			continue
		}
		live = append(live, seg)
	}
	s.segments = live
}

func (s *Sidecar) startReaperLocked() {
	if s.reaping {
		return
	}
	s.reaping = true
	go func() {
		for {
			time.Sleep(30 * time.Second)
			s.mu.Lock()
			if s.stopped {
				s.reaping = false
				s.mu.Unlock()
				return
			}
			s.pruneDeadLocked()
			s.reapLocked(time.Now(), false)
			s.mu.Unlock()
		}
	}()
}

func (s *Sidecar) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	for _, seg := range s.segments {
		s.killSegment(seg)
	}
	s.segments = nil
	s.dialers = map[string]Dialer{}
	s.clearPIDFileLocked()
}

// killAllOrphans removes every sing-box a previous run left behind, one pid file per process.
func (s *Sidecar) killAllOrphans() {
	s.killOrphan()
	files, _ := filepath.Glob(filepath.Join(s.opts.WorkDir, "sing-box-*.pid"))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		_ = os.Remove(f)
		if err != nil {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil || pid <= 1 || pid == os.Getpid() {
			continue
		}
		proc, err := os.FindProcess(pid)
		if err != nil || !alive(proc) {
			continue
		}
		_ = proc.Signal(terminateSignal())
		for i := 0; i < 40 && alive(proc); i++ {
			time.Sleep(50 * time.Millisecond)
		}
		if alive(proc) {
			_ = proc.Kill()
		}
		utils.Debugf("[OUTBOUND] removed a sing-box left over from pid %d", pid)
	}
}
