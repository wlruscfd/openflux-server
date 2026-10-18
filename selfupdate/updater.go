package selfupdate

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

//go:embed guard.sh
var guardScript []byte

// DefaultIndexURL points at a rolling release whose assets are replaced on every publish.
const DefaultIndexURL = "https://github.com/wlruscfd/openflux-server/releases/download/node-stable/node-update.json"

const (
	maxBinaryBytes   = 200 << 20
	badVersionFor    = 12 * time.Hour
	minHealthyUptime = 90 * time.Second
	exitForRestart   = 75
)

type Config struct {
	IndexURL  string
	Interval  time.Duration
	Probation time.Duration
	// Healthy says whether this process has proven itself (for the node: it reached the control plane).
	Healthy func() bool
	Exe     string
	Keys    []ed25519.PublicKey
	HTTP    *http.Client
	Now     func() time.Time
	Exit    func(code int)
	// SkipRun leaves out the "new binary reports its version" check; for tests that stage a plain file.
	SkipRun bool
	Logf    func(format string, args ...any)
}

type Updater struct {
	cfg     Config
	started time.Time
}

func New(cfg Config) *Updater {
	if cfg.IndexURL == "" {
		cfg.IndexURL = DefaultIndexURL
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Minute
	}
	if cfg.Probation <= 0 {
		cfg.Probation = 10 * time.Minute
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 5 * time.Minute}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Exit == nil {
		cfg.Exit = os.Exit
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.Keys == nil {
		cfg.Keys = parseKeys(PinnedKeys)
	}
	return &Updater{cfg: cfg, started: cfg.Now()}
}

// WriteGuard installs the shell guard; its line endings are normalised because a checkout on Windows may have turned them into CRLF.
func WriteGuard(path string) error {
	body := bytes.ReplaceAll(guardScript, []byte("\r\n"), []byte("\n"))
	return os.WriteFile(path, body, 0o755)
}

func (u *Updater) logf(format string, args ...any) { u.cfg.Logf("[UPDATE] "+format, args...) }

// Enabled reports whether the guard that makes an update reversible is installed; without it the node never replaces itself.
func Enabled(exe string) bool {
	st, err := os.Stat(GuardPath(exe))
	return err == nil && !st.IsDir()
}

func (u *Updater) Run(ctx context.Context) {
	if !Enabled(u.cfg.Exe) {
		u.logf("auto-update is off: %s is missing (run install.sh again to enable it)", GuardPath(u.cfg.Exe))
		return
	}
	go u.watchProbation(ctx)

	timer := time.NewTimer(2*time.Minute + time.Duration(rand.Int63n(int64(3*time.Minute))))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if applied, err := u.CheckOnce(ctx); err != nil {
			u.logf("check failed: %v", err)
		} else if applied {
			return
		}
		timer.Reset(u.cfg.Interval + time.Duration(rand.Int63n(int64(2*time.Minute))))
	}
}

// watchProbation confirms the update once this process is healthy, or ends the process so that the guard rolls it back.
func (u *Updater) watchProbation(ctx context.Context) {
	path := StatePath(u.cfg.Exe)
	st := ReadState(path)
	if st.Phase != PhaseProbation || st.Version != Version {
		return
	}
	u.logf("version %s is on probation until it proves healthy", Version)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := u.cfg.Now()
		if now.Sub(u.started) >= minHealthyUptime && (u.cfg.Healthy == nil || u.cfg.Healthy()) {
			st := ReadState(path)
			st.Phase = ""
			st.Starts = 0
			if err := st.Write(path); err != nil {
				u.logf("could not record the confirmation: %v", err)
			}
			u.logf("version %s confirmed", Version)
			return
		}
		if now.Unix() > st.Deadline {
			u.logf("version %s did not become healthy in time, exiting so that the guard rolls back", Version)
			u.cfg.Exit(1)
			return
		}
	}
}

func (u *Updater) fetch(ctx context.Context, url string, limit int64) ([]byte, error) {
	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://127.0.0.1") {
		return nil, fmt.Errorf("refusing a non-https url: %s", url)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := u.cfg.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s answered %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%s is larger than %d bytes", url, limit)
	}
	return body, nil
}

// CheckOnce looks for a newer signed release and, when one exists, stages it and restarts into it.
func (u *Updater) CheckOnce(ctx context.Context) (bool, error) {
	statePath := StatePath(u.cfg.Exe)
	st := ReadState(statePath)
	if st.Phase == PhaseProbation {
		return false, nil
	}

	raw, err := u.fetch(ctx, u.cfg.IndexURL, maxIndexBytes)
	if err != nil {
		return false, err
	}
	sig, err := u.fetch(ctx, u.cfg.IndexURL+".sig", 4096)
	if err != nil {
		return false, err
	}
	idx, err := ParseIndex(raw, sig, u.cfg.Keys)
	if err != nil {
		return false, err
	}
	if CompareVersions(idx.Version, Version) <= 0 {
		return false, nil
	}
	if st.BadVersion == idx.Version && u.cfg.Now().Unix() < st.BadUntil {
		return false, nil
	}
	asset, ok := idx.Assets[PlatformKey()]
	if !ok {
		return false, fmt.Errorf("release %s has no build for %s", idx.Version, PlatformKey())
	}
	if len(asset.SHA256) != 64 {
		return false, errors.New("release asset has no sha256")
	}

	u.logf("downloading %s (current %s)", idx.Version, Version)
	bin, err := u.fetch(ctx, asset.URL, maxBinaryBytes)
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(bin)
	if !strings.EqualFold(hex.EncodeToString(sum[:]), asset.SHA256) {
		return false, errors.New("downloaded binary does not match the signed sha256")
	}

	newPath := u.cfg.Exe + ".new"
	if err := os.WriteFile(newPath, bin, 0o755); err != nil {
		return false, err
	}
	if !u.cfg.SkipRun {
		if err := checkRuns(newPath, idx.Version); err != nil {
			os.Remove(newPath)
			return false, err
		}
	}

	if err := copyFile(u.cfg.Exe, PrevPath(u.cfg.Exe)); err != nil {
		os.Remove(newPath)
		return false, fmt.Errorf("keeping the current binary: %w", err)
	}
	if err := os.Rename(newPath, u.cfg.Exe); err != nil {
		return false, fmt.Errorf("replacing the binary: %w", err)
	}
	ns := State{
		Phase:       PhaseProbation,
		Version:     idx.Version,
		PrevVersion: Version,
		Deadline:    u.cfg.Now().Add(u.cfg.Probation).Unix(),
		BadVersion:  st.BadVersion,
		BadUntil:    st.BadUntil,
	}
	if err := ns.Write(statePath); err != nil {
		os.Rename(PrevPath(u.cfg.Exe), u.cfg.Exe)
		return false, fmt.Errorf("recording the update: %w", err)
	}
	u.logf("version %s is installed, restarting into it", idx.Version)
	time.AfterFunc(2*time.Second, func() { u.cfg.Exit(exitForRestart) })
	return true, nil
}

func checkRuns(path, want string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "--version").Output()
	if err != nil {
		return fmt.Errorf("the new binary does not run: %w", err)
	}
	if got := strings.TrimSpace(string(out)); got != want {
		return fmt.Errorf("the new binary reports version %q, expected %q", got, want)
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
