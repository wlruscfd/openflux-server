package selfupdate

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"0.1.0", "0.1.0", 0},
		{"0.1.1", "0.1.0", 1},
		{"0.2", "0.10", -1},
		{"1.0.0", "v0.9.9", 1},
		{"dev", "0.0.1", -1},
		{"0.0.1", "dev", 1},
		{"dev", "dev", 0},
	}
	for _, c := range cases {
		if got := CompareVersions(c.a, c.b); got != c.want {
			t.Errorf("CompareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func signedIndex(t *testing.T, priv ed25519.PrivateKey, idx Index) ([]byte, []byte) {
	t.Helper()
	raw, _ := json.Marshal(idx)
	return raw, []byte(hex.EncodeToString(ed25519.Sign(priv, raw)))
}

func TestParseIndexNeedsAPinnedSignature(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	raw, sig := signedIndex(t, priv, Index{Format: 1, Version: "0.2.0"})

	if _, err := ParseIndex(raw, sig, []ed25519.PublicKey{pub}); err != nil {
		t.Fatalf("a signed index must pass: %v", err)
	}
	if _, err := ParseIndex(raw, sig, []ed25519.PublicKey{other}); err == nil {
		t.Fatal("an index signed by an unknown key must be refused")
	}
	tampered := []byte(strings.Replace(string(raw), "0.2.0", "9.9.9", 1))
	if _, err := ParseIndex(tampered, sig, []ed25519.PublicKey{pub}); err == nil {
		t.Fatal("a changed index must be refused")
	}
	if _, err := ParseIndex(raw, []byte("nothex"), []ed25519.PublicKey{pub}); err == nil {
		t.Fatal("a malformed signature must be refused")
	}
}

type fixture struct {
	dir     string
	exe     string
	u       *Updater
	exitCh  chan int
	mkIndex func(version string, bin []byte)
	srv     *httptest.Server
}

func newFixture(t *testing.T, current string) *fixture {
	t.Helper()
	old := Version
	Version = current
	t.Cleanup(func() { Version = old })

	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	exe := filepath.Join(dir, "node")
	if err := os.WriteFile(exe, []byte("old binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteGuard(GuardPath(exe)); err != nil {
		t.Fatal(err)
	}

	f := &fixture{dir: dir, exe: exe, exitCh: make(chan int, 4)}
	var index, sig, bin []byte
	mux := http.NewServeMux()
	mux.HandleFunc("/node-update.json", func(w http.ResponseWriter, r *http.Request) { w.Write(index) })
	mux.HandleFunc("/node-update.json.sig", func(w http.ResponseWriter, r *http.Request) { w.Write(sig) })
	mux.HandleFunc("/bin", func(w http.ResponseWriter, r *http.Request) { w.Write(bin) })
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	f.mkIndex = func(version string, body []byte) {
		bin = body
		sum := sha256.Sum256(body)
		index, sig = signedIndex(t, priv, Index{Format: 1, Version: version, Assets: map[string]Asset{
			PlatformKey(): {URL: f.srv.URL + "/bin", SHA256: hex.EncodeToString(sum[:])},
		}})
	}
	f.u = New(Config{
		IndexURL: f.srv.URL + "/node-update.json",
		Exe:      exe,
		Keys:     []ed25519.PublicKey{pub},
		SkipRun:  true,
		Exit:     func(code int) { f.exitCh <- code },
		Logf:     t.Logf,
	})
	return f
}

func TestCheckOnceStagesAKeptRollbackCopy(t *testing.T) {
	f := newFixture(t, "0.1.0")
	f.mkIndex("0.2.0", []byte("new binary"))

	applied, err := f.u.CheckOnce(context.Background())
	if err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	if got, _ := os.ReadFile(f.exe); string(got) != "new binary" {
		t.Fatalf("binary = %q", got)
	}
	if got, _ := os.ReadFile(PrevPath(f.exe)); string(got) != "old binary" {
		t.Fatalf("previous = %q", got)
	}
	st := ReadState(StatePath(f.exe))
	if st.Phase != PhaseProbation || st.Version != "0.2.0" || st.PrevVersion != "0.1.0" || st.Deadline == 0 {
		t.Fatalf("state = %+v", st)
	}
	select {
	case code := <-f.exitCh:
		if code != exitForRestart {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the node must restart into the new binary")
	}

	again, err := f.u.CheckOnce(context.Background())
	if again || err != nil {
		t.Fatalf("no second update while one is on probation: %v %v", again, err)
	}
}

func TestCheckOnceIgnoresOlderTamperedAndBadReleases(t *testing.T) {
	f := newFixture(t, "0.3.0")
	f.mkIndex("0.2.0", []byte("older"))
	if applied, err := f.u.CheckOnce(context.Background()); applied || err != nil {
		t.Fatalf("an older release must be ignored: %v %v", applied, err)
	}

	f.mkIndex("0.4.0", []byte("payload"))
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	f.u.cfg.Keys = []ed25519.PublicKey{other}
	if applied, err := f.u.CheckOnce(context.Background()); applied || err == nil {
		t.Fatalf("an index signed by another key must fail: %v %v", applied, err)
	}
	if got, _ := os.ReadFile(f.exe); string(got) != "old binary" {
		t.Fatal("the binary must be untouched")
	}
}

func TestCheckOnceRefusesAWrongHash(t *testing.T) {
	f := newFixture(t, "0.1.0")
	f.mkIndex("0.2.0", []byte("real"))
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f.u.cfg.Keys = []ed25519.PublicKey{pub}
	raw, _ := json.Marshal(Index{Format: 1, Version: "0.2.0", Assets: map[string]Asset{PlatformKey(): {URL: f.srv.URL + "/bin", SHA256: strings.Repeat("0", 64)}}})
	idxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sig") {
			w.Write([]byte(hex.EncodeToString(ed25519.Sign(priv, raw))))
			return
		}
		w.Write(raw)
	}))
	defer idxSrv.Close()
	f.u.cfg.IndexURL = idxSrv.URL + "/node-update.json"

	if applied, err := f.u.CheckOnce(context.Background()); applied || err == nil {
		t.Fatalf("a binary that does not match the signed hash must be refused: %v %v", applied, err)
	}
	if got, _ := os.ReadFile(f.exe); string(got) != "old binary" {
		t.Fatal("the binary must be untouched")
	}
}

func TestProbationIsConfirmedWhenHealthy(t *testing.T) {
	f := newFixture(t, "0.2.0")
	State{Phase: PhaseProbation, Version: "0.2.0", PrevVersion: "0.1.0", Deadline: time.Now().Add(time.Hour).Unix()}.Write(StatePath(f.exe))
	f.u.cfg.Healthy = func() bool { return true }
	f.u.started = time.Now().Add(-time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	go f.u.watchProbation(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ReadState(StatePath(f.exe)).Phase == "" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("a healthy new version must end its probation")
}

func TestProbationEndsTheProcessWhenItNeverGetsHealthy(t *testing.T) {
	f := newFixture(t, "0.2.0")
	State{Phase: PhaseProbation, Version: "0.2.0", PrevVersion: "0.1.0", Deadline: time.Now().Add(-time.Minute).Unix()}.Write(StatePath(f.exe))
	f.u.cfg.Healthy = func() bool { return false }

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	go f.u.watchProbation(ctx)
	select {
	case code := <-f.exitCh:
		if code != 1 {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("an unhealthy version past its deadline must exit so that the guard rolls it back")
	}
}

func runGuard(t *testing.T, exe string) string {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	out, err := exec.Command(sh, GuardPath(exe), exe).CombinedOutput()
	if err != nil {
		t.Fatalf("guard failed: %v\n%s", err, out)
	}
	return string(out)
}

func TestGuardRollsBackAfterRepeatedStartsAndKeepsTheOldBinary(t *testing.T) {
	f := newFixture(t, "0.1.0")
	f.mkIndex("0.2.0", []byte("broken new"))
	if _, err := f.u.CheckOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		runGuard(t, f.exe)
		if got, _ := os.ReadFile(f.exe); string(got) != "broken new" {
			t.Fatalf("start %d: the new binary must still get its chances", i)
		}
	}
	out := runGuard(t, f.exe)
	if !strings.Contains(out, "rolled back") {
		t.Fatalf("guard output: %q", out)
	}
	if got, _ := os.ReadFile(f.exe); string(got) != "old binary" {
		t.Fatalf("binary after rollback = %q", got)
	}
	st := ReadState(StatePath(f.exe))
	if st.Phase != "" || st.BadVersion != "0.2.0" || st.BadUntil <= time.Now().Unix() {
		t.Fatalf("state after rollback = %+v", st)
	}

	f.u.cfg.Healthy = func() bool { return true }
	if applied, _ := f.u.CheckOnce(context.Background()); applied {
		t.Fatal("a version that was just rolled back must not be retried at once")
	}
}

func TestGuardRollsBackPastTheDeadlineAndIsQuietOtherwise(t *testing.T) {
	f := newFixture(t, "0.1.0")
	if out := runGuard(t, f.exe); strings.TrimSpace(out) != "" {
		t.Fatalf("no update in progress: %q", out)
	}

	os.WriteFile(f.exe, []byte("slow new"), 0o755)
	os.WriteFile(PrevPath(f.exe), []byte("old binary"), 0o755)
	State{Phase: PhaseProbation, Version: "0.2.0", PrevVersion: "0.1.0", Deadline: time.Now().Add(-time.Minute).Unix()}.Write(StatePath(f.exe))
	runGuard(t, f.exe)
	if got, _ := os.ReadFile(f.exe); string(got) != "old binary" {
		t.Fatalf("binary = %q", got)
	}
}
