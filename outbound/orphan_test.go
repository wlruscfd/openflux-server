package outbound

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A node restart never calls Stop on the sidecar the previous run left running. That process keeps
// the ports the new one has to bind, so the new sing-box opens no listener at all and the proxy
// pool silently never comes up. The pid file is what lets the new run find and remove it.
func TestKillOrphanRemovesTheProcessFromAPreviousRun(t *testing.T) {
	dir := t.TempDir()
	s := NewSidecar(SidecarOptions{Binary: "sing-box", WorkDir: dir})
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	cmd := startSleeper(t)
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	if err := os.WriteFile(s.pidFile(), []byte(itoaForTest(cmd.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}

	s.killOrphan()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cmd.ProcessState != nil || !processAlive(cmd.Process.Pid) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the sidecar left over from the previous run is still running")
}

func TestKillOrphanIsSafeWithoutAPidFile(t *testing.T) {
	s := NewSidecar(SidecarOptions{WorkDir: t.TempDir()})
	s.killOrphan()
}

func TestKillOrphanIgnoresAGarbagePidFile(t *testing.T) {
	dir := t.TempDir()
	s := NewSidecar(SidecarOptions{WorkDir: dir})
	for _, junk := range []string{"", "not-a-pid", "1", "-5"} {
		if err := os.WriteFile(filepath.Join(dir, "sing-box.pid"), []byte(junk), 0o600); err != nil {
			t.Fatal(err)
		}
		s.killOrphan()
	}
	if _, err := os.Stat(s.pidFile()); err == nil {
		t.Fatal("a pid file that names nothing should not be left behind")
	}
}

func TestPidFileIsRemovedWhenTheSidecarStops(t *testing.T) {
	dir := t.TempDir()
	s := NewSidecar(SidecarOptions{WorkDir: dir})
	s.writePIDFileLocked(os.Getpid())
	if _, err := os.Stat(s.pidFile()); err != nil {
		t.Fatalf("the pid file was not written: %v", err)
	}
	s.clearPIDFileLocked()
	if _, err := os.Stat(s.pidFile()); err == nil {
		t.Fatal("the pid file outlived the process it named")
	}
}

func itoaForTest(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
