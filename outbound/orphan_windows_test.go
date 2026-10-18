//go:build windows

package outbound

import (
	"os"
	"os/exec"
	"testing"
)

func startSleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("cmd", "/c", "ping", "-n", "300", "127.0.0.1")
	if err := cmd.Start(); err != nil {
		t.Skip("no ping binary:", err)
	}
	return cmd
}

func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return alive(proc)
}
