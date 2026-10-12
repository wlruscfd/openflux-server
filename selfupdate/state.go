package selfupdate

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	PhaseProbation = "probation"
)

// State lives in a KEY=VALUE file next to the binary so that the shell guard, which must work when the new binary does not even start, reads and writes the very same file.
type State struct {
	Phase       string
	Version     string
	PrevVersion string
	Starts      int
	Deadline    int64
	BadVersion  string
	BadUntil    int64
}

func StatePath(exe string) string {
	return filepath.Join(filepath.Dir(exe), "update.state")
}

func GuardPath(exe string) string {
	return filepath.Join(filepath.Dir(exe), "openflux-guard.sh")
}

func PrevPath(exe string) string { return exe + ".prev" }

func ReadState(path string) State {
	var s State
	f, err := os.Open(path)
	if err != nil {
		return s
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(strings.TrimSpace(sc.Text()), "=")
		if !ok {
			continue
		}
		switch k {
		case "PHASE":
			s.Phase = v
		case "VERSION":
			s.Version = v
		case "PREV_VERSION":
			s.PrevVersion = v
		case "STARTS":
			s.Starts, _ = strconv.Atoi(v)
		case "DEADLINE":
			s.Deadline, _ = strconv.ParseInt(v, 10, 64)
		case "BAD_VERSION":
			s.BadVersion = v
		case "BAD_UNTIL":
			s.BadUntil, _ = strconv.ParseInt(v, 10, 64)
		}
	}
	return s
}

func (s State) Write(path string) error {
	body := fmt.Sprintf("PHASE=%s\nVERSION=%s\nPREV_VERSION=%s\nSTARTS=%d\nDEADLINE=%d\nBAD_VERSION=%s\nBAD_UNTIL=%d\n",
		s.Phase, s.Version, s.PrevVersion, s.Starts, s.Deadline, s.BadVersion, s.BadUntil)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
