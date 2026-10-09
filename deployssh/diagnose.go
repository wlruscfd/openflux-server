package deployssh

import (
	"errors"
	"fmt"
	"strings"
)

const diagnoseScript = `
echo "== units"
for u in openflux-controlplane openflux-nodeagent openflux-web nginx postgresql; do
  printf '%s: %s\n' "$u" "$(systemctl is-active "$u" 2>&1 | head -n1)"
done
echo "== memory"
free -m
echo "== disk"
df -h / | tail -n 1
echo "== nodeagent"
journalctl -u openflux-nodeagent -n 150 --no-pager 2>&1
echo "== controlplane"
journalctl -u openflux-controlplane -n 40 --no-pager 2>&1
echo "== listening"
ss -tln 2>/dev/null | head -n 15
echo "== oom"
dmesg 2>/dev/null | grep -iE 'out of memory|killed process' | tail -n 3
echo "== iptables"
iptables -S OUTPUT 2>&1 | grep -E 'RST|port-unreachable'
echo "== end"
`

const verdictPrefix = "VERDICT "

func Diagnose(target SSHTarget, cb Callback) error {
	if cb == nil {
		cb = noopCallback{}
	}
	if strings.TrimSpace(target.Host) == "" {
		return errors.New("the server address is empty")
	}
	if target.Port == 0 {
		target.Port = 22
	}
	if target.Username == "" {
		target.Username = "root"
	}

	r := &remote{target: target, cb: cb}
	if err := r.connect(); err != nil {
		return err
	}
	defer r.close()
	if err := r.detectPrivileges(); err != nil {
		return err
	}

	stdout, stderr, err := r.exec(r.sudo + "bash -c " + shellQuote(diagnoseScript))
	if err != nil {
		return fmt.Errorf("run the diagnostics: %w%s", err, detail(stderr))
	}
	for _, line := range strings.Split(strings.TrimRight(stdout, "\n"), "\n") {
		cb.OnLog(line)
	}
	for _, v := range Analyze(stdout) {
		cb.OnLog(verdictPrefix + v)
	}
	return nil
}

func section(report, name string) string {
	marker := "== " + name + "\n"
	i := strings.Index(report, marker)
	if i < 0 {
		return ""
	}
	rest := report[i+len(marker):]
	if strings.HasPrefix(rest, "== ") {
		return ""
	}
	if j := strings.Index(rest, "\n== "); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

func unitState(units, name string) string {
	for _, line := range strings.Split(units, "\n") {
		if state, ok := strings.CutPrefix(line, name+": "); ok {
			return strings.TrimSpace(state)
		}
	}
	return ""
}

func Analyze(report string) []string {
	var verdicts []string
	add := func(format string, args ...interface{}) {
		verdicts = append(verdicts, fmt.Sprintf(format, args...))
	}

	units := section(report, "units")
	node := section(report, "nodeagent")
	control := section(report, "controlplane")

	if state := unitState(units, "openflux-controlplane"); state != "" && state != "active" {
		add("CONTROLPLANE_DOWN %s", state)
	}
	if strings.TrimSpace(section(report, "oom")) != "" {
		add("OOM")
	}

	nodeState := unitState(units, "openflux-nodeagent")
	if nodeState == "" || nodeState == "inactive" && strings.Contains(node, "No entries") {
		add("NODE_NOT_INSTALLED")
		return verdicts
	}
	if nodeState != "active" {
		add("NODE_DOWN %s", nodeState)
		return verdicts
	}

	if strings.TrimSpace(section(report, "iptables")) == "" {
		add("NO_FIREWALL_RULES")
	}

	last := func(needle string) int { return strings.LastIndex(node, needle) }
	worker := last("worker started")
	connected := last("transport connected")
	captcha := last("asked for a captcha")
	applied := last("applying the uploaded cookie jar")
	unusable := last("cannot use it")

	switch {
	case worker < 0 && connected < 0:
		if strings.Contains(node, "concurrent-key ceiling") {
			add("KEY_LIMIT")
		} else {
			add("NO_KEYS")
		}
	case unusable >= 0 && unusable > applied:
		add("COOKIES_UNUSABLE")
	case captcha >= 0 && applied < 0:
		add("COOKIES_NOT_RECEIVED")
	case captcha >= 0 && captcha > applied:
		add("COOKIES_REJECTED")
	case connected >= 0 && connected > captcha:
		add("NODE_CONNECTED")
	}

	failures := failureReasons(node)
	if len(failures) > 0 {
		add("FAILURES %s", strings.Join(failures, "; "))
	}
	if worker >= 0 && connected < 0 && captcha < 0 && len(failures) == 0 {
		add("NO_PROVIDER_ANSWER")
	}
	if strings.Contains(control, "panic") || strings.Contains(control, "fatal") {
		add("CONTROLPLANE_ERRORS")
	}
	return verdicts
}

func failureReasons(node string) []string {
	seen := map[string]bool{}
	var out []string
	lines := strings.Split(node, "\n")
	for i := len(lines) - 1; i >= 0 && len(out) < 3; i-- {
		_, rest, ok := strings.Cut(lines[i], "failed (")
		if !ok || !strings.Contains(lines[i], "attempt ") {
			continue
		}
		reason, _, _ := strings.Cut(rest, ")")
		code, _, _ := strings.Cut(reason, ":")
		if seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, strings.TrimSpace(reason))
	}
	return out
}
