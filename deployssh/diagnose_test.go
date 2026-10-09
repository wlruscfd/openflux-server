package deployssh

import (
	"strings"
	"testing"
)

func report(units, node string) string {
	return "== units\n" + units + "== memory\nx\n== nodeagent\n" + node + "\n== controlplane\nok\n== oom\n== iptables\n-A OUTPUT -p tcp -m mark ! --mark 0x2547 -j DROP\n== end\n"
}

const activeUnits = "openflux-controlplane: active\nopenflux-nodeagent: active\nopenflux-web: active\nnginx: active\n"

func hasVerdict(verdicts []string, code string) bool {
	for _, v := range verdicts {
		if v == code || strings.HasPrefix(v, code+" ") {
			return true
		}
	}
	return false
}

func TestAnalyzeCookiesNeverReachedTheNode(t *testing.T) {
	node := "[NODEAGENT] key k1 (yandex): worker started\n[NODEAGENT] key k1 (yandex): the provider asked for a captcha; waiting for a cookie jar pushed from the app\n"
	if v := Analyze(report(activeUnits, node)); !hasVerdict(v, "COOKIES_NOT_RECEIVED") {
		t.Fatalf("got %v", v)
	}
}

func TestAnalyzeCookiesRejectedByTheProvider(t *testing.T) {
	node := "worker started\nthe provider asked for a captcha; waiting\napplying the uploaded cookie jar (900 bytes) and reconnecting\nthe provider asked for a captcha; waiting\n"
	if v := Analyze(report(activeUnits, node)); !hasVerdict(v, "COOKIES_REJECTED") {
		t.Fatalf("got %v", v)
	}
}

func TestAnalyzeConnectedAfterCookies(t *testing.T) {
	node := "worker started\nthe provider asked for a captcha; waiting\napplying the uploaded cookie jar (900 bytes)\ntransport connected\n"
	v := Analyze(report(activeUnits, node))
	if !hasVerdict(v, "NODE_CONNECTED") || hasVerdict(v, "COOKIES_REJECTED") {
		t.Fatalf("got %v", v)
	}
}

func TestAnalyzeNodeDownAndMissing(t *testing.T) {
	if v := Analyze(report("openflux-controlplane: active\nopenflux-nodeagent: failed\n", "")); !hasVerdict(v, "NODE_DOWN") {
		t.Fatalf("got %v", v)
	}
	if v := Analyze(report("openflux-controlplane: active\nopenflux-nodeagent: inactive\n", "-- No entries --")); !hasVerdict(v, "NODE_NOT_INSTALLED") {
		t.Fatalf("got %v", v)
	}
}

func TestAnalyzeNoKeysAndFailures(t *testing.T) {
	if v := Analyze(report(activeUnits, "[NODEAGENT] started\n")); !hasVerdict(v, "NO_KEYS") {
		t.Fatalf("got %v", v)
	}
	node := "worker started\nkey k1 (boards): attempt 3 failed (auth_failed: 403 forbidden), next try in 30s\n"
	v := Analyze(report(activeUnits, node))
	if !hasVerdict(v, "FAILURES") || !strings.Contains(strings.Join(v, " "), "auth_failed") {
		t.Fatalf("got %v", v)
	}
}

func TestAnalyzeMissingFirewallRules(t *testing.T) {
	r := strings.Replace(report(activeUnits, "worker started\ntransport connected\n"), "-A OUTPUT -p tcp -m mark ! --mark 0x2547 -j DROP\n", "", 1)
	if v := Analyze(r); !hasVerdict(v, "NO_FIREWALL_RULES") {
		t.Fatalf("got %v", v)
	}
}
