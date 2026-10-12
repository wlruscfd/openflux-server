package mailru

import (
	"strings"
	"testing"
	"time"
)

func TestUserAgentsFollowTheReleaseCycle(t *testing.T) {
	today := mailruUserAgents(time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC))
	later := mailruUserAgents(time.Date(2026, 12, 12, 0, 0, 0, 0, time.UTC))
	if len(today) < 4 {
		t.Fatalf("want several candidates, got %d", len(today))
	}
	if !strings.Contains(today[0], "Firefox/155.0") {
		t.Fatalf("first candidate on 2026-10-12 = %q", today[0])
	}
	if today[0] == later[0] {
		t.Fatal("candidates must move forward with the calendar")
	}
	for _, ua := range today {
		if !strings.HasPrefix(ua, "Mozilla/5.0 ") {
			t.Fatalf("not a browser UA: %q", ua)
		}
	}
	tr := &MailruDocsTransport{}
	first := tr.userAgent()
	tr.uaIdx.Add(1)
	if tr.userAgent() == first {
		t.Fatal("a 409 must move to another UA")
	}
}
