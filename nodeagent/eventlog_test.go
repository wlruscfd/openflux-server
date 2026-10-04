package nodeagent

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(nil) })
	return &buf
}

func TestKeyEventLoggerThrottlesRepeatedFailuresButNotConnects(t *testing.T) {
	buf := captureLog(t)
	emit := keyEventLogger("k1", "yandex")

	for i := 0; i < 5; i++ {
		emit(transport.EventRetrying, "1|5|fetch_failed|timeout")
	}
	emit(transport.EventRetrying, "2|180|captcha_blocked|captcha page")
	emit(transport.EventCaptchaRequired, "https://docs.yandex.ru/secret-doc-id")
	emit(transport.EventCaptchaRequired, "https://docs.yandex.ru/secret-doc-id")
	emit(transport.EventConnected, "1")
	emit(transport.EventConnected, "2")
	emit(transport.EventConnecting, "")

	out := buf.String()
	if got := strings.Count(out, "fetch_failed"); got != 1 {
		t.Fatalf("a repeated failure reason must be logged once a minute, got %d:\n%s", got, out)
	}
	if !strings.Contains(out, "captcha_blocked") || !strings.Contains(out, "asked for a captcha") {
		t.Fatalf("captcha failures must be visible:\n%s", out)
	}
	if got := strings.Count(out, "transport connected"); got != 2 {
		t.Fatalf("every connect must be logged, got %d", got)
	}
	if strings.Contains(out, "secret-doc-id") {
		t.Fatalf("the document link must not reach the journal:\n%s", out)
	}
	if !strings.Contains(out, "key k1 (yandex)") {
		t.Fatalf("lines must name the key and transport:\n%s", out)
	}
}

func TestKeyEventLoggerIgnoresMalformedRetry(t *testing.T) {
	buf := captureLog(t)
	keyEventLogger("k1", "yandex")(transport.EventRetrying, "garbage")
	if buf.Len() != 0 {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}
