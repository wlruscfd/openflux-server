package nodeagent

import (
	"testing"

	"universal-bypass-tool/transport"
)

func TestCookieFingerprintChangesWithTheJar(t *testing.T) {
	a := cookieFingerprint("a=1; b=2")
	if a == "" {
		t.Fatal("fingerprint must not be empty")
	}
	if a != cookieFingerprint("a=1; b=2") {
		t.Fatal("same jar must fingerprint identically, otherwise the node re-applies cookies every tick")
	}
	if a == cookieFingerprint("a=1; b=3") {
		t.Fatal("a changed jar must fingerprint differently, otherwise an updated jar is ignored")
	}
}

type recordingTransport struct {
	cookies  string
	reforced int
}

func (r *recordingTransport) Start() error                          { return nil }
func (r *recordingTransport) Stop() error                           { return nil }
func (r *recordingTransport) Send([]byte) error                     { return nil }
func (r *recordingTransport) Receive(func([]byte))                  {}
func (r *recordingTransport) IsConnected() bool                     { return true }
func (r *recordingTransport) Stats() transport.TransportStats       { return transport.TransportStats{} }
func (r *recordingTransport) SetEventCallback(func(string, string)) {}
func (r *recordingTransport) ProvideCookies(c string)               { r.cookies = c }
func (r *recordingTransport) ForceReconnect()                       { r.reforced++ }

var _ transport.CookieProvider = (*recordingTransport)(nil)

func TestApplyCookiesSkipsUnchangedAndUnsupported(t *testing.T) {
	tr := &recordingTransport{}
	w := &worker{trans: tr, cookieHash: cookieFingerprint("a=1")}
	o := &Orchestrator{workers: map[string]*worker{"k1": w}}

	o.applyCookiesWith(map[string]RemoteKeyCookie{
		"k1": {KeyID: "k1", Transport: "yandex", Cookies: "a=1"},
	})
	if tr.cookies != "" {
		t.Fatal("an unchanged jar must not be re-applied")
	}
	if tr.reforced != 0 {
		t.Fatal("an unchanged jar must not force a reconnect")
	}

	o.applyCookiesWith(map[string]RemoteKeyCookie{
		"k1": {KeyID: "k1", Transport: "yandex", Cookies: "a=2"},
	})
	if tr.cookies != "a=2" {
		t.Fatalf("new jar not applied, got %q", tr.cookies)
	}
	if tr.reforced != 1 {
		t.Fatalf("a new jar must drop the session so it is presented, forced %d times", tr.reforced)
	}
}
