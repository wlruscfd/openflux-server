package yandex

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestCursorMessageCarriesRealLengthAndRoundTrips(t *testing.T) {
	raw := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	msg := cursorMessage(raw)
	if !strings.Contains(msg, `"cursor":"36;`) {
		t.Fatalf("the prefix must be the packet length: %s", msg)
	}
	got := ExtractBase64(msg)
	if want := base64.StdEncoding.EncodeToString(raw); got != want {
		t.Fatalf("round trip lost the payload: %q != %q", got, want)
	}
}

func TestExtractBase64AcceptsLegacyPrefixAndKeepaliveButNotForeignCursors(t *testing.T) {
	legacy := `42["message",{"type":"cursor","cursor":"18;AAAA"}]`
	if ExtractBase64(legacy) != "AAAA" {
		t.Fatal("the fixed 18 prefix of older peers must still be accepted")
	}
	keepalive := `42["message",{"type":"cursor","cursor":"18;---KA---"}]`
	if ExtractBase64(keepalive) == "" {
		t.Fatal("a keepalive must be accepted")
	}
	foreign := `42["message",{"type":"cursor","cursor":"35;QUJD"}]`
	if ExtractBase64(foreign) != "" {
		t.Fatal("another participant's cursor must not be taken for tunnel data")
	}
}
