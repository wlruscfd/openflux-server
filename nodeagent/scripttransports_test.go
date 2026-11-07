package nodeagent

import "testing"

func TestManagedTransportSupported(t *testing.T) {
	for _, name := range []string{"yandex", "yandex_multistream", "boards", "mailru", "mts", "telemost", "cupsonline", "avito", "bitrix", "pruffme"} {
		if !managedTransportSupported(name) {
			t.Errorf("%s: managedTransportSupported = false, want true", name)
		}
	}
	for _, name := range []string{"", "bad id!", "with.dot"} {
		if managedTransportSupported(name) {
			t.Errorf("%s: managedTransportSupported = true, want false", name)
		}
	}
	if !managedTransportSupported("vkteams") {
		t.Error("vkteams: an arbitrary catalog id must pass the gate (the package resolves at start)")
	}
}

func TestNewScriptTransportVerifiesAndBuilds(t *testing.T) {
	for _, name := range []string{"bitrix", "pruffme"} {
		tr, err := newScriptTransport(name, "https://example.com/board", "")
		if err != nil {
			t.Fatalf("%s: newScriptTransport: %v", name, err)
		}
		tr.Stop()
	}
	if _, err := newScriptTransport("vkteams", "https://example.com", ""); err == nil {
		t.Fatal("vkteams: expected an error, the node ships no package for it")
	}
}
