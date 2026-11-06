package api

import (
	"strings"
	"testing"
)

func TestAvitoCookiesAcceptTheHeaderTheBrowserShows(t *testing.T) {
	got, err := normalizeAvitoCookies("Cookie: sid=abc; _avt=1; sid=def\n")
	if err != nil {
		t.Fatal(err)
	}
	if got != "sid=def; _avt=1" {
		t.Fatalf("got %q", got)
	}
}

func TestAvitoCookiesAcceptNetscapeFiles(t *testing.T) {
	file := "# Netscape HTTP Cookie File\n" +
		".avito.ru\tTRUE\t/\tTRUE\t0\tsid\tabc\n" +
		"#HttpOnly_.avito.ru\tTRUE\t/\tTRUE\t0\tauth\ttok=en\n"
	got, err := normalizeAvitoCookies(file)
	if err != nil {
		t.Fatal(err)
	}
	if got != "sid=abc; auth=tok=en" {
		t.Fatalf("got %q", got)
	}
}

func TestAvitoCookiesAcceptJSONExportsAndSkipOtherSites(t *testing.T) {
	export := `[{"name":"sid","value":"abc","domain":".avito.ru"},{"name":"ym","value":"1","domain":".yandex.ru"},{"name":"x","value":"2"}]`
	got, err := normalizeAvitoCookies(export)
	if err != nil {
		t.Fatal(err)
	}
	if got != "sid=abc; x=2" {
		t.Fatalf("got %q", got)
	}
}

func TestAvitoCookiesRefuseWhatCouldBreakTheHeader(t *testing.T) {
	for _, bad := range []string{"", "   ", "nothing useful", "bad name=1", "a=b\x01c", "[not json", strings.Repeat("a=b; ", 20000)} {
		if got, err := normalizeAvitoCookies(bad); err == nil {
			t.Errorf("%q accepted as %q", bad[:min(len(bad), 20)], got)
		}
	}
}
