package yandex

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"universal-bypass-tool/transport"
)

func TestIsCaptchaURL(t *testing.T) {
	for _, raw := range []string{
		"https://docs.yandex.ru/showcaptcha?retpath=x",
		"https://docs.yandex.ru/showcaptchafast?retpath=x",
		"https://docs.yandex.ru/checkcaptcha?key=x",
		"/showcaptcha?retpath=x",
	} {
		if !isCaptchaURL(raw) {
			t.Errorf("isCaptchaURL(%q) = false", raw)
		}
	}
	for _, raw := range []string{"https://docs.yandex.ru/docs/edit?url=x", "/favicon.ico"} {
		if isCaptchaURL(raw) {
			t.Errorf("isCaptchaURL(%q) = true", raw)
		}
	}
}

func TestCaptchaHashKnownVector(t *testing.T) {
	prefix := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77}
	nonce := []byte{0x00, 0x00, 0x00, 0x01, 0x23, 0x45, 0x67, 0x89}
	sum := captchaHash(prefix, nonce)
	got := hex.EncodeToString(sum[:])
	const want = "5840c7b5904c3abcc3dbf7b121935644fb17929941e200125b69ede49e10a1a6"
	if got != want {
		t.Fatalf("hash = %s, want %s", got, want)
	}
}

func TestSolveCaptchaPoW(t *testing.T) {
	nonce, attempts, _, err := solveCaptchaPoW(context.Background(), "0011223344556677", 12)
	if err != nil {
		t.Fatal(err)
	}
	if attempts == 0 || nonce == "" {
		t.Fatal("PoW did not produce a nonce")
	}
}

func TestParseCurrentCaptchaHTML(t *testing.T) {
	page := `<script>window.__SSR_DATA__={formAction:"/checkcaptcha?key=x",uniqueKey:"u-1",powPrefix:"abcd",powComplexity:"15",timestamp:"123456"}</script>` +
		`<form method="POST" action="/checkcaptcha?key=x" id="checkbox-captcha-form"><input name="rdata"></form>`

	ssr, action, err := parseCaptchaHTML(page, "https://docs.yandex.ru/showcaptcha?retpath=x")
	if err != nil {
		t.Fatal(err)
	}
	if ssr.Legacy {
		t.Fatal("current captcha parsed as legacy")
	}
	if ssr.PowPrefix != "abcd" || ssr.PowComplexity != 15 || ssr.UniqueKey != "u-1" {
		t.Fatalf("unexpected SSR data: %+v", ssr)
	}
	if action != "https://docs.yandex.ru/checkcaptcha?key=x" {
		t.Fatalf("action = %q", action)
	}
}

func TestFetchDocInfoHandlesDirectCaptchaPage(t *testing.T) {
	var mu sync.Mutex
	solved := false

	mux := http.NewServeMux()
	mux.HandleFunc("/doc", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ok := solved
		mu.Unlock()
		if !ok {
			_, _ = w.Write([]byte(`<script>window.__SSR_DATA__={formAction:"/checkcaptcha",uniqueKey:"u",powPrefix:"00",powComplexity:"0",timestamp:"1"}</script><form action="/checkcaptcha" id="checkbox-captcha-form"><input name="pdata"></form>`))
			return
		}
		_, _ = w.Write([]byte(`<script id="client-config">{"officeActionData":{"balancer_url":"https://balancer.example","editor_config":{"document":{"key":"k"},"token":"t"}}}</script>`))
	})
	mux.HandleFunc("/checkcaptcha", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		solved = true
		mu.Unlock()
		http.Redirect(w, r, "/doc", http.StatusFound)
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	tr := NewYandexDocsTransport(server.URL+"/doc", transport.DefaultConfig())
	if _, err := tr.fetchDocInfo(server.URL+"/doc", "user1"); err != nil {
		t.Fatal(err)
	}
}

func TestFetchDocInfoHandlesCurrentShowcaptchaFlow(t *testing.T) {
	var mu sync.Mutex
	solved := false
	var posted url.Values

	mux := http.NewServeMux()
	mux.HandleFunc("/doc", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ok := solved
		mu.Unlock()
		if !ok {
			http.Redirect(w, r, "/showcaptcha?retpath=%2Fdoc", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<script id="client-config">{"officeActionData":{"balancer_url":"https://balancer.example","editor_config":{"document":{"key":"doc-key"},"token":"tok"}}}</script>`))
	})
	mux.HandleFunc("/showcaptcha", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<script>window.__SSR_DATA__={formAction:"/checkcaptcha?key=x",uniqueKey:"u-1",powPrefix:"abcd",powComplexity:"8",timestamp:"123456"}</script><form method="POST" action="/checkcaptcha?key=x" id="checkbox-captcha-form"><input name="rdata"><input name="pdata"></form>`))
	})
	mux.HandleFunc("/checkcaptcha", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
			return
		}
		mu.Lock()
		posted = r.PostForm
		solved = true
		mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "captcha", Value: "ok", Path: "/"})
		http.Redirect(w, r, "/doc", http.StatusFound)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	tr := NewYandexDocsTransport(server.URL+"/doc", transport.DefaultConfig())
	info, err := tr.fetchDocInfo(server.URL+"/doc", "user1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Token != "tok" || info.DocID != "doc-key" {
		t.Fatalf("unexpected info: %+v", info)
	}

	mu.Lock()
	form := posted
	mu.Unlock()
	if form.Get("pdata") == "" || form.Get("rdata") == "" {
		t.Fatalf("missing current captcha fields: %v", form)
	}
	if form.Has("fingerprint") || form.Has("chstate") || form.Has("version") {
		t.Fatalf("legacy fields posted to current endpoint: %v", form)
	}

	raw, err := base64.StdEncoding.DecodeString(form.Get("pdata"))
	if err != nil {
		t.Fatal(err)
	}
	var pdata struct {
		Nonce  string `json:"powNonce"`
		Prefix string `json:"powPrefix"`
	}
	if err := json.Unmarshal(raw, &pdata); err != nil {
		t.Fatal(err)
	}
	if pdata.Nonce == "" || pdata.Prefix != "abcd" {
		t.Fatalf("unexpected pdata: %+v", pdata)
	}
}

func TestLiveNativeCaptcha(t *testing.T) {
	target := os.Getenv("OPENFLUX_LIVE_NATIVE_URL")
	if target == "" {
		t.Skip("set OPENFLUX_LIVE_NATIVE_URL to run a native CAPTCHA test")
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	userAgent := os.Getenv("OPENFLUX_LIVE_NATIVE_UA")
	if userAgent == "" {
		userAgent = browserUserAgent
	}
	if _, err := solveCaptcha(target, jar, userAgent, http.DefaultTransport); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyFingerprintUsesCurrentSchema(t *testing.T) {
	fingerprint := buildLegacyCaptchaFingerprint("00112233", "u-1", 10)
	if fingerprint["version"] != "1.8.2" || fingerprint["uniqueKey"] != "u-1" {
		t.Fatalf("unexpected fingerprint metadata: %+v", fingerprint)
	}
	factors := fingerprint["factors"].(map[string]interface{})
	m10 := factors["m10"].(map[string]interface{})
	if !strings.HasPrefix(m10["value"].(string), "00112233") {
		t.Fatalf("unexpected m10 factor: %+v", m10)
	}
}
