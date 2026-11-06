package mobile

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport/script"
)

type repoFixture struct {
	srv     *httptest.Server
	priv    ed25519.PrivateKey
	dir     string
	mu      sync.Mutex
	version string
	pkg     []byte
	sha     string
}

func newRepoFixture(t *testing.T) *repoFixture {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	old := projectKeyHex
	projectKeyHex = func() string { return hex.EncodeToString(pub) }
	t.Cleanup(func() { projectKeyHex = old })

	f := &repoFixture{priv: priv, dir: t.TempDir()}
	mux := http.NewServeMux()
	mux.HandleFunc("/catalog.json", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(catalogFile{Format: 1, Transports: []catalogEntry{{ID: "mailru", Update: f.srv.URL + "/mailru/update.json"}}})
	})
	mux.HandleFunc("/mailru/update.json", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		json.NewEncoder(w).Encode(script.UpdateIndex{Format: 1, ID: "mailru", Channels: map[string]script.IndexChannel{
			"stable": {Version: f.version, URL: "pkg.flux", SHA256: f.sha},
		}})
	})
	mux.HandleFunc("/mailru/pkg.flux", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Write(f.pkg)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *repoFixture) publish(t *testing.T, version string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "p.flux")
	m := script.PackageManifest{ID: "mailru", Name: "Mail.ru", Version: version, Author: "test"}
	if err := script.WritePackage(path, m, []byte("var Transport = {};"), nil, f.priv, true); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	sum := sha256.Sum256(data)
	f.mu.Lock()
	f.version, f.pkg, f.sha = version, data, hex.EncodeToString(sum[:])
	f.mu.Unlock()
}

func updates(t *testing.T, raw string) []TransportUpdate {
	t.Helper()
	var out []TransportUpdate
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("not a result list: %s", raw)
	}
	return out
}

func TestTransportsUpdateInstallsUpdatesAndRollsBack(t *testing.T) {
	f := newRepoFixture(t)
	f.publish(t, "1.0.0")

	got := updates(t, TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable"))
	if len(got) != 1 || got[0].Status != script.UpdateInstalled || got[0].To != "1.0.0" {
		t.Fatalf("first install: %+v", got)
	}
	if v, ok := installedVersion(f.dir, "mailru"); !ok || v != "1.0.0" {
		t.Fatalf("installed = %q %v", v, ok)
	}

	again := updates(t, TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable"))
	if again[0].Status != script.UpdateUpToDate {
		t.Fatalf("second run: %+v", again)
	}

	f.publish(t, "1.1.0")
	up := updates(t, TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable"))
	if up[0].Status != script.UpdateInstalled || up[0].From != "1.0.0" || up[0].To != "1.1.0" {
		t.Fatalf("update: %+v", up)
	}

	var list []installedTransport
	json.Unmarshal([]byte(TransportsInstalled(f.dir)), &list)
	if len(list) != 1 || list[0].Version != "1.1.0" || !list[0].HasPrev {
		t.Fatalf("installed list: %+v", list)
	}

	if rb := TransportsRollback(f.dir, "mailru"); !strings.Contains(rb, `"installed"`) {
		t.Fatalf("rollback: %s", rb)
	}
	if v, _ := installedVersion(f.dir, "mailru"); v != "1.0.0" {
		t.Fatalf("after rollback = %s", v)
	}
	skipped := updates(t, TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable"))
	if skipped[0].Status != "skipped" {
		t.Fatalf("a rolled-back version must not be installed again: %+v", skipped)
	}

	f.publish(t, "1.2.0")
	next := updates(t, TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable"))
	if next[0].Status != script.UpdateInstalled || next[0].To != "1.2.0" {
		t.Fatalf("a newer version after a rollback must install: %+v", next)
	}
}

func TestTransportsUpdateRefusesAnotherKeyAndKeepsTheOldPackage(t *testing.T) {
	f := newRepoFixture(t)
	f.publish(t, "1.0.0")
	TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable")

	_, evil, _ := ed25519.GenerateKey(rand.Reader)
	f.priv = evil
	f.publish(t, "9.9.9")
	got := updates(t, TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable"))
	if got[0].Status == script.UpdateInstalled {
		t.Fatalf("a package signed by another key must be refused: %+v", got)
	}
	if v, _ := installedVersion(f.dir, "mailru"); v != "1.0.0" {
		t.Fatalf("the installed package must stay: %s", v)
	}
}

func TestOverlayFailuresRollBackThenDisable(t *testing.T) {
	f := newRepoFixture(t)
	f.publish(t, "1.0.0")
	TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable")
	f.publish(t, "1.1.0")
	TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable")

	recordOverlayFailure(f.dir, "mailru", "1.1.0")
	if v, _ := installedVersion(f.dir, "mailru"); v != "1.1.0" {
		t.Fatalf("one failure must not roll back yet: %s", v)
	}
	recordOverlayFailure(f.dir, "mailru", "1.1.0")
	if v, _ := installedVersion(f.dir, "mailru"); v != "1.0.0" {
		t.Fatalf("two failures must roll back to the previous package: %s", v)
	}

	recordOverlayFailure(f.dir, "mailru", "1.0.0")
	recordOverlayFailure(f.dir, "mailru", "1.0.0")
	if _, err := os.Stat(disabledPath(f.dir, "mailru")); err != nil {
		t.Fatalf("with nothing to roll back to the transport must go back to the built-in one")
	}
	if overlayScript(Config{ScriptsDir: f.dir}, "mailru") != nil {
		t.Fatal("a disabled transport must not be opened")
	}
}

func TestCatalogDecidesWhichTransportsAreInstalled(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	old := projectKeyHex
	projectKeyHex = func() string { return hex.EncodeToString(pub) }
	t.Cleanup(func() { projectKeyHex = old })

	path := filepath.Join(t.TempDir(), "p.flux")
	m := script.PackageManifest{ID: "boards", Name: "Boards", Version: "1.0.0", Author: "test"}
	if err := script.WritePackage(path, m, []byte("var Transport = {};"), nil, priv, true); err != nil {
		t.Fatal(err)
	}
	pkg, _ := os.ReadFile(path)
	sum := sha256.Sum256(pkg)

	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/catalog.json", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(catalogFile{Format: 1, Transports: []catalogEntry{
			{ID: "boards", Update: srv.URL + "/boards/update.json"},
			{ID: "../evil", Update: srv.URL + "/boards/update.json"},
			{ID: "a.b", Update: srv.URL + "/boards/update.json"},
		}})
	})
	mux.HandleFunc("/boards/update.json", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(script.UpdateIndex{Format: 1, ID: "boards", Channels: map[string]script.IndexChannel{
			"stable": {Version: "1.0.0", URL: "pkg.flux", SHA256: hex.EncodeToString(sum[:])},
		}})
	})
	mux.HandleFunc("/boards/pkg.flux", func(w http.ResponseWriter, r *http.Request) { w.Write(pkg) })
	srv = httptest.NewServer(mux)
	defer srv.Close()

	dir := t.TempDir()
	got := updates(t, TransportsUpdate(dir, srv.URL+"/catalog.json", "stable"))
	if len(got) != 1 || got[0].ID != "boards" || got[0].Status != script.UpdateInstalled {
		t.Fatalf("only the safe listed transport must be installed: %+v", got)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(files) != 1 {
		t.Fatalf("unexpected files: %v", files)
	}
}
