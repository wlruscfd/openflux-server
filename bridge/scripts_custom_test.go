package mobile

import (
	"fmt"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/transport/script"
)

type customRepo struct {
	srv     *httptest.Server
	priv    ed25519.PrivateKey
	pubHex  string
	id      string
	mu      sync.Mutex
	version string
	wire    int
	pkg     []byte
	sha     string
}

func newCustomRepo(t *testing.T, id string) *customRepo {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	c := &customRepo{priv: priv, pubHex: hex.EncodeToString(pub), id: id}
	mux := http.NewServeMux()
	mux.HandleFunc("/update.json", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		json.NewEncoder(w).Encode(script.UpdateIndex{Format: 1, ID: c.id, Channels: map[string]script.IndexChannel{
			"stable": {Version: c.version, Wire: c.wire, URL: "pkg.flux", SHA256: c.sha},
		}})
	})
	mux.HandleFunc("/pkg.flux", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		w.Write(c.pkg)
	})
	mux.HandleFunc("/empty-catalog.json", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(catalogFile{Format: 1})
	})
	c.srv = httptest.NewServer(mux)
	t.Cleanup(c.srv.Close)
	return c
}

func (c *customRepo) publish(t *testing.T, version string, wire int, signer ed25519.PrivateKey) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "p.flux")
	m := script.PackageManifest{ID: c.id, Name: "Mine", Version: version, Author: "me", Description: "a custom transport", Wire: wire}
	if err := script.WritePackage(path, m, []byte("var Transport = {};"), nil, signer, true); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	sum := sha256.Sum256(data)
	c.mu.Lock()
	c.version, c.wire, c.pkg, c.sha = version, wire, data, hex.EncodeToString(sum[:])
	c.mu.Unlock()
}

func result(t *testing.T, raw string) map[string]string {
	t.Helper()
	var any map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &any); err != nil {
		t.Fatalf("not an object: %s", raw)
	}
	m := make(map[string]string, len(any))
	for k, v := range any {
		m[k] = fmt.Sprint(v)
	}
	return m
}

func TestCustomTransportInstallsUpdatesUnderItsOwnKeyAndCanBeRemoved(t *testing.T) {
	c := newCustomRepo(t, "mine")
	c.publish(t, "1.0.0", 0, c.priv)
	dir := t.TempDir()

	added := result(t, TransportsAddCustom(dir, c.srv.URL+"/update.json", c.pubHex, "stable"))
	if added["status"] != script.UpdateInstalled || added["id"] != "mine" || added["version"] != "1.0.0" {
		t.Fatalf("add: %v", added)
	}
	var list []installedTransport
	json.Unmarshal([]byte(TransportsList(dir)), &list)
	if len(list) != 1 || !list[0].Custom || list[0].Pubkey != c.pubHex || list[0].Name != "Mine" || list[0].Description != "a custom transport" {
		t.Fatalf("list: %+v", list)
	}

	c.publish(t, "1.1.0", 0, c.priv)
	got := updates(t, TransportsUpdate(dir, c.srv.URL+"/empty-catalog.json", "stable"))
	if len(got) != 1 || got[0].ID != "mine" || !got[0].Custom || got[0].Status != script.UpdateInstalled || got[0].To != "1.1.0" {
		t.Fatalf("update: %+v", got)
	}
	if rb := TransportsRollback(dir, "mine"); result(t, rb)["status"] != script.UpdateInstalled {
		t.Fatalf("rollback must verify under the custom key: %s", rb)
	}
	if v, _ := installedVersion(dir, "mine"); v != "1.0.0" {
		t.Fatalf("after rollback %s", v)
	}

	if r := result(t, TransportsRemove(dir, "mine")); r["status"] != "removed" {
		t.Fatalf("remove: %v", r)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*")); len(files) != 0 {
		t.Fatalf("left behind: %v", files)
	}
}

func TestCustomTransportSignedByAnotherKeyIsRefused(t *testing.T) {
	c := newCustomRepo(t, "mine")
	_, stranger, _ := ed25519.GenerateKey(rand.Reader)
	c.publish(t, "1.0.0", 0, stranger)
	dir := t.TempDir()
	r := result(t, TransportsAddCustom(dir, c.srv.URL+"/update.json", c.pubHex, "stable"))
	if r["status"] != "error" || r["code"] != script.CodeBadSignature {
		t.Fatalf("got %v", r)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*")); len(files) != 0 {
		t.Fatalf("a refused package must leave nothing: %v", files)
	}
	for _, bad := range []string{"", "zz", "abcd"} {
		if r := result(t, TransportsAddCustom(dir, c.srv.URL+"/update.json", bad, "stable")); r["code"] != "bad_pubkey" {
			t.Fatalf("key %q: %v", bad, r)
		}
	}
	if r := result(t, TransportsAddCustom(dir, "http://example.org/update.json", c.pubHex, "stable")); r["code"] != script.CodeInsecureURL {
		t.Fatalf("plain http must be refused: %v", r)
	}
}

func TestDirectPackageInstallsWithoutAnUpdateSource(t *testing.T) {
	c := newCustomRepo(t, "direct")
	c.publish(t, "2.0.0", 0, c.priv)
	dir := t.TempDir()
	r := result(t, TransportsAddCustom(dir, c.srv.URL+"/pkg.flux", c.pubHex, "stable"))
	if r["status"] != script.UpdateInstalled || r["id"] != "direct" || r["version"] != "2.0.0" {
		t.Fatalf("add: %v", r)
	}
	if v, ok := installedVersion(dir, "direct"); !ok || v != "2.0.0" {
		t.Fatalf("installed %q %v", v, ok)
	}
	if _, ok := readSource(dir, "direct"); !ok {
		t.Fatal("the custom source must be remembered")
	}
}

func TestCustomPackageOverridesTheCatalogEntryOfTheSameID(t *testing.T) {
	f := newRepoFixture(t)
	f.publish(t, "1.0.0")
	c := newCustomRepo(t, "mailru")
	c.publish(t, "5.0.0", 0, c.priv)
	dir := t.TempDir()
	if r := result(t, TransportsAddCustom(dir, c.srv.URL+"/update.json", c.pubHex, "stable")); r["status"] != script.UpdateInstalled {
		t.Fatalf("add: %v", r)
	}
	got := updates(t, TransportsUpdate(dir, f.srv.URL+"/catalog.json", "stable"))
	if len(got) != 1 || !got[0].Custom {
		t.Fatalf("the catalog entry must be shadowed by the custom source: %+v", got)
	}
}

func TestCheckReportsWithoutInstalling(t *testing.T) {
	f := newRepoFixture(t)
	f.publish(t, "1.0.0")
	dir := t.TempDir()
	got := updates(t, TransportsCheck(dir, f.srv.URL+"/catalog.json", "stable"))
	if len(got) != 1 || got[0].Status != script.UpdateAvailable || got[0].To != "1.0.0" {
		t.Fatalf("check: %+v", got)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*")); len(files) != 0 {
		t.Fatalf("a check must not write: %v", files)
	}
}

func TestExplicitUpdateOffersARolledBackVersionAgainAndTakesAWireBreakOnRequest(t *testing.T) {
	f := newRepoFixture(t)
	f.publish(t, "1.0.0")
	TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable")
	f.publish(t, "1.1.0")
	TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable")
	TransportsRollback(f.dir, "mailru")
	if skipped := updates(t, TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable")); skipped[0].Status != "skipped" {
		t.Fatalf("automatic update must skip the rolled back version: %+v", skipped)
	}
	one := result(t, TransportsUpdateOne(f.dir, "mailru", f.srv.URL+"/catalog.json", "stable", false))
	if one["status"] != script.UpdateInstalled || one["to"] != "1.1.0" {
		t.Fatalf("explicit update: %v", one)
	}
	if readBad(f.dir, "mailru") != "" {
		t.Fatal("an explicit update clears the rollback mark")
	}

	c := newCustomRepo(t, "wired")
	c.publish(t, "1.0.0", 0, c.priv)
	dir := t.TempDir()
	TransportsAddCustom(dir, c.srv.URL+"/update.json", c.pubHex, "stable")
	c.publish(t, "2.0.0", 2, c.priv)
	blocked := result(t, TransportsUpdateOne(dir, "wired", c.srv.URL+"/empty-catalog.json", "stable", false))
	if blocked["status"] != script.UpdateBlocked {
		t.Fatalf("a wire break must wait for consent: %v", blocked)
	}
	allowed := result(t, TransportsUpdateOne(dir, "wired", c.srv.URL+"/empty-catalog.json", "stable", true))
	if allowed["status"] != script.UpdateInstalled {
		t.Fatalf("a wire break goes through when asked for: %v", allowed)
	}
}

func TestRemoveOnlyTouchesCustomTransportsAndIDsStaySafe(t *testing.T) {
	f := newRepoFixture(t)
	f.publish(t, "1.0.0")
	TransportsUpdate(f.dir, f.srv.URL+"/catalog.json", "stable")
	if r := result(t, TransportsRemove(f.dir, "mailru")); r["code"] != "not_custom" {
		t.Fatalf("a catalog transport is not removed: %v", r)
	}
	if _, ok := installedVersion(f.dir, "mailru"); !ok {
		t.Fatal("the catalog transport must stay")
	}
	for _, id := range []string{"", "../x", "a.b", "a/b"} {
		if r := result(t, TransportsRemove(f.dir, id)); r["code"] != "usage" {
			t.Fatalf("id %q: %v", id, r)
		}
	}
}

func TestScriptTransportNeedsAnInstalledPackage(t *testing.T) {
	if _, err := buildTransport(Config{Mode: "manual", Transport: "script", ScriptID: "nothing", ScriptsDir: t.TempDir()}, transport.DefaultConfig()); err == nil {
		t.Fatal("a script that is not installed must fail")
	}
	dir := t.TempDir()
	TransportsSetEnabled(dir, "off", false)
	if _, err := buildTransport(Config{Mode: "manual", Transport: "script", ScriptID: "off", ScriptsDir: dir}, transport.DefaultConfig()); err == nil {
		t.Fatal("a switched off script must not run")
	}
}
