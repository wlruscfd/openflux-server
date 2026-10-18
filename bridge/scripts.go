package mobile

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/transport/script"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// Dynamic transports: signed packages fetched from the transports repository replace the built-in implementation of the same transport while they are installed and healthy. The built-in one stays in the app as the fallback.

const (
	defaultCatalogURL = "https://raw.githubusercontent.com/wlruscfd/openflux-transports/main/catalog.json"
	catalogLimit      = 256 << 10
	overlayConnectIn  = 45 * time.Second
	overlayMaxFails   = 2
)

// dynamicIDs are the transports this core can run from a package; the catalog may list more for newer apps.
var dynamicIDs = map[string]bool{"mailru": true, "cupsonline": true}

type catalogEntry struct {
	ID     string `json:"id"`
	Name   string `json:"name,omitempty"`
	Update string `json:"update"`
}

type catalogFile struct {
	Format     int            `json:"format"`
	Transports []catalogEntry `json:"transports"`
}

type TransportUpdate struct {
	ID      string `json:"id"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Status  string `json:"status"`
	Code    string `json:"code,omitempty"`
	Notes   string `json:"notes,omitempty"`
	WireBrk bool   `json:"wireBreak,omitempty"`
}

type installedTransport struct {
	ID       string `json:"id"`
	Version  string `json:"version"`
	Disabled bool   `json:"disabled,omitempty"`
	HasPrev  bool   `json:"hasPrevious,omitempty"`
}

var projectKeyHex = func() string { return script.OfficialKeys()[0] }

func packagePath(dir, id string) string { return filepath.Join(dir, id+".flux") }

func disabledPath(dir, id string) string { return filepath.Join(dir, id+".disabled") }

func failsPath(dir, id string) string { return filepath.Join(dir, id+".fails") }

func badPath(dir, id string) string { return filepath.Join(dir, id+".bad") }

func readBad(dir, id string) string {
	raw, _ := os.ReadFile(badPath(dir, id))
	return strings.TrimSpace(string(raw))
}

func installedVersion(dir, id string) (string, bool) { return versionAt(packagePath(dir, id)) }

func versionAt(path string) (string, bool) {
	pub, err := script.DecodePublicKeyHex(projectKeyHex())
	if err != nil {
		return "", false
	}
	p, err := script.LoadSignedPackage(path, pub)
	if err != nil {
		return "", false
	}
	return p.Manifest.Version, true
}

func readFails(dir, id string) (version string, n int) {
	raw, err := os.ReadFile(failsPath(dir, id))
	if err != nil {
		return "", 0
	}
	fmt.Sscanf(strings.TrimSpace(string(raw)), "%s %d", &version, &n)
	return version, n
}

// TransportsUpdate checks the catalog and installs every newer package that is signed by the project key and keeps the wire format. It returns a JSON array of results; a failure of one package does not stop the others.
func TransportsUpdate(dir, catalogURL, channel string) string {
	if dir == "" {
		return `{"error":"no directory"}`
	}
	if catalogURL == "" {
		catalogURL = defaultCatalogURL
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return jsonErr(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	raw, err := script.HTTPFetcher(ctx, catalogURL, catalogLimit)
	if err != nil {
		return jsonErr(fmt.Errorf("catalog: %w", err))
	}
	var cat catalogFile
	if json.Unmarshal(raw, &cat) != nil || cat.Format != 1 {
		return jsonErr(fmt.Errorf("catalog: unsupported"))
	}

	out := []TransportUpdate{}
	for _, e := range cat.Transports {
		if !dynamicIDs[e.ID] || !script.SecureURL(e.Update) {
			continue
		}
		cur, have := installedVersion(dir, e.ID)
		if !have {
			cur = "0.0.0"
		}
		inst := script.Installed{ID: e.ID, Version: cur, PubkeyHex: projectKeyHex(), Update: []string{e.Update}}
		check := script.CheckUpdate(ctx, inst, channel, script.HTTPFetcher)
		rep := check
		if bad := readBad(dir, e.ID); check.Status == script.UpdateAvailable && bad != "" && script.CompareVersions(check.Latest, bad) <= 0 {
			rep.Status, rep.Code = "skipped", "rolled_back_version"
		} else {
			rep = script.ApplyUpdate(ctx, inst, channel, dir, false, script.HTTPFetcher)
		}
		res := TransportUpdate{ID: e.ID, From: rep.Current, To: rep.Latest, Status: rep.Status, Code: rep.Code, Notes: rep.Notes, WireBrk: rep.WireBreak}
		if rep.Status == script.UpdateInstalled {
			os.Remove(disabledPath(dir, e.ID))
			os.Remove(failsPath(dir, e.ID))
		}
		out = append(out, res)
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// TransportsInstalled lists the packages in dir that verify under the project key.
func TransportsInstalled(dir string) string {
	out := []installedTransport{}
	files, _ := filepath.Glob(filepath.Join(dir, "*.flux"))
	for _, f := range files {
		id := strings.TrimSuffix(filepath.Base(f), ".flux")
		v, ok := installedVersion(dir, id)
		if !ok {
			continue
		}
		_, disabled := os.Stat(disabledPath(dir, id))
		_, prev := os.Stat(f + ".prev")
		out = append(out, installedTransport{ID: id, Version: v, Disabled: disabled == nil, HasPrev: prev == nil})
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// TransportsRollback restores the previous package of one transport; without one the transport goes back to the built-in implementation.
func TransportsRollback(dir, id string) string {
	if dir == "" || id == "" || strings.ContainsAny(id, `/\.`) {
		return `{"status":"error","code":"usage"}`
	}
	if v, ok := installedVersion(dir, id); ok {
		os.WriteFile(badPath(dir, id), []byte(v), 0o644)
	}
	rep := script.RollbackPackage(dir, id+".flux", projectKeyHex())
	if rep.Status != script.UpdateInstalled {
		os.WriteFile(disabledPath(dir, id), []byte("rolled back"), 0o644)
	}
	b, _ := json.Marshal(rep)
	return string(b)
}

// TransportsSetEnabled switches one dynamic transport off (or on) without removing it.
func TransportsSetEnabled(dir, id string, enabled bool) {
	if dir == "" || id == "" || strings.ContainsAny(id, `/\.`) {
		return
	}
	if enabled {
		os.Remove(disabledPath(dir, id))
		os.Remove(failsPath(dir, id))
		return
	}
	os.WriteFile(disabledPath(dir, id), []byte("off"), 0o644)
}

// overlayScript opens the installed package for id when it may be used: not switched off, verified, and loadable.
func overlayScript(cfg Config, id string) *script.ScriptTransport {
	dir := cfg.ScriptsDir
	if dir == "" {
		return nil
	}
	if _, off := os.Stat(disabledPath(dir, id)); off == nil {
		return nil
	}
	pub, err := script.DecodePublicKeyHex(projectKeyHex())
	if err != nil {
		return nil
	}
	url := cfg.DocURL
	st, err := script.New(id, packagePath(dir, id), pub, url, map[string]interface{}{"exit": false}, transport.DefaultConfig())
	if err != nil {
		if !os.IsNotExist(err) {
			utils.Debugf("[SCRIPTS] %s: %v", id, err)
		}
		return nil
	}
	return st
}

// withOverlay builds the transport for id: the installed dynamic package when there is one, with the built-in implementation standing by, otherwise just the built-in one.
func withOverlay(cfg Config, id string, native func() transport.Transport, wrap func(transport.Transport) transport.Transport) transport.Transport {
	st := overlayScript(cfg, id)
	if st == nil {
		return wrap(native())
	}
	version, _ := installedVersion(cfg.ScriptsDir, id)
	utils.Infof("[SCRIPTS] %s %s is used instead of the built-in transport", id, version)
	var fb *transport.FallbackTransport
	fb = transport.NewFallbackTransport(wrap(st), func() transport.Transport { return wrap(native()) }, overlayConnectIn, func(builtinConnected bool) {
		if builtinConnected {
			recordOverlayFailure(cfg.ScriptsDir, id, version)
		}
	})
	return fb
}

// recordOverlayFailure counts a version that did not connect while the built-in one did; after overlayMaxFails the previous package comes back, or the transport reverts to the built-in one.
func recordOverlayFailure(dir, id, version string) {
	v, n := readFails(dir, id)
	if v != version {
		n = 0
	}
	n++
	os.WriteFile(failsPath(dir, id), []byte(fmt.Sprintf("%s %d", version, n)), 0o644)
	utils.Infof("[SCRIPTS] %s %s did not connect while the built-in transport did (%d/%d)", id, version, n, overlayMaxFails)
	if n < overlayMaxFails {
		return
	}
	os.WriteFile(badPath(dir, id), []byte(version), 0o644)
	var rep script.UpdateReport
	if pv, ok := versionAt(packagePath(dir, id) + ".prev"); ok && script.CompareVersions(pv, version) < 0 {
		rep = script.RollbackPackage(dir, id+".flux", projectKeyHex())
	}
	if rep.Status != script.UpdateInstalled {
		os.WriteFile(disabledPath(dir, id), []byte(version), 0o644)
		utils.Infof("[SCRIPTS] %s goes back to the built-in transport", id)
	} else {
		utils.Infof("[SCRIPTS] %s rolled back to the previous version", id)
	}
	os.Remove(failsPath(dir, id))
}

func jsonErr(err error) string {
	b, _ := json.Marshal(map[string]string{"error": err.Error()})
	return string(b)
}
