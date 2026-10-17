package mobile

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/transport/script"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// Dynamic transports: signed packages replace the built-in implementation of the same transport while they are installed and healthy; the built-in one stays in the app as the fallback.

const (
	defaultCatalogURL = "https://raw.githubusercontent.com/wlruscfd/openflux-transports/main/catalog.json"
	catalogLimit      = 256 << 10
	packageLimit      = 4 << 20
	overlayConnectIn  = 45 * time.Second
	overlayMaxFails   = 2
	updateTimeout     = 2 * time.Minute
)

var overlayIDs = map[string]bool{"mailru": true, "cupsonline": true}

var validIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

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
	Name    string `json:"name,omitempty"`
	Custom  bool   `json:"custom,omitempty"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Status  string `json:"status"`
	Code    string `json:"code,omitempty"`
	Notes   string `json:"notes,omitempty"`
	WireBrk bool   `json:"wireBreak,omitempty"`
}

type installedTransport struct {
	ID          string   `json:"id"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Author      string   `json:"author,omitempty"`
	Version     string   `json:"version"`
	Wire        int      `json:"wire,omitempty"`
	Custom      bool     `json:"custom,omitempty"`
	Update      []string `json:"update,omitempty"`
	Pubkey      string   `json:"pubkey,omitempty"`
	Disabled    bool     `json:"disabled,omitempty"`
	HasPrev     bool     `json:"hasPrevious,omitempty"`
	Bad         string   `json:"badVersion,omitempty"`
	Wired       bool     `json:"wired,omitempty"`
}

type sourceMeta struct {
	Custom bool     `json:"custom"`
	Update []string `json:"update,omitempty"`
	Pubkey string   `json:"pubkey,omitempty"`
}

var projectKeyHex = func() string { return script.OfficialKeys()[0] }

func packagePath(dir, id string) string { return filepath.Join(dir, id+".flux") }

func disabledPath(dir, id string) string { return filepath.Join(dir, id+".disabled") }

func failsPath(dir, id string) string { return filepath.Join(dir, id+".fails") }

func badPath(dir, id string) string { return filepath.Join(dir, id+".bad") }

func sourcePath(dir, id string) string { return filepath.Join(dir, id+".source.json") }

func readBad(dir, id string) string {
	raw, _ := os.ReadFile(badPath(dir, id))
	return strings.TrimSpace(string(raw))
}

func readSource(dir, id string) (sourceMeta, bool) {
	raw, err := os.ReadFile(sourcePath(dir, id))
	if err != nil {
		return sourceMeta{}, false
	}
	var m sourceMeta
	if json.Unmarshal(raw, &m) != nil || !m.Custom {
		return sourceMeta{}, false
	}
	return m, true
}

func writeSource(dir, id string, m sourceMeta) error {
	raw, _ := json.Marshal(m)
	return os.WriteFile(sourcePath(dir, id), raw, 0o644)
}

func keyFor(dir, id string) string {
	if m, ok := readSource(dir, id); ok && m.Pubkey != "" {
		return m.Pubkey
	}
	return projectKeyHex()
}

func loadPackage(path, keyHex string) (*script.Package, bool) {
	pub, err := script.DecodePublicKeyHex(keyHex)
	if err != nil {
		return nil, false
	}
	p, err := script.LoadSignedPackage(path, pub)
	if err != nil {
		return nil, false
	}
	return p, true
}

func installedVersion(dir, id string) (string, bool) {
	return versionAt(packagePath(dir, id), keyFor(dir, id))
}

func versionAt(path, keyHex string) (string, bool) {
	p, ok := loadPackage(path, keyHex)
	if !ok {
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

type target struct {
	inst   script.Installed
	name   string
	custom bool
}

func installedFor(dir, id, keyHex string, update []string) script.Installed {
	inst := script.Installed{ID: id, Version: "0.0.0", PubkeyHex: keyHex, Update: update}
	if p, ok := loadPackage(packagePath(dir, id), keyHex); ok {
		inst.Version = p.Manifest.Version
		inst.Wire = p.Manifest.EffectiveWire()
	}
	return inst
}

func customTargets(dir string) []target {
	files, _ := filepath.Glob(filepath.Join(dir, "*.source.json"))
	var out []target
	for _, f := range files {
		id := strings.TrimSuffix(filepath.Base(f), ".source.json")
		m, ok := readSource(dir, id)
		if !ok || !validIDRe.MatchString(id) {
			continue
		}
		out = append(out, target{inst: installedFor(dir, id, m.Pubkey, m.Update), name: id, custom: true})
	}
	return out
}

func catalogTargets(ctx context.Context, dir, catalogURL string, skip map[string]bool) ([]target, error) {
	if catalogURL == "" {
		catalogURL = defaultCatalogURL
	}
	raw, err := script.HTTPFetcher(ctx, catalogURL, catalogLimit)
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	var cat catalogFile
	if json.Unmarshal(raw, &cat) != nil || cat.Format != 1 {
		return nil, fmt.Errorf("catalog: unsupported")
	}
	var out []target
	for _, e := range cat.Transports {
		if !validIDRe.MatchString(e.ID) || skip[e.ID] || !script.SecureURL(e.Update) {
			continue
		}
		name := e.Name
		if name == "" {
			name = e.ID
		}
		out = append(out, target{inst: installedFor(dir, e.ID, projectKeyHex(), []string{e.Update}), name: name})
	}
	return out, nil
}

func allTargets(ctx context.Context, dir, catalogURL string) ([]target, error) {
	customs := customTargets(dir)
	skip := make(map[string]bool, len(customs))
	for _, c := range customs {
		skip[c.inst.ID] = true
	}
	cats, err := catalogTargets(ctx, dir, catalogURL, skip)
	return append(customs, cats...), err
}

func reportOf(t target, rep script.UpdateReport) TransportUpdate {
	return TransportUpdate{ID: t.inst.ID, Name: t.name, Custom: t.custom, From: rep.Current, To: rep.Latest, Status: rep.Status, Code: rep.Code, Notes: rep.Notes, WireBrk: rep.WireBreak}
}

func settleInstall(dir, id string) {
	os.Remove(disabledPath(dir, id))
	os.Remove(failsPath(dir, id))
}

func applyTarget(ctx context.Context, dir, channel string, t target, allowWireBreak, honourBad bool) TransportUpdate {
	check := script.CheckUpdate(ctx, t.inst, channel, script.HTTPFetcher)
	rep := check
	if bad := readBad(dir, t.inst.ID); honourBad && check.Status == script.UpdateAvailable && bad != "" && script.CompareVersions(check.Latest, bad) <= 0 {
		rep.Status, rep.Code = "skipped", "rolled_back_version"
	} else {
		rep = script.ApplyUpdate(ctx, t.inst, channel, dir, allowWireBreak, script.HTTPFetcher)
	}
	if rep.Status == script.UpdateInstalled {
		settleInstall(dir, t.inst.ID)
		if !honourBad {
			os.Remove(badPath(dir, t.inst.ID))
		}
	}
	return reportOf(t, rep)
}

func encode(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// TransportsCheck looks for newer packages without installing anything.
func TransportsCheck(dir, catalogURL, channel string) string {
	if dir == "" {
		return `{"error":"no directory"}`
	}
	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)
	defer cancel()
	targets, err := allTargets(ctx, dir, catalogURL)
	if err != nil && len(targets) == 0 {
		return jsonErr(err)
	}
	out := []TransportUpdate{}
	if err != nil {
		out = append(out, TransportUpdate{ID: "catalog", Status: "error", Code: script.CodeFetchFailed})
	}
	for _, t := range targets {
		out = append(out, reportOf(t, script.CheckUpdate(ctx, t.inst, channel, script.HTTPFetcher)))
	}
	return encode(out)
}

// TransportsUpdate installs every newer package that its source signed with the pinned key offers and that keeps the wire format. It returns a JSON array of results; a failure of one package does not stop the others.
func TransportsUpdate(dir, catalogURL, channel string) string {
	if dir == "" {
		return `{"error":"no directory"}`
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return jsonErr(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)
	defer cancel()
	targets, err := allTargets(ctx, dir, catalogURL)
	if err != nil && len(targets) == 0 {
		return jsonErr(err)
	}
	out := []TransportUpdate{}
	if err != nil {
		out = append(out, TransportUpdate{ID: "catalog", Status: "error", Code: script.CodeFetchFailed})
	}
	for _, t := range targets {
		out = append(out, applyTarget(ctx, dir, channel, t, false, true))
	}
	return encode(out)
}

// TransportsUpdateOne is the user's explicit update of one transport: a version they rolled back from is offered again, and a wire break goes through when allowWireBreak says so.
func TransportsUpdateOne(dir, id, catalogURL, channel string, allowWireBreak bool) string {
	if dir == "" || !validIDRe.MatchString(id) {
		return `{"status":"error","code":"usage"}`
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return jsonErr(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)
	defer cancel()
	targets, err := allTargets(ctx, dir, catalogURL)
	for _, t := range targets {
		if t.inst.ID == id {
			return encode(applyTarget(ctx, dir, channel, t, allowWireBreak, false))
		}
	}
	if err != nil {
		return jsonErr(err)
	}
	return `{"status":"error","code":"no_source"}`
}

// TransportsList describes every package in dir.
func TransportsList(dir string) string {
	out := []installedTransport{}
	files, _ := filepath.Glob(filepath.Join(dir, "*.flux"))
	sort.Strings(files)
	for _, f := range files {
		id := strings.TrimSuffix(filepath.Base(f), ".flux")
		meta, custom := readSource(dir, id)
		key := keyFor(dir, id)
		p, ok := loadPackage(f, key)
		if !ok {
			continue
		}
		_, disabled := os.Stat(disabledPath(dir, id))
		_, prev := os.Stat(f + ".prev")
		item := installedTransport{
			ID: id, Name: p.Manifest.Name, Description: p.Manifest.Description, Author: p.Manifest.Author,
			Version: p.Manifest.Version, Wire: p.Manifest.EffectiveWire(), Custom: custom,
			Disabled: disabled == nil, HasPrev: prev == nil, Bad: readBad(dir, id), Wired: overlayIDs[id],
		}
		if custom {
			item.Update, item.Pubkey = meta.Update, meta.Pubkey
		}
		out = append(out, item)
	}
	return encode(out)
}

// TransportsInstalled lists the packages in dir by id and version.
func TransportsInstalled(dir string) string { return TransportsList(dir) }

// TransportsAddCustom installs a transport from a source of the user's own: an update index (it keeps updating from there) or a direct .flux, both verified under the public key the user trusts for it.
func TransportsAddCustom(dir, source, pubkeyHex, channel string) string {
	fail := func(code string) string { return encode(map[string]string{"status": "error", "code": code}) }
	pubkeyHex = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(pubkeyHex), " ", ""))
	pub, err := script.DecodePublicKeyHex(pubkeyHex)
	if dir == "" || err != nil {
		return fail("bad_pubkey")
	}
	source = strings.TrimSpace(source)
	if !script.SecureURL(source) {
		return fail(script.CodeInsecureURL)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fail(script.CodeInstallFailed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)
	defer cancel()

	if u, err := url.Parse(source); err == nil && strings.HasSuffix(strings.ToLower(u.Path), ".flux") {
		data, err := script.HTTPFetcher(ctx, source, packageLimit)
		if err != nil {
			return fail(script.CodeFetchFailed)
		}
		pkg, err := script.ReadPackage(data)
		if err != nil {
			return fail(script.CodeBadPackage)
		}
		if pkg.Verify(pub) != nil {
			return fail(script.CodeBadSignature)
		}
		id := pkg.Manifest.EffectiveID()
		if !validIDRe.MatchString(id) {
			return fail(script.CodeIDMismatch)
		}
		if pkg.Manifest.EffectiveAPI() > script.APIVersion {
			return fail(script.CodeNeedsNewerApp)
		}
		if err := script.InstallPackage(dir, id+".flux", data); err != nil {
			return fail(script.CodeInstallFailed)
		}
		writeSource(dir, id, sourceMeta{Custom: true, Pubkey: pubkeyHex})
		settleInstall(dir, id)
		os.Remove(badPath(dir, id))
		return encode(map[string]string{"status": script.UpdateInstalled, "id": id, "version": pkg.Manifest.Version})
	}

	raw, err := script.HTTPFetcher(ctx, source, 256<<10)
	if err != nil {
		return fail(script.CodeFetchFailed)
	}
	var idx script.UpdateIndex
	if json.Unmarshal(raw, &idx) != nil || idx.Format != 1 || !validIDRe.MatchString(idx.ID) {
		return fail(script.CodeBadIndex)
	}
	inst := script.Installed{ID: idx.ID, Version: "0.0.0", PubkeyHex: pubkeyHex, Update: []string{source}}
	rep := script.ApplyUpdate(ctx, inst, channel, dir, true, script.HTTPFetcher)
	if rep.Status != script.UpdateInstalled {
		code := rep.Code
		if code == "" {
			code = rep.Status
		}
		return fail(code)
	}
	writeSource(dir, idx.ID, sourceMeta{Custom: true, Update: []string{source}, Pubkey: pubkeyHex})
	settleInstall(dir, idx.ID)
	os.Remove(badPath(dir, idx.ID))
	return encode(map[string]string{"status": script.UpdateInstalled, "id": idx.ID, "version": rep.Latest})
}

// TransportsRemove deletes a custom transport; one that comes from the catalog is switched off instead, or it would come back at the next update.
func TransportsRemove(dir, id string) string {
	if dir == "" || !validIDRe.MatchString(id) {
		return `{"status":"error","code":"usage"}`
	}
	if _, ok := readSource(dir, id); !ok {
		return `{"status":"error","code":"not_custom"}`
	}
	for _, p := range []string{packagePath(dir, id), packagePath(dir, id) + ".prev", packagePath(dir, id) + ".new", disabledPath(dir, id), failsPath(dir, id), badPath(dir, id), sourcePath(dir, id)} {
		os.Remove(p)
	}
	return `{"status":"removed"}`
}

// TransportsRollback restores the previous package of one transport; without one the transport goes back to the built-in implementation.
func TransportsRollback(dir, id string) string {
	if dir == "" || !validIDRe.MatchString(id) {
		return `{"status":"error","code":"usage"}`
	}
	if v, ok := installedVersion(dir, id); ok {
		os.WriteFile(badPath(dir, id), []byte(v), 0o644)
	}
	rep := script.RollbackPackage(dir, id+".flux", keyFor(dir, id))
	if rep.Status != script.UpdateInstalled {
		os.WriteFile(disabledPath(dir, id), []byte("rolled back"), 0o644)
	}
	return encode(rep)
}

// TransportsSetEnabled switches one dynamic transport off (or on) without removing it.
func TransportsSetEnabled(dir, id string, enabled bool) {
	if dir == "" || !validIDRe.MatchString(id) {
		return
	}
	if enabled {
		os.Remove(disabledPath(dir, id))
		os.Remove(failsPath(dir, id))
		return
	}
	os.WriteFile(disabledPath(dir, id), []byte("off"), 0o644)
}

func openPackage(cfg Config, id string) (*script.ScriptTransport, error) {
	dir := cfg.ScriptsDir
	if dir == "" || !validIDRe.MatchString(id) {
		return nil, fmt.Errorf("transport %q is not installed", id)
	}
	if _, off := os.Stat(disabledPath(dir, id)); off == nil {
		return nil, fmt.Errorf("transport %q is switched off", id)
	}
	pub, err := script.DecodePublicKeyHex(keyFor(dir, id))
	if err != nil {
		return nil, err
	}
	return script.New(id, packagePath(dir, id), pub, cfg.DocURL, map[string]interface{}{"exit": false}, transport.DefaultConfig())
}

// overlayScript opens the installed package for id when it may be used: not switched off, verified, and loadable.
func overlayScript(cfg Config, id string) *script.ScriptTransport {
	st, err := openPackage(cfg, id)
	if err != nil {
		if !os.IsNotExist(err) && !strings.Contains(err.Error(), "switched off") && !strings.Contains(err.Error(), "not installed") {
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
	if pv, ok := versionAt(packagePath(dir, id)+".prev", keyFor(dir, id)); ok && script.CompareVersions(pv, version) < 0 {
		rep = script.RollbackPackage(dir, id+".flux", keyFor(dir, id))
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
