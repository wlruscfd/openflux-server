package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport/script"
)

// catalogURL lists the transports the node can run from signed packages it
// has no embedded copy of. Anything a panel key names that the core does not
// build natively resolves here, so a new JS transport reaches managed keys
// without a core change.
const catalogURL = "https://raw.githubusercontent.com/wlruscfd/openflux-transports/main/catalog.json"

var packageIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type catalogEntry struct {
	ID     string `json:"id"`
	Update string `json:"update"`
}

type updateIndex struct {
	Format   int `json:"format"`
	Channels map[string]struct {
		Version string `json:"version"`
		URL     string `json:"url"`
		SHA256  string `json:"sha256"`
	} `json:"channels"`
}

// fetchScriptPackage returns a verified package file for the transport id:
// the embedded copy when the node ships one, otherwise the signed package
// from the catalog (sha256 checked, signature verified against the fork key
// before anything runs). The download is cached in the package directory.
func fetchScriptPackage(name string) (string, error) {
	if data, ok := managedScriptPkgs[name]; ok {
		return scriptPkgPath(name, data)
	}
	if !packageIDRe.MatchString(name) {
		return "", fmt.Errorf("invalid transport id %q", name)
	}
	cache := filepath.Join(scriptPkgDirOrTemp(), name+".flux")
	if _, err := os.Stat(cache); err == nil {
		return cache, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	catRaw, err := script.HTTPFetcher(ctx, catalogURL, 256<<10)
	if err != nil {
		return "", fmt.Errorf("catalog: %w", err)
	}
	var cat struct {
		Format     int            `json:"format"`
		Transports []catalogEntry `json:"transports"`
	}
	if json.Unmarshal(catRaw, &cat) != nil || cat.Format != 1 {
		return "", fmt.Errorf("catalog: unsupported")
	}
	var entry *catalogEntry
	for i := range cat.Transports {
		if cat.Transports[i].ID == name {
			entry = &cat.Transports[i]
			break
		}
	}
	if entry == nil || entry.Update == "" {
		return "", fmt.Errorf("catalog: no transport %q", name)
	}

	idxRaw, err := script.HTTPFetcher(ctx, entry.Update, 64<<10)
	if err != nil {
		return "", fmt.Errorf("update index: %w", err)
	}
	var idx updateIndex
	if json.Unmarshal(idxRaw, &idx) != nil || idx.Format != 1 {
		return "", fmt.Errorf("update index: unsupported")
	}
	ch, ok := idx.Channels["stable"]
	if !ok || ch.URL == "" || len(ch.SHA256) != 64 {
		return "", fmt.Errorf("update index: no stable channel for %q", name)
	}
	pkgURL := resolveCatalogURL(entry.Update, ch.URL)
	pkgRaw, err := script.HTTPFetcher(ctx, pkgURL, 8<<20)
	if err != nil {
		return "", fmt.Errorf("package: %w", err)
	}
	sum := sha256.Sum256(pkgRaw)
	if hex.EncodeToString(sum[:]) != ch.SHA256 {
		return "", fmt.Errorf("package: sha256 mismatch")
	}
	dir := scriptPkgDirOrTemp()
	tmp := filepath.Join(dir, name+".flux.tmp")
	if err := os.WriteFile(tmp, pkgRaw, 0o644); err != nil {
		return "", err
	}
	pub, err := script.DecodePublicKeyHex(script.ForkKeyHex)
	if err != nil {
		os.Remove(tmp)
		return "", err
	}
	if _, err := script.LoadSignedPackage(tmp, pub); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("package: signature: %w", err)
	}
	final := filepath.Join(dir, name+".flux")
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return final, nil
}

func scriptPkgDirOrTemp() string {
	dir, err := scriptPkgDir()
	if err != nil {
		return os.TempDir()
	}
	return dir
}

// resolveCatalogURL makes the package URL absolute against the update.json
// URL it came from (catalog entries use relative file names).
func resolveCatalogURL(updateURL, pkgURL string) string {
	if len(pkgURL) > 8 && (pkgURL[:8] == "https://" || pkgURL[:7] == "http://") {
		return pkgURL
	}
	base := updateURL
	if i := len(base) - 1; i >= 0 {
		for ; i >= 0 && base[i] != '/'; i-- {
		}
		if i >= 0 {
			return base[:i+1] + pkgURL
		}
	}
	return pkgURL
}
