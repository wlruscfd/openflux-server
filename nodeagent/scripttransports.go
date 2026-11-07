package nodeagent

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/transport/registry"
	"github.com/p1neappleXpress/OpenFlux/transport/script"
)

//go:embed scriptpkgs/bitrix.flux
var bitrixFlux []byte

//go:embed scriptpkgs/pruffme.flux
var pruffmeFlux []byte

//go:embed scriptpkgs/bitrix-call.flux
var bitrixCallFlux []byte

// managedScriptPkgs maps the panel's transport name to the signed package the
// node runs that key with. The packages are verified against the fork's
// transports key on every start (script.New fails closed), so a swapped file
// on disk cannot run.
var managedScriptPkgs = map[string][]byte{
	"bitrix":       bitrixFlux,
	"pruffme":      pruffmeFlux,
	"bitrix-call":  bitrixCallFlux,
}

func managedTransportSupported(name string) bool {
	switch name {
	case "yandex", "yandex_multistream", "boards", "mailru", "mts", "telemost", "cupsonline", "avito":
		return true
	}
	return packageIDRe.MatchString(name)
}

var scriptPkgPaths sync.Map

func scriptPkgDir() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		dir = os.TempDir()
	}
	pkgDir := filepath.Join(dir, "openflux", "scriptpkgs")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		pkgDir = os.TempDir()
	}
	return pkgDir, nil
}

func scriptPkgPath(name string, data []byte) (string, error) {
	if p, ok := scriptPkgPaths.Load(name); ok {
		return p.(string), nil
	}
	pkgDir, err := scriptPkgDir()
	if err != nil {
		return "", err
	}
	path := filepath.Join(pkgDir, name+".flux")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("materialize the %s package: %w", name, err)
	}
	scriptPkgPaths.Store(name, path)
	return path, nil
}

// newScriptTransport starts a managed key's carrier from the embedded signed
// package. The script gets the key's doc_url as cfg.url and the exit role; its
// own settings (info().params) are covered by the panel's doc_url.
func newScriptTransport(name, docURL, session string) (transport.Transport, error) {
	path, err := fetchScriptPackage(name)
	if err != nil {
		return nil, err
	}
	return registry.New("script", docURL, map[string]interface{}{
		"path":    path,
		"pubkey":  script.ForkKeyHex,
		"name":    name,
		"session": session,
	}, registry.Options{Base: transport.DefaultConfig(), IsExit: true})
}
