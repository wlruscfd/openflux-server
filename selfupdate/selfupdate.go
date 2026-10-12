package selfupdate

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

// Version is stamped into release builds (-ldflags "-X .../selfupdate.Version=0.1.0"); a build from source keeps "dev" and counts as older than every release.
var Version = "dev"

// PinnedKeys are the ed25519 public keys (hex) whose signature a release index must carry.
var PinnedKeys = []string{
	"1ac6bdfd704fb02129dfea5e25dac1140764d042fc1c9fa04ff34738a3a692ea",
}

const (
	indexFormat   = 1
	maxIndexBytes = 1 << 20
)

type Asset struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

type Index struct {
	Format  int              `json:"format"`
	Version string           `json:"version"`
	Notes   string           `json:"notes,omitempty"`
	Assets  map[string]Asset `json:"assets"`
}

func PlatformKey() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

func parseKeys(hexKeys []string) []ed25519.PublicKey {
	var out []ed25519.PublicKey
	for _, k := range hexKeys {
		raw, err := hex.DecodeString(strings.TrimSpace(k))
		if err != nil || len(raw) != ed25519.PublicKeySize {
			continue
		}
		out = append(out, ed25519.PublicKey(raw))
	}
	return out
}

// ParseIndex accepts an index only when its detached signature verifies under one of the pinned keys, so a hijacked mirror or release page can at worst withhold an update.
func ParseIndex(index, sigHex []byte, keys []ed25519.PublicKey) (Index, error) {
	var idx Index
	if len(index) == 0 || len(index) > maxIndexBytes {
		return idx, errors.New("update index has an unreasonable size")
	}
	sig, err := hex.DecodeString(strings.TrimSpace(string(sigHex)))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return idx, errors.New("update index signature is malformed")
	}
	ok := false
	for _, k := range keys {
		if ed25519.Verify(k, index, sig) {
			ok = true
			break
		}
	}
	if !ok {
		return idx, errors.New("update index is not signed by a pinned key")
	}
	if err := json.Unmarshal(index, &idx); err != nil {
		return idx, fmt.Errorf("update index does not parse: %w", err)
	}
	if idx.Format != indexFormat {
		return idx, fmt.Errorf("update index format %d is not supported", idx.Format)
	}
	if strings.TrimSpace(idx.Version) == "" {
		return idx, errors.New("update index has no version")
	}
	return idx, nil
}

// CompareVersions orders dotted numeric versions; "dev" or anything unparsable sorts below every release.
func CompareVersions(a, b string) int {
	pa, oka := versionParts(a)
	pb, okb := versionParts(b)
	switch {
	case !oka && !okb:
		return 0
	case !oka:
		return -1
	case !okb:
		return 1
	}
	for i := 0; i < len(pa) || i < len(pb); i++ {
		var x, y int
		if i < len(pa) {
			x = pa[i]
		}
		if i < len(pb) {
			y = pb[i]
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

func versionParts(v string) ([]int, bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if v == "" {
		return nil, false
	}
	var out []int
	for _, p := range strings.Split(v, ".") {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}
