package outbound

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestDumpSingboxConfigForLinks writes a sing-box config for a list of links, so the same
// builder the node uses can be tried against the real thing outside the node.
func TestDumpSingboxConfigForLinks(t *testing.T) {
	path := os.Getenv("OFX_LINKS")
	if path == "" {
		t.Skip("OFX_LINKS is not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	limit := 6
	if v := os.Getenv("OFX_LIMIT"); v != "" {
		limit = 6
	}
	keys := []Key{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, err := ParseLink(line)
		if err != nil {
			continue
		}
		if err := k.Valid(); err != nil {
			continue
		}
		k.ID = "k" + strconv.Itoa(len(keys))
		keys = append(keys, k)
		if len(keys) >= limit {
			break
		}
	}
	if len(keys) == 0 {
		t.Fatal("no usable link")
	}
	ports := map[string]int{}
	for i, k := range keys {
		ports[k.ID] = 41900 + i
	}
	cfg, err := buildSingboxConfig(keys, ports, "info")
	if err != nil {
		t.Fatal(err)
	}
	out := os.Getenv("OFX_CONFIG_OUT")
	if out == "" {
		out = "singbox-probe.json"
	}
	if err := os.WriteFile(out, cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		t.Logf("key %s kind=%s server=%s port=%d socks=%d uuid=%s sni=%s transport=%s path=%s",
			k.ID, k.Kind, k.Server, k.Port, ports[k.ID], k.UUID, k.SNI, k.TransportType, k.Path)
	}
	t.Logf("config written to %s", out)
}
