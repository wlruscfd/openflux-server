// Command proxyprobe takes a pile of proxy links and reports which ones actually reach a target.
//
// The panel balances over every key it is given, and a public key list is mostly dead: a dead key
// costs a carrier dial its full timeout before the balancer moves on. So candidates have to be
// measured before they are configured, and that is what this does: it stands up sing-box the way
// the node does, then reaches the target through each key and prints what answered.
//
//	echo proxyprobe -in keys.txt -target docs.yandex.ru
//	echo proxyprobe -in keys.txt -target docs.yandex.ru -count 20 -json
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/p1neappleXpress/OpenFlux/outbound"
)

type result struct {
	Link     string        `json:"link"`
	Name     string        `json:"name"`
	Kind     string        `json:"kind"`
	Server   string        `json:"server"`
	Port     int           `json:"port"`
	OK       bool          `json:"ok"`
	Latency  time.Duration `json:"-"`
	Millis   int64         `json:"latency_ms"`
	Status   int           `json:"status,omitempty"`
	Error    string        `json:"error,omitempty"`
	ExitAddr string        `json:"exit_addr,omitempty"`
}

func main() {
	in := flag.String("in", "", "file with one proxy link per line; - reads stdin")
	target := flag.String("target", "docs.yandex.ru", "host:port to reach through every key")
	path := flag.String("path", "/", "http path to request")
	count := flag.Int("count", 0, "stop after this many links (0 = all)")
	parallel := flag.Int("parallel", 16, "how many keys to test at once")
	timeout := flag.Duration("timeout", 12*time.Second, "per key budget")
	binary := flag.String("singbox", "sing-box", "sing-box binary")
	asJSON := flag.Bool("json", false, "print JSON")
	flag.Parse()

	links, err := readLinks(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proxyprobe:", err)
		os.Exit(2)
	}
	if *count > 0 && len(links) > *count {
		links = links[:*count]
	}
	if len(links) == 0 {
		fmt.Fprintln(os.Stderr, "proxyprobe: no links to test")
		os.Exit(2)
	}

	keys := make([]outbound.Key, 0, len(links))
	skipped := 0
	for _, link := range links {
		k, err := outbound.ParseLink(link)
		if err != nil {
			skipped++
			continue
		}
		k.ID = fmt.Sprintf("probe-%d", len(keys))
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		fmt.Fprintln(os.Stderr, "proxyprobe: not one link parsed")
		os.Exit(2)
	}
	if skipped > 0 {
		fmt.Fprintf(os.Stderr, "proxyprobe: %d links did not parse, skipped\n", skipped)
	}

	sidecar := outbound.NewSidecar(outbound.SidecarOptions{
		Binary:    *binary,
		PortLow:   42000,
		PortHigh:   42999,
		LogLevel:  "error",
		StartWait: 30 * time.Second,
	})
	defer sidecar.Stop()

	results := probeInBatches(sidecar, keys, *target, *path, *parallel, *timeout)

	alive := 0
	for _, r := range results {
		if r.OK {
			alive++
		}
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(results); err != nil {
			fmt.Fprintln(os.Stderr, "proxyprobe:", err)
		}
	} else {
		printTable(results)
	}
	fmt.Fprintf(os.Stderr, "\n%d of %d keys reached %s\n", alive, len(keys), *target)
	if alive == 0 {
		os.Exit(1)
	}
}

// probeInBatches applies a batch and probes it before starting the next one. sing-box is restarted
// for every batch because it holds one listener per key, so a batch that is applied and left behind
// is a batch whose ports die when the next one starts - and a run that applies everything first and
// probes afterwards measures only the last batch and calls the rest dead.
func probeInBatches(sidecar *outbound.Sidecar, keys []outbound.Key, target, path string, parallel int, timeout time.Duration) []result {
	const batch = 64
	targetAddr := target
	if _, _, err := net.SplitHostPort(target); err != nil {
		targetAddr = net.JoinHostPort(target, "443")
	}
	var all []result
	for i := 0; i < len(keys); i += batch {
		j := min(i+batch, len(keys))
		chunk := keys[i:j]
		if err := sidecar.Apply(chunk); err != nil {
			for _, k := range chunk {
				all = append(all, result{Link: k.Link, Name: k.Name, Kind: string(k.Kind),
					Server: k.Server, Port: k.Port, Error: "batch failed: " + err.Error()})
			}
			fmt.Fprintf(os.Stderr, "proxyprobe: batch %d of %d not served: %v\n", i/batch+1, (len(keys)+batch-1)/batch, err)
			continue
		}
		all = append(all, probeBatch(sidecar, chunk, targetAddr, path, parallel, timeout)...)
	}
	sort.SliceStable(all, func(a, b int) bool {
		if all[a].OK != all[b].OK {
			return all[a].OK
		}
		return all[a].Millis < all[b].Millis
	})
	return all
}

func readLinks(path string) ([]string, error) {
	var f *os.File
	if path == "" || path == "-" {
		f = os.Stdin
	} else {
		var err error
		f, err = os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
	}
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out, sc.Err()
}

func probeBatch(sidecar *outbound.Sidecar, keys []outbound.Key, targetAddr, path string, parallel int, timeout time.Duration) []result {
	results := make([]result, len(keys))
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(parallel, 1))
	for i, k := range keys {
		wg.Add(1)
		go func(i int, k outbound.Key) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = probeOne(sidecar, k, targetAddr, path, timeout)
		}(i, k)
	}
	wg.Wait()
	return results
}

func probeOne(sidecar *outbound.Sidecar, k outbound.Key, targetAddr, path string, timeout time.Duration) result {
	r := result{Link: k.Link, Name: k.Name, Kind: string(k.Kind), Server: k.Server, Port: k.Port}
	d := sidecar.DialerFor(k.ID)
	if d == nil {
		r.Error = "no local listener for this key"
		return r
	}
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: d.DialContext,
			// One host per probe; a shared pool would keep a dead key's connection alive.
			DisableKeepAlives: true,
			TLSHandshakeTimeout: timeout / 2,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := "https://" + targetAddr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	started := time.Now()
	resp, err := client.Do(req)
	r.Latency = time.Since(started)
	r.Millis = r.Latency.Milliseconds()
	if err != nil {
		r.Error = trimError(err.Error())
		return r
	}
	defer resp.Body.Close()
	r.OK = true
	r.Status = resp.StatusCode
	return r
}

func trimError(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}

func printTable(results []result) {
	fmt.Printf("%-6s %-10s %-22s %-9s %s\n", "OK", "LATENCY", "SERVER", "KIND", "ERROR")
	for _, r := range results {
		ok, latency, server, errText := "-", "-", r.Server, r.Error
		if r.OK {
			ok = "yes"
			latency = fmt.Sprintf("%dms", r.Millis)
			if r.Status != 0 && r.Status != 200 {
				ok = fmt.Sprintf("%d", r.Status)
			}
		} else if r.Millis > 0 {
			latency = fmt.Sprintf("%dms", r.Millis)
		}
		fmt.Printf("%-6s %-10s %-22s %-9s %s\n", ok, latency, server, r.Kind, errText)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}