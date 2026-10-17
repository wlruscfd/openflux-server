package transport

import (
	"os"
	"strconv"
	"strings"
	"sync"
)

var defaultHostRates = map[string]float64{
	"cloud.mail.ru": 0.2,
	"code.avito.ru": 4,
}

var (
	hostGatesMu sync.Mutex
	hostGates   map[string]*gate
)

func hostRates() map[string]float64 {
	rates := make(map[string]float64, len(defaultHostRates))
	for h, r := range defaultHostRates {
		rates[h] = r
	}
	for _, part := range strings.Split(os.Getenv("OPENFLUX_HOST_RATE"), ",") {
		host, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		if r, err := strconv.ParseFloat(value, 64); err == nil {
			rates[host] = r
		}
	}
	return rates
}

func WaitHostSlot(host string, done <-chan struct{}) bool {
	hostGatesMu.Lock()
	if hostGates == nil {
		hostGates = make(map[string]*gate)
		for h, r := range hostRates() {
			hostGates[h] = newConnectGate(r)
		}
	}
	g := hostGates[host]
	hostGatesMu.Unlock()
	if g == nil {
		return true
	}
	return g.wait(done)
}
