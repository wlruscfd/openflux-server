package nodeagent

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

const (
	eventLogInterval   = time.Minute
	summaryLogInterval = time.Minute
	maxWaitingListed   = 10
)

func keyEventLogger(keyID, transportName string) func(code, detail string) {
	var mu sync.Mutex
	last := make(map[string]time.Time)

	return func(code, detail string) {
		reason := code
		message := ""
		switch code {
		case transport.EventConnected:
			message = "transport connected"
		case transport.EventRetrying:
			parts := strings.SplitN(detail, "|", 4)
			if len(parts) != 4 {
				return
			}
			reason = code + ":" + parts[2]
			message = fmt.Sprintf("attempt %s failed (%s: %s), next try in %ss", parts[0], parts[2], parts[3], parts[1])
		case transport.EventCaptchaRequired:
			message = "the provider asked for a captcha; waiting for a cookie jar pushed from the app"
		default:
			return
		}

		mu.Lock()
		now := time.Now()
		if code != transport.EventConnected && now.Sub(last[reason]) < eventLogInterval {
			mu.Unlock()
			return
		}
		last[reason] = now
		mu.Unlock()

		log.Printf("[NODEAGENT] key %s (%s): %s", keyID, transportName, message)
	}
}

func (o *Orchestrator) logSummary() {
	o.mu.Lock()
	total := len(o.workers)
	var waiting []string
	for id, w := range o.workers {
		if !w.trans.IsConnected() {
			waiting = append(waiting, id)
		}
	}
	due := time.Since(o.lastSummaryAt) >= summaryLogInterval
	if due {
		o.lastSummaryAt = time.Now()
	}
	o.mu.Unlock()

	if !due || total == 0 {
		return
	}
	if len(waiting) == 0 {
		log.Printf("[NODEAGENT] %d key(s) running, all connected to their provider", total)
		return
	}
	shown := waiting
	more := ""
	if len(shown) > maxWaitingListed {
		shown = shown[:maxWaitingListed]
		more = fmt.Sprintf(" and %d more", len(waiting)-maxWaitingListed)
	}
	log.Printf("[NODEAGENT] %d key(s) running, %d not connected to their provider yet: %s%s", total, len(waiting), strings.Join(shown, ", "), more)
}

func (o *Orchestrator) warnThrottled(key, format string, args ...interface{}) {
	o.mu.Lock()
	if o.warnedAt == nil {
		o.warnedAt = make(map[string]time.Time)
	}
	due := time.Since(o.warnedAt[key]) >= eventLogInterval
	if due {
		o.warnedAt[key] = time.Now()
	}
	o.mu.Unlock()
	if due {
		log.Printf(format, args...)
	}
}
