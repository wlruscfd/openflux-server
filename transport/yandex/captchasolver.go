package yandex

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// headlessCaptchaSolveTimeout bounds a single solve attempt: what typically "solves itself" is a
// JS/behavioral check a real browser clears in a couple of seconds, not a puzzle a human would
// need to click through - if client-config hasn't shown up by this point it's not going to, and
// holding the shared browser here only delays whatever key is queued behind this one.
const headlessCaptchaSolveTimeout = 20 * time.Second

// headlessBrowserIdleTimeout: the shared browser process is the only meaningfully expensive part
// of this (order 100+MB RSS) - killing it after a stretch of no captchas returns the node to its
// normal (tiny) footprint until the next one actually happens, instead of paying that cost forever
// on the off chance another key gets flagged soon.
const headlessBrowserIdleTimeout = 3 * time.Minute

// headlessSolver is one shared instance for the whole process, not one per key/transport - see
// EnableHeadlessCaptchaSolving's doc comment for why a single browser (and a single in-flight
// solve at a time, via solveMu) is the point on a small VPS running many keys.
var headlessSolver = &captchaSolver{}

type captchaSolver struct {
	// solveMu serializes actual navigations: two CAPTCHAs at once would otherwise mean two tabs
	// doing real work concurrently, which is exactly the CPU/memory spike a 1-CPU/1GB node can't
	// absorb. A second key just waits its turn - captchas are rare enough that this is cheap.
	solveMu sync.Mutex

	// mu guards the shared browser process's lifecycle (start/idle-shutdown) separately from
	// solveMu, so a solve in progress doesn't block a concurrent idle-timeout shutdown decision.
	mu          sync.Mutex
	allocCancel context.CancelFunc
	browserCtx  context.Context
	idleTimer   *time.Timer
}

// SolveCaptcha drives a shared, lazily-started headless Chrome/Chromium instance to the same
// doc_url a plain fetch got CAPTCHA-blocked on. What Yandex calls a "CAPTCHA" is often just a
// JS/behavioral bot check that a real browser environment clears on its own within a few seconds -
// this exists to catch exactly that case with no human present. A genuine interactive puzzle just
// times out here, and the caller's normal cooldown-and-retry path (see captchaCooldown in
// yandex.go) takes over exactly as it did before this existed - this is a best-effort bonus, never
// the only way forward.
func SolveCaptcha(docURL string) (string, error) {
	return headlessSolver.solve(docURL)
}

func (s *captchaSolver) solve(docURL string) (string, error) {
	s.solveMu.Lock()
	defer s.solveMu.Unlock()

	s.mu.Lock()
	if err := s.ensureStartedLocked(); err != nil {
		s.mu.Unlock()
		return "", err
	}
	browserCtx := s.browserCtx
	s.mu.Unlock()

	tabCtx, cancelTab := chromedp.NewContext(browserCtx)
	defer cancelTab()
	tabCtx, cancelTimeout := context.WithTimeout(tabCtx, headlessCaptchaSolveTimeout)
	defer cancelTimeout()

	// WaitReady blocks until an element matching the selector exists - the same signal
	// fetchDocInfo itself keys off (see yandex.go): present means the real doc editor loaded,
	// not a CAPTCHA/interstitial page.
	if err := chromedp.Run(tabCtx,
		chromedp.Navigate(docURL),
		chromedp.WaitReady("#client-config", chromedp.ByID),
	); err != nil {
		return "", fmt.Errorf("headless solve did not clear the check: %w", err)
	}

	cookieStr, err := extractCookies(tabCtx, docURL)
	if err != nil {
		return "", fmt.Errorf("extract cookies after solve: %w", err)
	}
	if cookieStr == "" {
		return "", fmt.Errorf("solve reported success but no cookies were set")
	}

	s.mu.Lock()
	s.resetIdleTimerLocked()
	s.mu.Unlock()

	return cookieStr, nil
}

func extractCookies(ctx context.Context, docURL string) (string, error) {
	var cookies []*network.Cookie
	err := chromedp.Run(ctx, chromedp.ActionFunc(func(ctx context.Context) error {
		var err error
		cookies, err = network.GetCookies().WithURLs([]string{docURL}).Do(ctx)
		return err
	}))
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; "), nil
}

// ensureStartedLocked must be called with mu held. Flags below are chosen for a box that can be as
// small as 1 CPU / 1GB RAM running this as one of several processes (controlplane/nodeagent/DB) -
// this isn't a general "headless Chrome tips" list, every flag here is answering a specific memory
// or process-count question for that box.
func (s *captchaSolver) ensureStartedLocked() error {
	if s.browserCtx != nil {
		return nil
	}

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		// Already root for raw-mode exit nodes anyway; the sandbox needs a user-namespace setup
		// many minimal VPS images/containers don't have, and failing to start over that would be
		// worse than not sandboxing a browser we only ever point at one fixed doc_url.
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-gpu", true),
		// /dev/shm is tiny or absent on many minimal VPS images; Chrome's default use of shared
		// memory for tab data crashes there instead of just running slower.
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("disable-translate", true),
		chromedp.Flag("disable-default-apps", true),
		chromedp.Flag("mute-audio", true),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("metrics-recording-only", true),
		// One renderer process total instead of one per tab - the single biggest memory lever
		// available here, since we only ever run one tab at a time anyway (see solveMu).
		chromedp.Flag("renderer-process-limit", "1"),
		chromedp.Flag("disk-cache-size", "1"),
		// Caps V8's heap per renderer; a doc-editor bot-check page doesn't need more than this,
		// and an unbounded heap is exactly what a 1GB box can't spare.
		chromedp.Flag("js-flags", "--max-old-space-size=128"),
		chromedp.WindowSize(1024, 768),
	)

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	browserCtx, _ := chromedp.NewContext(allocCtx)
	// Forces the browser process to actually start now instead of on first navigate, so a
	// missing/broken Chrome/Chromium binary fails right here - where the caller already has a
	// working fallback (captchaCooldown) - rather than partway through a solve.
	if err := chromedp.Run(browserCtx); err != nil {
		allocCancel()
		return fmt.Errorf("start headless browser: %w", err)
	}

	s.allocCancel = allocCancel
	s.browserCtx = browserCtx
	s.resetIdleTimerLocked()
	return nil
}

// resetIdleTimerLocked must be called with mu held.
func (s *captchaSolver) resetIdleTimerLocked() {
	if s.idleTimer != nil {
		s.idleTimer.Stop()
	}
	s.idleTimer = time.AfterFunc(headlessBrowserIdleTimeout, s.shutdownIdle)
}

func (s *captchaSolver) shutdownIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.allocCancel != nil {
		s.allocCancel()
	}
	s.allocCancel = nil
	s.browserCtx = nil
	s.idleTimer = nil
}
