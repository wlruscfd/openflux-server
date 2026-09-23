package yandex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

const headlessCaptchaSolveTimeout = 20 * time.Second
const headlessBrowserIdleTimeout = 3 * time.Minute

var headlessSolver = &captchaSolver{}

type captchaSolver struct {
	solveMu sync.Mutex

	mu          sync.Mutex
	allocCancel context.CancelFunc
	browserCtx  context.Context
	idleTimer   *time.Timer
}

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

	waitCtx, cancelWait := context.WithTimeout(tabCtx, headlessCaptchaSolveTimeout)
	err := chromedp.Run(waitCtx,
		chromedp.Navigate(docURL),
		chromedp.WaitReady("#client-config", chromedp.ByID),
	)
	cancelWait()
	if err != nil {
		if shot := saveFailureScreenshot(tabCtx); shot != "" {
			return "", fmt.Errorf("headless solve did not clear the check (screenshot: %s): %w", shot, err)
		}
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

func saveFailureScreenshot(ctx context.Context) string {
	shotCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var buf []byte
	if err := chromedp.Run(shotCtx, chromedp.CaptureScreenshot(&buf)); err != nil || len(buf) == 0 {
		return ""
	}
	path := filepath.Join(os.TempDir(), fmt.Sprintf("openflux-captcha-%d.png", time.Now().UnixNano()))
	if os.WriteFile(path, buf, 0o644) != nil {
		return ""
	}
	return path
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

func (s *captchaSolver) ensureStartedLocked() error {
	if s.browserCtx != nil {
		return nil
	}

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("disable-translate", true),
		chromedp.Flag("disable-default-apps", true),
		chromedp.Flag("mute-audio", true),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("metrics-recording-only", true),
		chromedp.Flag("renderer-process-limit", "1"),
		chromedp.Flag("disk-cache-size", "1"),
		chromedp.Flag("js-flags", "--max-old-space-size=128"),
		chromedp.WindowSize(1024, 768),
	)

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	browserCtx, _ := chromedp.NewContext(allocCtx)
	if err := chromedp.Run(browserCtx); err != nil {
		allocCancel()
		return fmt.Errorf("start headless browser: %w", err)
	}

	s.allocCancel = allocCancel
	s.browserCtx = browserCtx
	s.resetIdleTimerLocked()
	return nil
}

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
