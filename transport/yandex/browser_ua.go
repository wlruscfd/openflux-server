package yandex

import "net/http"

// Deliberately bare/vague, not a specific version - a convincing version string is what triggers Yandex's CAPTCHA on a datacenter IP, confirmed by testing.
const browserUserAgent = "Mozilla/5.0"

// applyBrowserGetHeaders deliberately skips Accept-Encoding (would disable Go's transparent decompression) and Chromium-only Sec-Ch-Ua hints (a Firefox UA sending them is a bigger tell than sending neither).
func applyBrowserGetHeaders(h http.Header) {
	h.Set("User-Agent", browserUserAgent)
	h.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	h.Set("Accept-Language", "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3")
	h.Set("Upgrade-Insecure-Requests", "1")
	h.Set("Sec-Fetch-Dest", "document")
	h.Set("Sec-Fetch-Mode", "navigate")
	h.Set("Sec-Fetch-Site", "none")
	h.Set("Sec-Fetch-User", "?1")
}
