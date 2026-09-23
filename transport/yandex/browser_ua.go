package yandex

import "net/http"

// browserUserAgent is deliberately the bare, vague string and not a specific current browser
// fingerprint: side-by-side testing from a real exit-node IP (same doc_url, same other headers,
// only this value changed) showed Yandex's antibot CAPTCHA-walling every specific Firefox/Chrome/
// Safari version tried - including this file's own former value - while a bare "Mozilla/5.0"
// passed every time. A convincing *version* is apparently the tell on a datacenter IP, not an
// unconvincing one; don't "fix" this back to a realistic-looking UA without re-testing first.
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
