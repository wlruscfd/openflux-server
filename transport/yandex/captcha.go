package yandex

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"universal-bypass-tool/utils"
)

func solveCaptcha(docURL string, jar http.CookieJar, userAgent string, rt http.RoundTripper) (string, error) {
	return solveCaptchaDepth(docURL, jar, userAgent, rt, 3)
}

func solveCaptchaDepth(docURL string, jar http.CookieJar, userAgent string, rt http.RoundTripper, remaining int) (string, error) {
	if jar == nil {
		return "", fmt.Errorf("captcha: nil cookiejar")
	}
	if userAgent == "" {
		userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) Gecko/20100101 Firefox/153.0"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := &http.Client{
		Jar:       jar,
		Timeout:   30 * time.Second,
		Transport: rt,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	utils.Debugf("[CAPTCHA] solve start: url=%s", docURL)

	captchaURL := ""
	currentURL := docURL
	for i := 0; i < 10; i++ {
		utils.Debugf("[CAPTCHA] GET %s", shortStr(currentURL, 120))
		req, _ := http.NewRequestWithContext(ctx, "GET", currentURL, nil)
		setBrowserHeaders(req, userAgent)
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("captcha GET: %w", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		utils.Debugf("[CAPTCHA]   status=%d location=%s",
			resp.StatusCode, shortStr(resp.Header.Get("Location"), 100))

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if looksLikeCaptchaHTML(body) {
				captchaURL = resp.Request.URL.String()
				break
			}
			utils.Debugf("[CAPTCHA] 200 OK — капча не требуется")
			return "", nil
		}

		if resp.StatusCode < 300 || resp.StatusCode >= 400 {
			return "", fmt.Errorf("captcha unexpected status %d", resp.StatusCode)
		}

		loc, err := resp.Location()
		if err != nil {
			return "", fmt.Errorf("captcha redirect: %w", err)
		}
		nextURL, err := resp.Request.URL.Parse(loc.String())
		if err != nil {
			return "", fmt.Errorf("captcha redirect URL: %w", err)
		}
		if isCaptchaURL(nextURL.String()) {
			captchaURL = nextURL.String()
			break
		}
		currentURL = nextURL.String()
	}

	if captchaURL == "" {
		return "", fmt.Errorf("captcha: challenge URL not found in redirect chain")
	}

	utils.Debugf("[CAPTCHA] GET %s", shortStr(captchaURL, 120))
	req, _ := http.NewRequestWithContext(ctx, "GET", captchaURL, nil)
	setBrowserHeaders(req, userAgent)
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("captcha challenge GET: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("captcha challenge status %d", resp.StatusCode)
	}
	utils.Debugf("[CAPTCHA] challenge: %d bytes", len(body))

	ssr, formAction, err := parseCaptchaHTML(string(body), resp.Request.URL.String())
	if err != nil {
		return "", err
	}
	utils.Debugf("[CAPTCHA] uniqueKey=%s timestamp=%d complexity=%d prefix=%s",
		ssr.UniqueKey, ssr.Timestamp, ssr.PowComplexity, shortStr(ssr.PowPrefix, 32))

	started := time.Now()
	nonceHex, attempts, elapsed, err := solveCaptchaPoW(ctx, ssr.PowPrefix, ssr.PowComplexity)
	if err != nil {
		return "", fmt.Errorf("captcha PoW after %d attempts: %w", attempts, err)
	}
	if nonceHex == "" {
		return "", fmt.Errorf("captcha PoW exhausted after %d attempts", attempts)
	}
	calcTime := elapsed.Milliseconds()
	utils.Debugf("[CAPTCHA] PoW solved: nonce=%s attempts=%d time=%v",
		nonceHex, attempts, time.Since(started))

	form := url.Values{}
	if ssr.Legacy {
		form.Set("version", "1.5.0")
		form.Set("uniquekey", ssr.UniqueKey)
		form.Set("chstate", "ok")
		form.Set("fingerprint", encodeCaptchaFingerprint(buildLegacyCaptchaFingerprint(nonceHex, ssr.UniqueKey, elapsed)))
	} else {
		form.Set("rdata", encodeCaptchaJSON(buildCaptchaFingerprint(nonceHex, userAgent)))
		form.Set("pdata", encodeCaptchaPoWData(ssr.PowPrefix, nonceHex, calcTime))
		form.Set("tdata", "")
		form.Set("picasso", "")
	}

	utils.Debugf("[CAPTCHA] POST %s", shortStr(formAction, 100))
	req2, _ := http.NewRequestWithContext(ctx, "POST", formAction, strings.NewReader(form.Encode()))
	setBrowserHeaders(req2, userAgent)
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req2.Header.Set("Origin", originOf(resp.Request.URL))
	req2.Header.Set("Referer", captchaURL)

	resp2, err := client.Do(req2)
	if err != nil {
		return "", fmt.Errorf("captcha POST: %w", err)
	}
	responseBody, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	utils.Debugf("[CAPTCHA] POST result: status=%d location=%s",
		resp2.StatusCode, shortStr(resp2.Header.Get("Location"), 120))

	if resp2.StatusCode < 200 || resp2.StatusCode >= 400 {
		return "", fmt.Errorf("captcha POST unexpected status %d: %s", resp2.StatusCode, shortStr(string(responseBody), 200))
	}

	retpath := docURL
	if location := resp2.Header.Get("Location"); location != "" {
		parsed, err := resp2.Request.URL.Parse(location)
		if err != nil {
			return "", fmt.Errorf("captcha POST redirect: %w", err)
		}
		retpath = parsed.String()
	}

	acceptedURL, err := followCaptchaRetpath(ctx, client, retpath, userAgent)
	if err != nil {
		var redirect *captchaRedirectError
		if errors.As(err, &redirect) && remaining > 1 {
			return solveCaptchaDepth(redirect.url, jar, userAgent, rt, remaining-1)
		}
		return "", fmt.Errorf("captcha result rejected: %w", err)
	}

	utils.Debugf("[CAPTCHA] solve OK, retpath=%s", shortStr(acceptedURL, 120))
	return acceptedURL, nil
}

type captchaRedirectError struct {
	url string
}

func (e *captchaRedirectError) Error() string {
	return "another CAPTCHA at " + shortStr(e.url, 120)
}

func followCaptchaRetpath(ctx context.Context, client *http.Client, retpath, userAgent string) (string, error) {
	currentURL := retpath
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequestWithContext(ctx, "GET", currentURL, nil)
		setBrowserHeaders(req, userAgent)
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			loc, err := resp.Location()
			if err != nil {
				return "", err
			}
			nextURL, err := resp.Request.URL.Parse(loc.String())
			if err != nil {
				return "", err
			}
			if isCaptchaURL(nextURL.String()) {
				return "", &captchaRedirectError{url: nextURL.String()}
			}
			currentURL = nextURL.String()
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("retpath status %d", resp.StatusCode)
		}
		if looksLikeCaptchaHTML(body) {
			return "", &captchaRedirectError{url: currentURL}
		}
		return currentURL, nil
	}
	return "", fmt.Errorf("too many redirects from retpath")
}

type captchaSSRData struct {
	UniqueKey     string
	PowPrefix     string
	PowComplexity int
	Timestamp     int64
	Legacy        bool
}

var (
	reSSRDataLegacy = regexp.MustCompile(`window\.__SSR_DATA__\s*=\s*JSON\.parse\(atob\("([^"]+)"\)\)`)
	reFormTag       = regexp.MustCompile(`(?is)<form\b[^>]*>`)
	reFormAction    = regexp.MustCompile(`(?is)\baction\s*=\s*["']([^"']+)["']`)
)

func parseCaptchaHTML(pageHTML, pageURL string) (*captchaSSRData, string, error) {
	formAction, err := captchaFormAction(pageHTML, pageURL)
	if err != nil {
		return nil, "", err
	}

	if match := reSSRDataLegacy.FindStringSubmatch(pageHTML); len(match) > 1 {
		raw, err := base64.StdEncoding.DecodeString(match[1])
		if err != nil {
			return nil, "", fmt.Errorf("captcha: SSR_DATA base64: %w", err)
		}
		var legacy struct {
			UniqueKey string `json:"uniqueKey"`
			Pow       struct {
				Prefix     string `json:"prefix"`
				Complexity int    `json:"complexity"`
			} `json:"pow"`
			Timestamp int64 `json:"timestamp"`
		}
		if err := json.Unmarshal(raw, &legacy); err != nil {
			return nil, "", fmt.Errorf("captcha: SSR_DATA json: %w", err)
		}
		return &captchaSSRData{
			UniqueKey:     legacy.UniqueKey,
			PowPrefix:     legacy.Pow.Prefix,
			PowComplexity: legacy.Pow.Complexity,
			Timestamp:     legacy.Timestamp,
			Legacy:        true,
		}, formAction, nil
	}

	uniqueKey, _ := captchaJSStringField(pageHTML, "uniqueKey")
	powPrefix, _ := captchaJSStringField(pageHTML, "powPrefix")
	formActionValue, hasFormAction := captchaJSStringField(pageHTML, "formAction")
	if hasFormAction && formActionValue != "" {
		formAction, err = resolveURL(pageURL, formActionValue)
		if err != nil {
			return nil, "", fmt.Errorf("captcha: form action: %w", err)
		}
	}
	complexityText, _ := captchaJSStringField(pageHTML, "powComplexity")
	complexity, err := strconv.Atoi(complexityText)
	if err != nil {
		return nil, "", fmt.Errorf("captcha: powComplexity: %w", err)
	}
	timestampText, _ := captchaJSStringField(pageHTML, "timestamp")
	timestamp, _ := strconv.ParseInt(timestampText, 10, 64)
	if powPrefix == "" {
		return nil, "", fmt.Errorf("captcha: powPrefix not found")
	}
	return &captchaSSRData{
		UniqueKey:     uniqueKey,
		PowPrefix:     powPrefix,
		PowComplexity: complexity,
		Timestamp:     timestamp,
	}, formAction, nil
}

func captchaJSStringField(source, field string) (string, bool) {
	pattern := regexp.MustCompile(`(?:^|[,{}])\s*` + regexp.QuoteMeta(field) + `\s*:\s*("(?:\\.|[^"\\])*"|-?[0-9]+(?:\.[0-9]+)?)`)
	match := pattern.FindStringSubmatch(source)
	if len(match) < 2 {
		return "", false
	}
	if strings.HasPrefix(match[1], "\"") {
		value, err := strconv.Unquote(match[1])
		return value, err == nil
	}
	return match[1], true
}

func captchaFormAction(pageHTML, pageURL string) (string, error) {
	formTag := reFormTag.FindString(pageHTML)
	if formTag == "" {
		if action, ok := captchaJSStringField(pageHTML, "formAction"); ok {
			return resolveURL(pageURL, action)
		}
		return "", fmt.Errorf("captcha: form not found")
	}
	match := reFormAction.FindStringSubmatch(formTag)
	if len(match) < 2 {
		return "", fmt.Errorf("captcha: form action not found")
	}
	return resolveURL(pageURL, html.UnescapeString(match[1]))
}

func resolveURL(baseURL, ref string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed, err := base.Parse(ref)
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}

func originOf(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

func isCaptchaURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	path := strings.ToLower(u.Path)
	return strings.Contains(path, "showcaptcha") || strings.Contains(path, "checkcaptcha")
}

func looksLikeCaptchaHTML(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "captcha_smart") ||
		strings.Contains(lower, "showcaptcha") ||
		strings.Contains(lower, "checkcaptcha") ||
		strings.Contains(lower, "smartcaptcha") ||
		strings.Contains(lower, "are you not a robot")
}

func solveCaptchaPoW(ctx context.Context, prefixHex string, complexity int) (string, int, time.Duration, error) {
	if complexity < 0 || complexity > sha256.Size*8 {
		return "", 0, 0, fmt.Errorf("invalid complexity %d", complexity)
	}
	prefix, err := hexDecode(prefixHex)
	if err != nil || len(prefix) == 0 {
		prefix = []byte(prefixHex)
	}

	started := time.Now()
	var nonce [16]byte
	for attempts := 1; attempts < 10_000_000; attempts++ {
		if attempts%1024 == 0 {
			select {
			case <-ctx.Done():
				return "", attempts, time.Since(started), ctx.Err()
			default:
			}
		}
		binary.BigEndian.PutUint64(nonce[:8], uint64(time.Now().UnixMilli()))
		if _, err := rand.Read(nonce[8:]); err != nil {
			return "", attempts, time.Since(started), err
		}

		sum := captchaHash(prefix, nonce[:])
		if complexity == 0 || captchaCheckComplexity(sum[:], complexity) {
			return hexEncode(nonce[:]), attempts, time.Since(started), nil
		}
	}
	return "", 10_000_000, time.Since(started), fmt.Errorf("proof of work exhausted")
}

func captchaHash(prefix, nonce []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(prefix)
	_, _ = h.Write(nonce)
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func captchaCheckComplexity(hash []byte, complexity int) bool {
	if complexity < 0 || complexity > len(hash)*8 {
		return false
	}
	fullBytes := complexity / 8
	for _, b := range hash[:fullBytes] {
		if b != 0 {
			return false
		}
	}
	remaining := complexity % 8
	if remaining == 0 {
		return true
	}
	mask := byte(0xff) << (8 - remaining)
	return hash[fullBytes]&mask == 0
}

func buildLegacyCaptchaFingerprint(nonceHex, uniqueKey string, elapsed time.Duration) map[string]interface{} {
	end := float64(elapsed.Milliseconds())
	return map[string]interface{}{
		"start": 0.0,
		"end":   end,
		"factors": map[string]interface{}{
			"m10": map[string]interface{}{
				"value": nonceHex + ";133",
				"start": 0.0,
				"end":   end,
			},
		},
		"version":   "1.8.2",
		"uniqueKey": uniqueKey,
	}
}

func buildCaptchaFingerprint(nonceHex, userAgent string) map[string]interface{} {
	return map[string]interface{}{
		"b6": 8, "b7": 8, "b9": []string{"en-US", "en"},
		"c2": "", "c4": "MacIntel", "c5": []interface{}{}, "c9": userAgent,
		"f4": 1080, "f5": 1920, "f6": 24, "f7": 1080, "f8": true,
		"f9": []int{1920, 1080}, "g1": 1920,
		"g2": "Europe/Moscow", "g3": -180,
		"j5": true,
		"m2": map[string]interface{}{"mTP": 0, "tE": false, "tS": false},
		"n6": false,
		"o2": 0, "o3": "srgb", "o4": 0, "o5": "en-US",
		"o8": nil, "o9": nil,
		"p1": nil, "p2": 0, "p3": nil, "p4": nil,
		"p5": nil, "p6": nil, "p8": []interface{}{}, "p9": "111111111",
		"j6": 48000,
		"a1": "",
		"a2": map[string]interface{}{"w": false, "d": ""},
		"a3": map[string]interface{}{
			"acos": 1.4444399284962483, "asin": 0.12349655394506357,
			"atan": 0.4636476090008061, "cos": -0.8390715290095377,
			"exp": 2.718281828459045, "log1p": 2.3978952727983707,
			"sin": -0.9917788534431158, "tan": -0.23206847684369653,
		},
		"a4": map[string]interface{}{"minDelta": 0.1, "maxDelta": 1.2},
		"a5": nil,
		"k4": []interface{}{},
		"j1": map[string]interface{}{
			"vn": "WebKit", "vr": "WebKit WebGL", "vU": "",
			"r": "Mozilla", "rU": "", "sLV": "WebGL GLSL ES 1.0 (1.0)",
		},
		"j2": map[string]interface{}{
			"cA": []interface{}{}, "p": []interface{}{}, "sP": []interface{}{},
			"e": []interface{}{}, "eP": []interface{}{},
		},
		"m10":     nonceHex,
		"version": "1.5.0",
	}
}

func encodeCaptchaJSON(value interface{}) string {
	raw, _ := json.Marshal(value)
	return base64.StdEncoding.EncodeToString(raw)
}

func encodeCaptchaPoWData(prefix, nonce string, calcTime int64) string {
	return encodeCaptchaJSON(struct {
		PowNonce    string `json:"powNonce"`
		PowCalcTime int64  `json:"powCalcTime"`
		PowPrefix   string `json:"powPrefix"`
	}{nonce, calcTime, prefix})
}

func encodeCaptchaFingerprint(fp map[string]interface{}) string {
	raw, _ := json.Marshal(fp)
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(raw)
	_ = w.Close()
	return "~" + base64.StdEncoding.EncodeToString(buf.Bytes()) + "~"
}

func setBrowserHeaders(req *http.Request, userAgent string) {
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Sec-GPC", "1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Cache-Control", "no-cache")
}

func hexEncode(b []byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hex[v>>4]
		out[i*2+1] = hex[v&0x0f]
	}
	return string(out)
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, lo := hexVal(s[i*2]), hexVal(s[i*2+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("invalid hex")
		}
		out[i] = byte(hi<<4 | lo)
	}
	return out, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func shortStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
