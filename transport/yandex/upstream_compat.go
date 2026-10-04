package yandex

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

var ErrCaptchaRequired = errors.New("yandex docs: captcha required")

var ErrLoginRequired = errors.New("yandex docs: login required")

func siteCookies(u *url.URL, values map[string]string) []*http.Cookie {
	domain := ""
	if u != nil {
		if labels := strings.Split(u.Hostname(), "."); len(labels) >= 3 {
			domain = strings.Join(labels[1:], ".")
		}
	}
	cookies := make([]*http.Cookie, 0, len(values))
	for k, v := range values {
		cookies = append(cookies, &http.Cookie{Name: k, Value: v, Path: "/", Domain: domain})
	}
	return cookies
}

func solveCaptchaProtected(docURL string, jar http.CookieJar, userAgent string) (string, error) {
	rt := &http.Transport{DialContext: transport.ProtectedDialer().DialContext}
	return solveCaptcha(docURL, jar, userAgent, rt)
}

func (t *YandexDocsTransport) ApplyCookies(values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	pairs := make([]string, 0, len(values))
	for name, value := range values {
		pairs = append(pairs, name+"="+value)
	}
	t.ProvideCookies(strings.Join(pairs, "; "))
	return nil
}

func (t *YandexDocsTransport) FetchCookies() (map[string]string, error) {
	out := make(map[string]string)
	for _, c := range parseCookieHeader(t.getProvidedCookies()) {
		out[c.Name] = c.Value
	}
	return out, nil
}

func (t *BoardsTransport) ApplyCookies(values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	pairs := make([]string, 0, len(values))
	for name, value := range values {
		pairs = append(pairs, name+"="+value)
	}
	t.ProvideCookies(strings.Join(pairs, "; "))
	return nil
}

func (t *BoardsTransport) FetchCookies() (map[string]string, error) {
	out := make(map[string]string)
	for _, c := range parseCookieHeader(t.getProvidedCookies()) {
		out[c.Name] = c.Value
	}
	return out, nil
}
