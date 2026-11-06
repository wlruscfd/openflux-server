package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"openflux-control/internal/store"
)

var cookieNameRe = regexp.MustCompile("^[!#$%&'*+\\-.^_`|~0-9A-Za-z]+$")

type cookiePair struct{ name, value string }

func normalizeAvitoCookies(input string) (string, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return "", errors.New("cookies are empty")
	}
	if len(s) > maxCookieBytes {
		return "", errors.New("cookies are too large")
	}
	var pairs []cookiePair
	var err error
	switch {
	case strings.HasPrefix(s, "["):
		pairs, err = cookiesFromJSON(s)
	case strings.Contains(s, "\t"):
		pairs = cookiesFromNetscape(s)
	default:
		pairs = cookiesFromHeader(s)
	}
	if err != nil {
		return "", err
	}
	byName := make(map[string]int, len(pairs))
	out := make([]cookiePair, 0, len(pairs))
	for _, p := range pairs {
		if !cookieNameRe.MatchString(p.name) {
			return "", fmt.Errorf("cookie name %q is not valid", p.name)
		}
		if strings.ContainsFunc(p.value, func(r rune) bool { return r == ';' || (r < 0x20 && r != '\t') || r == 0x7f }) {
			return "", fmt.Errorf("cookie %q has a value that cannot be sent", p.name)
		}
		if i, ok := byName[p.name]; ok {
			out[i] = p
			continue
		}
		byName[p.name] = len(out)
		out = append(out, p)
	}
	if len(out) == 0 {
		return "", errors.New("no cookies found: paste the Cookie header, a cookies.txt or a JSON export")
	}
	parts := make([]string, len(out))
	for i, p := range out {
		parts[i] = p.name + "=" + p.value
	}
	return strings.Join(parts, "; "), nil
}

func cookiesFromHeader(s string) []cookiePair {
	s = strings.TrimSpace(s)
	for _, prefix := range []string{"Cookie:", "cookie:"} {
		s = strings.TrimSpace(strings.TrimPrefix(s, prefix))
	}
	var out []cookiePair
	for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ';' || r == '\n' || r == '\r' }) {
		part = strings.TrimSpace(part)
		if name, value, ok := strings.Cut(part, "="); ok && strings.TrimSpace(name) != "" {
			out = append(out, cookiePair{strings.TrimSpace(name), value})
		}
	}
	return out
}

func cookiesFromNetscape(s string) []cookiePair {
	var out []cookiePair
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(strings.Replace(line, "#HttpOnly_", "", 1))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) >= 7 {
			out = append(out, cookiePair{strings.TrimSpace(fields[5]), strings.TrimSpace(fields[6])})
		}
	}
	return out
}

func cookiesFromJSON(s string) ([]cookiePair, error) {
	var items []struct {
		Name   string `json:"name"`
		Value  string `json:"value"`
		Domain string `json:"domain"`
	}
	if err := json.Unmarshal([]byte(s), &items); err != nil {
		return nil, errors.New("cookies look like JSON but cannot be read")
	}
	var out []cookiePair
	for _, it := range items {
		if it.Domain != "" && !strings.Contains(it.Domain, "avito") {
			continue
		}
		out = append(out, cookiePair{it.Name, it.Value})
	}
	return out, nil
}

type setCookiesRequest struct {
	Cookies string `json:"cookies"`
}

func (a *App) handleAdminSetKeyCookies(w http.ResponseWriter, r *http.Request) {
	k, err := a.Store.GetKeyByID(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "get key failed", err)
		return
	}
	if k.Transport != transportAvito {
		writeError(w, http.StatusBadRequest, "cookies can only be attached to avito keys")
		return
	}
	var req setCookiesRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	jar, err := normalizeAvitoCookies(req.Cookies)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	enc, err := a.Cipher.Encrypt(jar)
	if err != nil {
		writeInternalError(w, r, "cookie encryption failed", err)
		return
	}
	if err := a.Store.SetKeyCookies(r.Context(), k.ID, enc, ""); err != nil {
		writeInternalError(w, r, "store cookies failed", err)
		return
	}
	writeJSON(w, http.StatusOK, postCookiesResponse{Status: "stored"})
}

func (a *App) handleAdminDeleteKeyCookies(w http.ResponseWriter, r *http.Request) {
	if err := a.Store.DeleteKeyCookies(r.Context(), r.PathValue("id")); err != nil {
		writeInternalError(w, r, "clear cookies failed", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) fillHasCookies(r *http.Request, ids []string) (map[string]bool, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return a.Store.KeyIDsWithCookies(r.Context(), ids)
}
