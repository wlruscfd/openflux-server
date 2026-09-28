package api

import (
	"errors"
	"net/http"
	"strings"

	"openflux-control/internal/auth"
	"openflux-control/internal/store"
)

type postCookiesRequest struct {
	Cookies string `json:"cookies"`
}

type postCookiesResponse struct {
	Status string `json:"status"`
}

const maxCookieBytes = 32 * 1024

// handlePostKeyCookies lets the client that owns a key push a cookie jar the controlplane then
// hands to the node running that key. This is the direct path for a provider check the client
// can pass (it has a browser) but the exit node cannot: no relaying through another tunnel.
func (a *App) handlePostKeyCookies(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.ExtractBearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	k, err := a.Store.GetKeyByTokenHash(r.Context(), a.Hasher.Hash(token))
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "lookup failed", err)
		return
	}

	var req postCookiesRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	jar := strings.TrimSpace(req.Cookies)
	if jar == "" {
		if err := a.Store.DeleteKeyCookies(r.Context(), k.ID); err != nil {
			writeInternalError(w, r, "clear cookies failed", err)
			return
		}
		writeJSON(w, http.StatusOK, postCookiesResponse{Status: "cleared"})
		return
	}
	if len(jar) > maxCookieBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "cookie jar too large")
		return
	}

	enc, err := a.Cipher.Encrypt(jar)
	if err != nil {
		writeInternalError(w, r, "cookie encryption failed", err)
		return
	}
	if err := a.Store.SetKeyCookies(r.Context(), k.ID, enc); err != nil {
		writeInternalError(w, r, "store cookies failed", err)
		return
	}
	writeJSON(w, http.StatusOK, postCookiesResponse{Status: "stored"})
}

type nodeKeyCookieResponse struct {
	KeyID     string `json:"key_id"`
	Transport string `json:"transport"`
	Cookies   string `json:"cookies"`
}

// handleNodeListCookies serves the jars for the keys this node actually runs, decrypted here
// because only a node-authenticated request reaches it. Kept off the key listing so the hot
// reconcile path is unchanged.
func (a *App) handleNodeListCookies(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())

	rows, err := a.Store.ListNodeKeyCookies(r.Context(), n.ID)
	if err != nil {
		writeInternalError(w, r, "list cookies failed", err)
		return
	}

	out := make([]nodeKeyCookieResponse, 0, len(rows))
	for _, c := range rows {
		jar, err := a.Cipher.Decrypt(c.CookiesEnc)
		if err != nil || len(jar) == 0 {
			continue
		}
		out = append(out, nodeKeyCookieResponse{KeyID: c.KeyID, Transport: c.Transport, Cookies: string(jar)})
	}
	writeJSON(w, http.StatusOK, out)
}

type keyCookieStatusResponse struct {
	HasCookies bool    `json:"has_cookies"`
	UpdatedAt  *string `json:"updated_at,omitempty"`
}

// handleKeyCookieStatus deliberately reports only whether a jar exists: the secret stays out
// of the admin key listing, and the panel has no reason to see it.
func (a *App) handleKeyCookieStatus(w http.ResponseWriter, r *http.Request) {
	has, updated, err := a.Store.KeyCookieStatus(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "cookie status failed", err)
		return
	}
	resp := keyCookieStatusResponse{HasCookies: has}
	if updated != nil {
		s := updated.Format("2006-01-02T15:04:05Z07:00")
		resp.UpdatedAt = &s
	}
	writeJSON(w, http.StatusOK, resp)
}
