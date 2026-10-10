package api

import (
	"net/http"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/store"
)

type resolveResponse struct {
	Status            string   `json:"status"`
	DocURL            string   `json:"doc_url,omitempty"`
	DocURLs           []string `json:"doc_urls,omitempty"`
	Transport         string   `json:"transport,omitempty"`
	E2EEncryption     bool     `json:"e2e_encryption,omitempty"`
	BytesUsedTotal    int64    `json:"bytes_used_total"`
	TrafficLimitBytes *int64   `json:"traffic_limit_bytes,omitempty"`
}

// handleResolve limits unknown tokens per IP (guessing) and known keys per key, so many users behind one NAT address do not share a budget.
func (a *App) handleResolve(w http.ResponseWriter, r *http.Request) {
	token, ok := auth.ExtractBearer(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return
	}

	k, err := a.Store.GetKeyByTokenHash(r.Context(), a.Hasher.Hash(token))
	if err == store.ErrNotFound {
		if !a.limiter.allow(clientIP(r)) {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		writeJSON(w, http.StatusOK, resolveResponse{Status: "not_found"})
		return
	}
	if err != nil {
		writeInternalError(w, r, "lookup failed", err)
		return
	}
	if !a.keyLimiter.allow(k.ID) {
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	status := k.Status(time.Now())
	resp := resolveResponse{
		Status:            status,
		BytesUsedTotal:    k.BytesUsedTotal(),
		TrafficLimitBytes: k.TrafficLimitBytes,
	}
	if status == "active" {
		resp.DocURL = k.DocURL
		resp.DocURLs = k.DocURLs
		resp.Transport = k.Transport
		resp.E2EEncryption = k.E2EEncryption
	}

	writeJSON(w, http.StatusOK, resp)
}
