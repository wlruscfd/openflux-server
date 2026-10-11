package api

import (
	"net/http"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/store"
)

type createKeyRequest struct {
	Label             string     `json:"label"`
	Transport         string     `json:"transport"`
	DocURL            string     `json:"doc_url"`
	DocURLs           []string   `json:"doc_urls"`
	E2EEncryption     bool       `json:"e2e_encryption"`
	TrafficLimitBytes *int64     `json:"traffic_limit_bytes"`
	OwnerRef          string     `json:"owner_ref"`
	ExpiresAt         *time.Time `json:"expires_at"`
}

const transportYandexMultistream = "yandex_multistream"

const transportCupsOnline = "cupsonline"

func transportTakesDocument(transport string) bool { return transport != transportCupsOnline }

type createdKey struct {
	ID       string `json:"id"`
	Token    string `json:"token"`
	DeepLink string `json:"deep_link,omitempty"`
}

func createKeyHandler(a *App, w http.ResponseWriter, r *http.Request, forcedOwnerRef string) {
	var reqs []createKeyRequest

	var single createKeyRequest
	var batch []createKeyRequest

	body, err := peekJSONShape(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	if body.isArray {
		if err := readJSON(r, &batch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		reqs = batch
	} else {
		if err := readJSON(r, &single); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		reqs = []createKeyRequest{single}
	}

	if len(reqs) == 0 {
		writeError(w, http.StatusBadRequest, "at least one key is required")
		return
	}
	if len(reqs) > 200 {
		writeError(w, http.StatusBadRequest, "too many keys in one request (max 200)")
		return
	}

	// Tracks every URL claimed earlier in this batch, since HasEnabledKeyWithAnyDocURL only sees keys already committed - two entries in one bulk import sharing a URL would otherwise both pass the check.
	urlsInBatch := make(map[string]bool, len(reqs))

	created := make([]createdKey, 0, len(reqs))
	for _, req := range reqs {
		if req.Transport == "" {
			req.Transport = "yandex"
		}

		var urls []string
		if req.Transport == transportYandexMultistream {
			if len(req.DocURLs) < 2 {
				writeError(w, http.StatusBadRequest, "doc_urls needs 2+ entries for transport=yandex_multistream")
				return
			}
			urls = req.DocURLs
			req.DocURL = "" // ignore a stray doc_url - doc_urls is authoritative for this transport
		} else if !transportTakesDocument(req.Transport) {
			// cupsonline's exit creates its own rooms, so the key carries no document and the
			// doc_url uniqueness check below would only ever compare empty strings.
			urls = nil
			req.DocURLs = nil
		} else {
			if req.DocURL == "" {
				writeError(w, http.StatusBadRequest, "doc_url is required for every key")
				return
			}
			urls = []string{req.DocURL}
			req.DocURLs = nil
		}

		// A new key is enabled immediately, so the doc_url collision check runs at creation time too, and a per-entry set catches a URL duplicated within one entry's own doc_urls list.
		urlSet := make(map[string]bool, len(urls))
		for _, u := range urls {
			if urlsInBatch[u] || urlSet[u] {
				writeError(w, http.StatusConflict, "doc_url is used by more than one key in this request - each key needs its own Yandex Docs document")
				return
			}
			urlSet[u] = true
		}
		for _, u := range urls {
			urlsInBatch[u] = true
		}
		ownerRef := req.OwnerRef
		if forcedOwnerRef != "" {
			ownerRef = forcedOwnerRef
		}

		token, err := auth.GenerateToken("key")
		if err != nil {
			writeInternalError(w, r, "token generation failed", err)
			return
		}
		tokenEnc, err := a.Cipher.Encrypt(token)
		if err != nil {
			writeInternalError(w, r, "token encryption failed", err)
			return
		}

		k, err := a.Store.CreateKeyGuarded(r.Context(), store.CreateKeyParams{
			TokenHash:         a.Hasher.Hash(token),
			TokenEnc:          tokenEnc,
			Label:             req.Label,
			Transport:         req.Transport,
			DocURL:            req.DocURL,
			DocURLs:           req.DocURLs,
			E2EEncryption:     req.E2EEncryption,
			TrafficLimitBytes: req.TrafficLimitBytes,
			OwnerRef:          ownerRef,
			ExpiresAt:         req.ExpiresAt,
		}, urls)
		if err == store.ErrDocURLInUse {
			writeError(w, http.StatusConflict, "doc_url is already used by another enabled key - each key needs its own Yandex Docs document")
			return
		}
		if err != nil {
			writeInternalError(w, r, "create key failed", err)
			return
		}

		created = append(created, createdKey{
			ID:       k.ID,
			Token:    token,
			DeepLink: buildDeepLink(a.Config.PublicBaseURL, req.Label, token, req.DocURL, req.DocURLs, req.Transport, req.E2EEncryption),
		})
	}

	if len(created) == 1 {
		writeJSON(w, http.StatusCreated, created[0])
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (a *App) handleIngestKeys(w http.ResponseWriter, r *http.Request) {
	it := ingestTokenFromContext(r.Context())
	if it.Scope != "keys:write" {
		writeError(w, http.StatusForbidden, "ingest token missing keys:write scope")
		return
	}
	createKeyHandler(a, w, r, "")
}
