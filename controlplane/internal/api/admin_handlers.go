package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"openflux-control/internal/auth"
	"openflux-control/internal/model"
	"openflux-control/internal/store"
)

type createNodeRequest struct {
	Name    string `json:"name"`
	MaxKeys int    `json:"max_keys"`
}

type createNodeResponse struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	MaxKeys int    `json:"max_keys"`
	Token   string `json:"token"`
}

func (a *App) handleCreateNode(w http.ResponseWriter, r *http.Request) {
	var req createNodeRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.MaxKeys <= 0 {
		req.MaxKeys = 999999 // no real limit by default
	}

	token, err := auth.GenerateToken("node")
	if err != nil {
		writeInternalError(w, r, "token generation failed", err)
		return
	}

	n, err := a.Store.CreateNode(r.Context(), req.Name, a.Hasher.Hash(token), req.MaxKeys)
	if err != nil {
		writeInternalError(w, r, "create node failed", err)
		return
	}

	writeJSON(w, http.StatusCreated, createNodeResponse{ID: n.ID, Name: n.Name, MaxKeys: n.MaxKeys, Token: token})
}

func (a *App) handleListNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.Store.ListNodes(r.Context())
	if err != nil {
		writeInternalError(w, r, "list nodes failed", err)
		return
	}
	if nodes == nil {
		nodes = []model.Node{}
	}
	writeJSON(w, http.StatusOK, nodes)
}

type patchNodeRequest struct {
	MaxKeys       *int    `json:"max_keys,omitempty"`
	PublicAddress *string `json:"public_address,omitempty"`
}

func (a *App) handlePatchNode(w http.ResponseWriter, r *http.Request) {
	var req patchNodeRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	id := r.PathValue("id")

	if req.MaxKeys != nil {
		if *req.MaxKeys <= 0 {
			writeError(w, http.StatusBadRequest, "max_keys must be positive")
			return
		}
		if err := a.Store.SetNodeMaxKeys(r.Context(), id, *req.MaxKeys); err == store.ErrNotFound {
			writeError(w, http.StatusNotFound, "node not found")
			return
		} else if err != nil {
			writeInternalError(w, r, "update node failed", err)
			return
		}
	}

	if req.PublicAddress != nil {
		if err := a.Store.SetNodePublicAddress(r.Context(), id, *req.PublicAddress); err == store.ErrNotFound {
			writeError(w, http.StatusNotFound, "node not found")
			return
		} else if err != nil {
			writeInternalError(w, r, "update node failed", err)
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (a *App) handleRotateNodeToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	token, err := auth.GenerateToken("node")
	if err != nil {
		writeInternalError(w, r, "token generation failed", err)
		return
	}

	if err := a.Store.RotateNodeSecret(r.Context(), id, a.Hasher.Hash(token)); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "node not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "rotate failed", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

type createIngestTokenRequest struct {
	Label string `json:"label"`
	Scope string `json:"scope"`
}

func (a *App) handleCreateIngestToken(w http.ResponseWriter, r *http.Request) {
	var req createIngestTokenRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if req.Scope == "" {
		req.Scope = "keys:write"
	}

	token, err := auth.GenerateToken("ingest")
	if err != nil {
		writeInternalError(w, r, "token generation failed", err)
		return
	}

	it, err := a.Store.CreateIngestToken(r.Context(), a.Hasher.Hash(token), req.Label, req.Scope)
	if err != nil {
		writeInternalError(w, r, "create ingest token failed", err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id": it.ID, "label": it.Label, "scope": it.Scope, "token": token,
	})
}

func (a *App) handleListIngestTokens(w http.ResponseWriter, r *http.Request) {
	tokens, err := a.Store.ListIngestTokens(r.Context())
	if err != nil {
		writeInternalError(w, r, "list ingest tokens failed", err)
		return
	}
	if tokens == nil {
		tokens = []model.IngestToken{}
	}
	writeJSON(w, http.StatusOK, tokens)
}

func (a *App) handleSetIngestTokenEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := a.Store.SetIngestTokenEnabled(r.Context(), r.PathValue("id"), enabled); err == store.ErrNotFound {
			writeError(w, http.StatusNotFound, "ingest token not found")
			return
		} else if err != nil {
			writeInternalError(w, r, "update ingest token failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"enabled": enabled})
	}
}

func (a *App) handleListKeys(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	keys, err := a.Store.ListKeys(r.Context(), store.ListKeysFilter{
		OwnerRef: q.Get("owner_ref"),
		Limit:    limit,
		Offset:   offset,
	})
	if err != nil {
		writeInternalError(w, r, "list keys failed", err)
		return
	}
	if keys == nil {
		keys = []model.Key{}
	}
	writeJSON(w, http.StatusOK, keys)
}

func (a *App) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	createKeyHandler(a, w, r, "")
}

type rotateKeyTokenResponse struct {
	Token    string `json:"token"`
	DeepLink string `json:"deep_link,omitempty"`
}

// handleRotateKeyToken exists because the original token is only ever stored hashed - there's no other way to get a usable token for a key whose raw token is gone.
func (a *App) handleRotateKeyToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

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

	k, err := a.Store.RotateKeyToken(r.Context(), id, a.Hasher.Hash(token), tokenEnc)
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "rotate key token failed", err)
		return
	}

	writeJSON(w, http.StatusOK, rotateKeyTokenResponse{
		Token:    token,
		DeepLink: buildDeepLink(a.Config.PublicBaseURL, k.Label, token, k.DocURL, k.DocURLs, k.Transport, k.E2EEncryption),
	})
}

func (a *App) handleGetKey(w http.ResponseWriter, r *http.Request) {
	k, err := a.Store.GetKeyByID(r.Context(), r.PathValue("id"))
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "get key failed", err)
		return
	}
	writeJSON(w, http.StatusOK, k)
}

// patchKeyRequest is a partial update: an absent field is left alone, while an explicit null
// clears the nullable ones (traffic limit, expiry, final exit). That distinction is why the
// nullable fields are pointers-to-pointers instead of plain pointers.
type patchKeyRequest struct {
	Label             *string   `json:"label"`
	Transport         *string   `json:"transport"`
	DocURL            *string   `json:"doc_url"`
	DocURLs           *[]string `json:"doc_urls"`
	E2EEncryption     *bool     `json:"e2e_encryption"`
	TrafficLimitBytes *int64    `json:"traffic_limit_bytes"`
	OwnerRef          *string   `json:"owner_ref"`
	ExpiresAt         *string   `json:"expires_at"`
	FinalExitNodeID   *string   `json:"final_exit_node_id"`
	Enabled           *bool     `json:"enabled"`
}

func (r patchKeyRequest) params() (store.UpdateKeyParams, error) {
	p := store.UpdateKeyParams{
		Label:         r.Label,
		Transport:     r.Transport,
		DocURL:        r.DocURL,
		DocURLs:       r.DocURLs,
		E2EEncryption: r.E2EEncryption,
		OwnerRef:      r.OwnerRef,
		Enabled:       r.Enabled,
	}
	if r.Label != nil && strings.TrimSpace(*r.Label) == "" {
		return p, errors.New("label must not be empty")
	}
	if r.Transport != nil && strings.TrimSpace(*r.Transport) == "" {
		return p, errors.New("transport must not be empty")
	}
	if r.DocURL != nil && strings.TrimSpace(*r.DocURL) == "" {
		return p, errors.New("doc_url must not be empty")
	}
	if r.DocURLs != nil {
		cleaned := make([]string, 0, len(*r.DocURLs))
		for _, u := range *r.DocURLs {
			if u = strings.TrimSpace(u); u != "" {
				cleaned = append(cleaned, u)
			}
		}
		if len(cleaned) == 0 {
			return p, errors.New("doc_urls must not be empty")
		}
		p.DocURLs = &cleaned
	}
	if r.TrafficLimitBytes != nil {
		if *r.TrafficLimitBytes < 0 {
			return p, errors.New("traffic_limit_bytes must not be negative")
		}
		p.TrafficLimitBytes = &store.OptionalInt64{Set: true, Value: r.TrafficLimitBytes}
	}
	if r.ExpiresAt != nil {
		var expires *time.Time
		if *r.ExpiresAt != "" {
			parsed, err := time.Parse(time.RFC3339, *r.ExpiresAt)
			if err != nil {
				return p, errors.New("expires_at must be RFC3339 or null")
			}
			expires = &parsed
		}
		p.ExpiresAt = &expires
	}
	if r.FinalExitNodeID != nil {
		var nodeID *string
		if *r.FinalExitNodeID != "" {
			v := *r.FinalExitNodeID
			nodeID = &v
		}
		p.FinalExitNodeID = &nodeID
	}
	return p, nil
}

func (a *App) handlePatchKey(w http.ResponseWriter, r *http.Request) {
	var req patchKeyRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	params, err := req.params()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	k, err := a.Store.UpdateKeyGuarded(r.Context(), r.PathValue("id"), params)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "key not found")
		return
	case errors.Is(err, store.ErrDocURLInUse):
		writeError(w, http.StatusConflict, "doc_url is already used by another enabled key - each key needs its own document")
		return
	case errors.Is(err, store.ErrNoFinalExitAddress):
		writeError(w, http.StatusConflict, "that node has no public_address set yet - add one before using it as a final exit")
		return
	case err != nil:
		writeInternalError(w, r, "update key failed", err)
		return
	}

	writeJSON(w, http.StatusOK, k)
}

type patchKeyFinalExitRequest struct {
	FinalExitNodeID *string `json:"final_exit_node_id"`
}

// handlePatchKeyFinalExit builds (or tears down) a cascade for one key: set final_exit_node_id to
// route this key's traffic through a second node's raw exit instead of the assigned node dialing
// the real internet itself; null clears it back to direct-exit.
func (a *App) handlePatchKeyFinalExit(w http.ResponseWriter, r *http.Request) {
	var req patchKeyFinalExitRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	k, err := a.Store.SetKeyFinalExit(r.Context(), r.PathValue("id"), req.FinalExitNodeID)
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key or final-exit node not found")
		return
	}
	if err == store.ErrNoFinalExitAddress {
		writeError(w, http.StatusConflict, "that node has no public_address set yet - add one before using it as a final exit")
		return
	}
	if err != nil {
		writeInternalError(w, r, "set key final exit failed", err)
		return
	}
	writeJSON(w, http.StatusOK, k)
}

func (a *App) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	if err := a.Store.DeleteKey(r.Context(), r.PathValue("id")); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "delete key failed", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) handleSetKeyEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		// Only re-enabling needs the doc_url check: a disabled key isn't running a worker and can't collide with anything, and disabling never changes its URLs.
		if enabled {
			k, err := a.Store.GetKeyByID(r.Context(), id)
			if err == store.ErrNotFound {
				writeError(w, http.StatusNotFound, "key not found")
				return
			} else if err != nil {
				writeInternalError(w, r, "get key failed", err)
				return
			}
			urls := k.DocURLs
			if len(urls) == 0 {
				urls = []string{k.DocURL}
			}
			// Guarded: holds a DB-level advisory lock on urls for the check and the enable together, so two concurrent re-enable requests can't both pass the check before either commits.
			err = a.Store.SetKeyEnabledGuarded(r.Context(), id, urls)
			if err == store.ErrDocURLInUse {
				writeError(w, http.StatusConflict, "doc_url is already used by another enabled key - each key needs its own Yandex Docs document")
				return
			} else if err == store.ErrNotFound {
				writeError(w, http.StatusNotFound, "key not found")
				return
			} else if err != nil {
				writeInternalError(w, r, "update key failed", err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]bool{"enabled": enabled})
			return
		}

		if err := a.Store.SetKeyEnabled(r.Context(), id, enabled); err == store.ErrNotFound {
			writeError(w, http.StatusNotFound, "key not found")
			return
		} else if err != nil {
			writeInternalError(w, r, "update key failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"enabled": enabled})
	}
}
