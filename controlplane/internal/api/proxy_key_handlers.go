package api

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"openflux-control/internal/store"
)

type proxyKeyResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Kind      string    `json:"kind"`
	Server    string    `json:"server"`
	Port      int       `json:"port"`
	Priority  int       `json:"priority"`
	Enabled   bool      `json:"enabled"`
	Carriers  []string  `json:"carriers"`
	CreatedAt time.Time `json:"created_at"`
}

func proxyKeyDTO(p store.ProxyKey) proxyKeyResponse {
	if p.Carriers == nil {
		p.Carriers = []string{}
	}
	return proxyKeyResponse{ID: p.ID, Name: p.Name, Kind: p.Kind, Server: p.Server, Port: p.Port,
		Priority: p.Priority, Enabled: p.Enabled, Carriers: p.Carriers, CreatedAt: p.CreatedAt}
}

type nodeProxyKeyResponse struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Link     string   `json:"link"`
	Kind     string   `json:"kind"`
	Priority int      `json:"priority"`
	Carriers []string `json:"carriers"`
}

// knownCarriers is the set the panel offers; the node's carrier list is validated against it so a
// typo cannot create a pool nothing ever dials.
var knownCarriers = map[string]bool{
	"yandex": true, "yandex_multistream": true, "boards": true, "mailru": true,
	"mts": true, "telemost": true, "cupsonline": true, "avito": true, "oneme": true,
}

func parseProxyLink(link string) (kind, server string, port int, name string, err error) {
	link = strings.TrimSpace(link)
	if link == "" {
		return "", "", 0, "", fmt.Errorf("empty link")
	}
	u, err := url.Parse(link)
	if err != nil {
		return "", "", 0, "", fmt.Errorf("link is not a URL")
	}
	switch u.Scheme {
	case "ss":
		kind = "shadowsocks"
	case "vless":
		kind = "vless"
	case "hysteria2", "hy2":
		kind = "hysteria2"
	default:
		return "", "", 0, "", fmt.Errorf("scheme %q is not supported, use ss://, vless:// or hysteria2://", u.Scheme)
	}
	server = u.Hostname()
	port, err = strconv.Atoi(u.Port())
	if err != nil {
		return "", "", 0, "", fmt.Errorf("the link needs an explicit port")
	}
	name = u.Fragment
	if i := strings.IndexByte(name, '?'); i >= 0 {
		name = name[:i]
	}
	if _, _, err := net.SplitHostPort(net.JoinHostPort(server, strconv.Itoa(port))); err != nil {
		return "", "", 0, "", fmt.Errorf("bad server address")
	}
	return kind, server, port, name, nil
}

func cleanCarriers(in []string) ([]string, error) {
	out := []string{}
	seen := map[string]bool{}
	for _, c := range in {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if !knownCarriers[c] {
			return nil, fmt.Errorf("unknown carrier %q", c)
		}
		if seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	return out, nil
}

type createProxyKeyRequest struct {
	Link     string   `json:"link"`
	Name     string   `json:"name"`
	Priority int      `json:"priority"`
	Carriers []string `json:"carriers"`
	Enabled  *bool    `json:"enabled"`
}

func (a *App) handleCreateProxyKey(w http.ResponseWriter, r *http.Request) {
	var req createProxyKeyRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	kind, server, port, name, err := parseProxyLink(req.Link)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	carriers, err := cleanCarriers(req.Carriers)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.checkCarrierRoom(w, r, carriers, ""); err != nil {
		return
	}
	if strings.TrimSpace(req.Name) != "" {
		name = strings.TrimSpace(req.Name)
	}
	priority := req.Priority
	if priority <= 0 {
		priority = 1
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	enc, err := a.Cipher.Encrypt(strings.TrimSpace(req.Link))
	if err != nil {
		writeInternalError(w, r, "encrypt proxy link failed", err)
		return
	}
	p, err := a.Store.CreateProxyKey(r.Context(), store.ProxyKey{
		Name: name, LinkEnc: enc, Kind: kind, Server: server, Port: port,
		Priority: priority, Enabled: enabled, Carriers: carriers,
	})
	if err != nil {
		writeInternalError(w, r, "create proxy key failed", err)
		return
	}
	writeJSON(w, http.StatusCreated, proxyKeyDTO(p))
}

// maxProxyKeys is how many keys one carrier may carry. A node opens a local listener and an
// outbound per key, and the public key lists people paste in run to thousands of entries, most of
// them dead long before they are ever dialled.
const maxProxyKeys = 64

// checkCarrierRoom keeps a carrier from collecting more keys than a node can carry. excludeID is the
// key being edited, so replacing a key never trips the limit against itself.
func (a *App) checkCarrierRoom(w http.ResponseWriter, r *http.Request, carriers []string, excludeID string) error {
	existing, err := a.Store.ListProxyKeys(r.Context())
	if err != nil {
		writeInternalError(w, r, "list proxy keys failed", err)
		return err
	}
	counts := map[string]int{}
	for _, p := range existing {
		if p.ID == excludeID {
			continue
		}
		for _, c := range p.Carriers {
			counts[c]++
		}
	}
	for _, c := range carriers {
		if counts[c] >= maxProxyKeys {
			writeError(w, http.StatusConflict,
				fmt.Sprintf("carrier %s already has %d keys, the limit is %d", c, counts[c], maxProxyKeys))
			return fmt.Errorf("carrier %s is full", c)
		}
	}
	return nil
}

type bulkImportProxyKeysRequest struct {
	Links    string   `json:"links"`
	Carriers []string `json:"carriers"`
	Priority int      `json:"priority"`
}

// maxBulkImport bounds one paste. A public key list runs to thousands of entries and almost all of
// them are dead; importing more than a carrier can hold would only crowd out the few that work.
const maxBulkImport = maxProxyKeys

// handleBulkImportProxyKeys takes a whole pasted list at once, one link per line, and reports what it
// stored and what it had to drop. Imports are partial on purpose: one malformed line does not throw
// away the other thousand, but nothing is silently swallowed either.
func (a *App) handleBulkImportProxyKeys(w http.ResponseWriter, r *http.Request) {
	var req bulkImportProxyKeysRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	carriers, err := cleanCarriers(req.Carriers)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	priority := req.Priority
	if priority <= 0 {
		priority = 1
	}
	lines := strings.Split(req.Links, "\n")
	created := 0
	rejected := 0
	malformed := 0
	var firstErrors []string
	for _, raw := range lines {
		link := strings.TrimSpace(raw)
		if link == "" || strings.HasPrefix(link, "#") {
			continue
		}
		if created >= maxBulkImport {
			rejected++
			continue
		}
		if err := a.checkCarrierRoom(w, r, carriers, ""); err != nil {
			writeJSON(w, http.StatusOK, bulkImportResponse{Created: created, Rejected: rejected,
				Malformed: malformed, Errors: firstErrors, Full: true})
			return
		}
		kind, server, port, name, err := parseProxyLink(link)
		if err != nil {
			malformed++
			if len(firstErrors) < 5 {
				firstErrors = append(firstErrors, err.Error())
			}
			continue
		}
		enc, err := a.Cipher.Encrypt(link)
		if err != nil {
			malformed++
			if len(firstErrors) < 5 {
				firstErrors = append(firstErrors, "could not seal the link")
			}
			continue
		}
		if _, err := a.Store.CreateProxyKey(r.Context(), store.ProxyKey{
			Name: name, LinkEnc: enc, Kind: kind, Server: server, Port: port,
			Priority: priority, Enabled: true, Carriers: carriers,
		}); err != nil {
			rejected++
			continue
		}
		created++
	}
	writeJSON(w, http.StatusOK, bulkImportResponse{Created: created, Rejected: rejected,
		Malformed: malformed, Errors: firstErrors})
}

type bulkImportResponse struct {
	Created   int      `json:"created"`
	Rejected  int      `json:"rejected"`
	Malformed int      `json:"malformed"`
	Errors    []string `json:"errors,omitempty"`
	Full      bool     `json:"full,omitempty"`
}

func (a *App) handleListProxyKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := a.Store.ListProxyKeys(r.Context())
	if err != nil {
		writeInternalError(w, r, "list proxy keys failed", err)
		return
	}
	out := make([]proxyKeyResponse, 0, len(keys))
	for _, p := range keys {
		out = append(out, proxyKeyDTO(p))
	}
	writeJSON(w, http.StatusOK, out)
}

type patchProxyKeyRequest struct {
	Name     *string   `json:"name"`
	Priority *int      `json:"priority"`
	Enabled  *bool     `json:"enabled"`
	Carriers *[]string `json:"carriers"`
	Link     *string   `json:"link"`
}

func (a *App) handlePatchProxyKey(w http.ResponseWriter, r *http.Request) {
	var req patchProxyKeyRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	id := r.PathValue("id")
	existing, err := a.Store.GetProxyKey(r.Context(), id)
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "proxy key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "get proxy key failed", err)
		return
	}
	kind, server, port, name := existing.Kind, existing.Server, existing.Port, existing.Name
	if req.Link != nil {
		k, s, p, n, err := parseProxyLink(*req.Link)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		kind, server, port, name = k, s, p, n
		enc, err := a.Cipher.Encrypt(strings.TrimSpace(*req.Link))
		if err != nil {
			writeInternalError(w, r, "encrypt proxy link failed", err)
			return
		}
		if err := a.Store.SetProxyKeyLink(r.Context(), id, enc); err != nil {
			writeInternalError(w, r, "update proxy link failed", err)
			return
		}
	}
	if req.Name != nil {
		name = strings.TrimSpace(*req.Name)
	}
	priority := existing.Priority
	if req.Priority != nil && *req.Priority > 0 {
		priority = *req.Priority
	}
	enabled := existing.Enabled
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if err := a.Store.UpdateProxyKey(r.Context(), id, name, kind, server, port, priority, enabled); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "proxy key not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "update proxy key failed", err)
		return
	}
	if req.Carriers != nil {
		carriers, err := cleanCarriers(*req.Carriers)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := a.checkCarrierRoom(w, r, carriers, id); err != nil {
			return
		}
		if err := a.Store.SetProxyKeyCarriers(r.Context(), id, carriers); err != nil {
			writeInternalError(w, r, "set proxy key carriers failed", err)
			return
		}
	}
	updated, err := a.Store.GetProxyKey(r.Context(), id)
	if err != nil {
		writeInternalError(w, r, "get proxy key failed", err)
		return
	}
	writeJSON(w, http.StatusOK, proxyKeyDTO(updated))
}

func (a *App) handleDeleteProxyKey(w http.ResponseWriter, r *http.Request) {
	if err := a.Store.DeleteProxyKey(r.Context(), r.PathValue("id")); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "proxy key not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "delete proxy key failed", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleNodeListProxyKeys serves the keys this node balances over, decrypted here because only a
// node-authenticated request reaches it. Kept off the key listing so the hot reconcile path is unchanged.
func (a *App) handleNodeListProxyKeys(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())
	keys, err := a.Store.ListProxyKeysForNode(r.Context())
	if err != nil {
		writeInternalError(w, r, "list proxy keys failed", err)
		return
	}
	assignments, err := a.Store.LoadProxyAssignments(r.Context(), n.ID)
	if err != nil {
		writeInternalError(w, r, "load proxy assignments failed", err)
		return
	}
	out := make([]nodeProxyKeyResponse, 0, len(keys))
	for _, k := range keys {
		link, err := a.Cipher.Decrypt(k.LinkEnc)
		if err != nil {
			continue
		}
		if k.Carriers == nil {
			k.Carriers = []string{}
		}
		out = append(out, nodeProxyKeyResponse{ID: k.ID, Name: k.Name, Link: link,
			Kind: k.Kind, Priority: k.Priority, Carriers: k.Carriers})
	}
	writeJSON(w, http.StatusOK, nodeProxyKeysPayload{Keys: out, Assignments: assignments})
}

type nodeProxyKeysPayload struct {
	Keys        []nodeProxyKeyResponse `json:"keys"`
	Assignments map[string]string      `json:"assignments,omitempty"`
}

type nodeAssignmentsRequest struct {
	Assignments map[string]string `json:"assignments"`
}

func (a *App) handleNodeSaveAssignments(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())
	var req nodeAssignmentsRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if len(req.Assignments) > 5000 {
		writeError(w, http.StatusBadRequest, "too many assignments")
		return
	}
	if err := a.Store.SaveProxyAssignments(r.Context(), n.ID, req.Assignments); err != nil {
		writeInternalError(w, r, "save proxy assignments failed", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}