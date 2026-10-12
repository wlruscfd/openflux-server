package api

import (
	"net/http"
	"strings"

	"openflux-control/internal/model"
	"openflux-control/internal/store"
)

type nodeKeyResponse struct {
	ID                string   `json:"id"`
	DocURL            string   `json:"doc_url"`
	DocURLs           []string `json:"doc_urls,omitempty"`
	Transport         string   `json:"transport"`
	TrafficLimitBytes *int64   `json:"traffic_limit_bytes,omitempty"`
	BytesUsedTotal    int64    `json:"bytes_used_total"`
	// Token is the raw key token, decrypted here since only a node-authenticated request reaches this handler, so the worker can derive the same end-to-end key the client used.
	Token string `json:"token,omitempty"`
	// E2EEncryption is binding: the node wraps the worker's transport in EncryptedTransport if and only if this is true - previously the node guessed per-peer, making the panel's toggle purely advisory.
	E2EEncryption bool `json:"e2e_encryption,omitempty"`
	// RelayHost/RelayPort are set only when this key is cascaded - present means "relay to this address instead of dialing the real internet yourself".
	RelayHost *string `json:"relay_host,omitempty"`
	RelayPort *int    `json:"relay_port,omitempty"`
	// Warp present means this key's exit dials the internet through that WireGuard peer instead of from the node's own address.
	Warp *nodeWarpResponse `json:"warp,omitempty"`
}

type nodeWarpResponse struct {
	ProfileID     string `json:"profile_id"`
	PrivateKey    string `json:"private_key"`
	PeerPublicKey string `json:"peer_public_key"`
	Endpoint      string `json:"endpoint"`
	AddressV4     string `json:"address_v4"`
	AddressV6     string `json:"address_v6,omitempty"`
}

func (a *App) handleNodeListKeys(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())

	keys, err := a.Store.ListActiveKeysForNode(r.Context(), n.ID)
	if err != nil {
		writeInternalError(w, r, "list keys failed", err)
		return
	}

	out := make([]nodeKeyResponse, 0, len(keys))
	for _, k := range keys {
		var token string
		if len(k.TokenEnc) > 0 {
			if t, err := a.Cipher.Decrypt(k.TokenEnc); err == nil {
				token = t
			}
		}
		var warpCfg *nodeWarpResponse
		if k.Warp != nil {
			if priv, err := a.Cipher.Decrypt(k.Warp.PrivateKeyEnc); err == nil {
				warpCfg = &nodeWarpResponse{ProfileID: k.Warp.ProfileID, PrivateKey: priv, PeerPublicKey: k.Warp.PeerPublicKey,
					Endpoint: k.Warp.Endpoint, AddressV4: k.Warp.AddressV4, AddressV6: k.Warp.AddressV6}
			}
		}
		out = append(out, nodeKeyResponse{
			ID:                k.ID,
			DocURL:            k.DocURL,
			DocURLs:           k.DocURLs,
			Transport:         k.Transport,
			TrafficLimitBytes: k.TrafficLimitBytes,
			BytesUsedTotal:    k.BytesSentTotal + k.BytesReceivedTotal,
			Token:             token,
			E2EEncryption:     k.E2EEncryption,
			RelayHost:         k.RelayHost,
			RelayPort:         k.RelayPort,
			Warp:              warpCfg,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

type relayExitKeyResponse struct {
	ID string `json:"id"`
	// Token derives the relay link's own encryption key (see transport.NewUDPRelayTransport) - this node never sees doc_url/doc_urls, since it never talks to Yandex.
	Token     string `json:"token"`
	RelayPort int    `json:"relay_port"`
}

// handleNodeListRelayKeys is the final-exit half of a cascade: keys whose final_exit_node_id is this node - it opens relay_port and, once a relayed packet decrypts, runs a normal raw-mode exit for it exactly like handleNodeListKeys's keys, just fed by the relay link instead of Yandex.
func (a *App) handleNodeListRelayKeys(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())

	keys, err := a.Store.ListActiveRelayExitKeysForNode(r.Context(), n.ID)
	if err != nil {
		writeInternalError(w, r, "list relay keys failed", err)
		return
	}

	out := make([]relayExitKeyResponse, 0, len(keys))
	for _, k := range keys {
		var token string
		if len(k.TokenEnc) > 0 {
			if t, err := a.Cipher.Decrypt(k.TokenEnc); err == nil {
				token = t
			}
		}
		if token == "" {
			continue
		}
		out = append(out, relayExitKeyResponse{ID: k.ID, Token: token, RelayPort: k.RelayPort})
	}
	writeJSON(w, http.StatusOK, out)
}

type usageDeltaRequest struct {
	KeyID              string `json:"key_id"`
	BytesSentDelta     int64  `json:"bytes_sent_delta"`
	BytesReceivedDelta int64  `json:"bytes_received_delta"`
}

type reportUsageRequest struct {
	Deltas []usageDeltaRequest `json:"deltas"`
}

type roomListRequest struct {
	RoomList string `json:"room_list"`
}

const maxRoomListBytes = 8 * 1024

func (a *App) handleNodeRoomList(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())

	var req roomListRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	list := strings.TrimSpace(req.RoomList)
	if list == "" {
		writeError(w, http.StatusBadRequest, "room_list must not be empty")
		return
	}
	if len(list) > maxRoomListBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "room list too large")
		return
	}

	k, err := a.Store.GetKeyByID(r.Context(), r.PathValue("id"))
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, "get key failed", err)
		return
	}
	if k.AssignedNodeID == nil || *k.AssignedNodeID != n.ID {
		writeError(w, http.StatusForbidden, "key is not assigned to this node")
		return
	}
	if k.Transport != transportCupsOnline {
		writeError(w, http.StatusBadRequest, "room lists are only for cupsonline keys")
		return
	}

	changed, err := a.Store.SetKeyDocURLFromNode(r.Context(), k.ID, list)
	if err != nil {
		writeInternalError(w, r, "store room list failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"changed": changed, "doc_url": list})
}

func (a *App) handleNodeUsage(w http.ResponseWriter, r *http.Request) {
	var req reportUsageRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if len(req.Deltas) > 1000 {
		writeError(w, http.StatusBadRequest, "too many deltas in one report (max 1000)")
		return
	}

	deltas := make([]model.UsageDelta, 0, len(req.Deltas))
	for _, d := range req.Deltas {
		if d.KeyID == "" {
			continue
		}
		deltas = append(deltas, model.UsageDelta{
			KeyID:              d.KeyID,
			BytesSentDelta:     d.BytesSentDelta,
			BytesReceivedDelta: d.BytesReceivedDelta,
		})
	}

	disabledNow, err := a.Store.ApplyUsageDeltas(r.Context(), deltas)
	if err != nil {
		writeInternalError(w, r, "apply usage failed", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string][]string{"disabled_now": disabledNow})
}

func (a *App) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	n := nodeFromContext(r.Context())

	if err := a.Store.TouchNodeHeartbeat(r.Context(), n.ID); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "node not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "heartbeat failed", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
