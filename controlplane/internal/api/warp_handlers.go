package api

import (
	"log"
	"net/http"
	"sync"
	"time"

	"openflux-control/internal/store"
	"openflux-control/internal/warp"
)

type warpProfileResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	DeviceID  string    `json:"device_id"`
	Endpoint  string    `json:"endpoint"`
	AddressV4 string    `json:"address_v4"`
	AddressV6 string    `json:"address_v6,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	KeyCount  int       `json:"key_count"`
}

func warpProfileDTO(p store.WarpProfile) warpProfileResponse {
	return warpProfileResponse{ID: p.ID, Name: p.Name, DeviceID: p.DeviceID, Endpoint: p.Endpoint,
		AddressV4: p.AddressV4, AddressV6: p.AddressV6, CreatedAt: p.CreatedAt, KeyCount: p.KeyCount}
}

func (a *App) handleListWarp(w http.ResponseWriter, r *http.Request) {
	profiles, err := a.Store.ListWarpProfiles(r.Context())
	if err != nil {
		writeInternalError(w, r, "list warp profiles failed", err)
		return
	}
	out := make([]warpProfileResponse, 0, len(profiles))
	for _, p := range profiles {
		out = append(out, warpProfileDTO(p))
	}
	writeJSON(w, http.StatusOK, out)
}

// registerWarp opens a new WARP account at Cloudflare and keeps it, with the private key sealed like a key token.
func (a *App) registerWarp(r *http.Request, name string) (store.WarpProfile, error) {
	return a.registerWarpFor(r, name, nil)
}

func (a *App) registerWarpFor(r *http.Request, name string, nodeID *string) (store.WarpProfile, error) {
	acc, err := a.registerWarpAccount(r)
	if err != nil {
		return store.WarpProfile{}, err
	}
	enc, err := a.Cipher.Encrypt(acc.PrivateKey)
	if err != nil {
		return store.WarpProfile{}, err
	}
	return a.Store.CreateWarpProfile(r.Context(), store.WarpProfile{
		Name: name, DeviceID: acc.DeviceID, PrivateKeyEnc: enc, PeerPublicKey: acc.PeerPublicKey,
		Endpoint: acc.Endpoint, AddressV4: acc.AddressV4, AddressV6: acc.AddressV6, NodeID: nodeID,
	})
}

func (a *App) registerWarpAccount(r *http.Request) (warp.Account, error) {
	return warp.Register(r.Context(), nil)
}

type createWarpRequest struct {
	Name string `json:"name"`
}

func (a *App) handleCreateWarp(w http.ResponseWriter, r *http.Request) {
	var req createWarpRequest
	if r.ContentLength != 0 {
		if err := readJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
	}
	p, err := a.registerWarp(r, req.Name)
	if err != nil {
		writeError(w, http.StatusBadGateway, "could not register a WARP account: "+err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, warpProfileDTO(p))
}

func (a *App) handleDeleteWarp(w http.ResponseWriter, r *http.Request) {
	if err := a.Store.DeleteWarpProfile(r.Context(), r.PathValue("id")); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "warp profile not found")
		return
	} else if err != nil {
		writeInternalError(w, r, "delete warp profile failed", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type patchKeyWarpRequest struct {
	Enabled   bool    `json:"enabled"`
	ProfileID *string `json:"profile_id"`
}

// handlePatchKeyWarp sends one key's exit traffic through WARP. Without a profile_id the first existing profile is used, and one is registered when none exists, so the admin never has to fetch a config by hand.
func (a *App) handlePatchKeyWarp(w http.ResponseWriter, r *http.Request) {
	var req patchKeyWarpRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	var profileID *string
	if req.Enabled {
		profileID = req.ProfileID
		if profileID == nil {
			profiles, err := a.Store.ListWarpProfiles(r.Context())
			if err != nil {
				writeInternalError(w, r, "list warp profiles failed", err)
				return
			}
			if len(profiles) > 0 {
				profileID = &profiles[0].ID
			} else {
				p, err := a.registerWarp(r, "default")
				if err != nil {
					writeError(w, http.StatusBadGateway, "could not register a WARP account: "+err.Error())
					return
				}
				profileID = &p.ID
			}
		}
	}

	k, err := a.Store.SetKeyWarp(r.Context(), r.PathValue("id"), profileID)
	switch err {
	case nil:
		writeJSON(w, http.StatusOK, k)
	case store.ErrNotFound:
		writeError(w, http.StatusNotFound, "key or warp profile not found")
	case store.ErrWarpWithCascade:
		writeError(w, http.StatusConflict, "this key already cascades through a final-exit node - clear that first")
	default:
		writeInternalError(w, r, "set key warp failed", err)
	}
}

var nodeWarpMu sync.Mutex

// ensureNodeWarp gives a node that runs WARP keys its own Cloudflare identity. Two nodes on one identity take turns losing every packet, so identities are never shared. Until it exists the node is not handed those keys, so they stay down instead of leaving from the node's own address.
func (a *App) ensureNodeWarp(r *http.Request, nodeID string) {
	nodeWarpMu.Lock()
	defer nodeWarpMu.Unlock()
	need, err := a.Store.NodeNeedsWarp(r.Context(), nodeID)
	if err != nil || !need {
		return
	}
	if _, err := a.registerWarpFor(r, "node-"+nodeID[:8], &nodeID); err != nil {
		log.Printf("[WARP] could not register an identity for node %s: %v", nodeID, err)
	}
}
