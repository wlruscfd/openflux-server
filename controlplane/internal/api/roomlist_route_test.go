package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRoomListRouteServesThePathNodesPostTo guards the seam between the node
// and the control plane.
//
// The node reports a key's rooms to /v1/nodes/me/keys/{keyID}/room-list. The
// route used to be /v1/nodes/{id}/room-list, which no node ever called, so
// every report came back 404 and the key kept an empty doc_url.
func TestRoomListRouteServesThePathNodesPostTo(t *testing.T) {
	router := (&App{}).Router()

	req := httptest.NewRequest(http.MethodPost, "/v1/nodes/me/keys/7d721764-5a56-4bd0-8b4d-3aacdb3611c4/room-list",
		strings.NewReader(`{"room_list":"room-a,room-b"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatal("the route a node posts a room list to is not served")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d want %d, the request without a token must not reach the handler", rec.Code, http.StatusUnauthorized)
	}
}

// TestRoomListRouteDoesNotPutTheKeyIdWhereTheNodeIdGoes guards the wildcard.
//
// The node token middleware reads the "id" path value as the node's own id, so
// a route that puts the key id in that slot answers a valid report with
// "node token does not match node id". The key id belongs in its own value.
func TestRoomListRouteDoesNotPutTheKeyIdWhereTheNodeIdGoes(t *testing.T) {
	router := (&App{}).Router()

	req := httptest.NewRequest(http.MethodPost, "/v1/nodes/7d721764-5a56-4bd0-8b4d-3aacdb3611c4/room-list",
		strings.NewReader(`{"room_list":"room-a,room-b"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d want %d, the key id must not be routed as a node id", rec.Code, http.StatusNotFound)
	}
}
