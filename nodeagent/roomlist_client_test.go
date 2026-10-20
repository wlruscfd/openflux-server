package nodeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestReportRoomListPostsWhereTheControlPlaneListens guards the seam between
// the node and the control plane.
//
// The control plane serves the room list report at
// /v1/nodes/me/keys/{keyID}/room-list. A path that does not match it comes
// back 404, the report is lost, and the key keeps an empty doc_url so the
// client has no document to open.
func TestReportRoomListPostsWhereTheControlPlaneListens(t *testing.T) {
	var gotPath string
	var gotBody roomListBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"changed":true,"doc_url":"room-a,room-b"}`))
	}))
	defer srv.Close()

	client := NewControlClient(srv.URL, "node-token")
	changed, err := client.ReportRoomList(context.Background(), "7d721764-5a56-4bd0-8b4d-3aacdb3611c4", "room-a,room-b")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("the control plane reported no change")
	}

	want := "/v1/nodes/me/keys/7d721764-5a56-4bd0-8b4d-3aacdb3611c4/room-list"
	if gotPath != want {
		t.Fatalf("posted to %q want %q", gotPath, want)
	}
	if gotBody.RoomList != "room-a,room-b" {
		t.Fatalf("room list %q", gotBody.RoomList)
	}
}

type roomListBody struct {
	RoomList string `json:"room_list"`
}
