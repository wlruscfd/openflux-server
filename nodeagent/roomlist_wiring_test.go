package nodeagent

import (
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/transport/cupsonline"
)

// TestCupsTransportStillCarriesItsRoomList guards the chain that publishes a
// cupsonline key's rooms to the control plane.
//
// The nodeagent builds the transport the way startWorker does and then asks it
// for a room list. The exit creates the rooms itself and reports them through
// OnRoomList, so a wrapper that hides those two methods leaves the key with an
// empty doc_url and the client with nothing to open. The wrapper is what broke
// it: it embeds the Transport interface, which promotes the interface's
// methods and not the room-list ones.
func TestCupsTransportStillCarriesItsRoomList(t *testing.T) {
	trans := transport.Transport(transport.NewBatchedTransport(cupsonline.NewCupsonlineTransport("", transport.DefaultConfig(), false)))

	roomLister, ok := trans.(interface {
		OnRoomList(func(string))
		RoomList() string
	})
	if !ok {
		t.Fatal("the batched transport hides the room list, so the exit never publishes its rooms")
	}

	var reported string
	var called bool
	roomLister.OnRoomList(func(list string) {
		called = true
		reported = list
	})

	if roomLister.RoomList() != "" {
		t.Fatalf("RoomList() = %q before any room was created", roomLister.RoomList())
	}
	if called {
		t.Fatal("a room list was reported before the transport created any room")
	}
	_ = reported
}
