package nodeagent

import (
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

// The client builds its mts transport through mobile.wrapGeneric, which wraps it in a
// CompressedTransport. If the exit side ever stops doing the same, every packet reaches the
// tunnel with the compressor's marker byte still on the front, the IPv4 header sits at offset 1,
// and the tunnel drops it silently. That fault cost a day to find and produced a channel that
// looked alive while carrying nothing.
func TestMTSExitTransportIsWrappedLikeTheClient(t *testing.T) {
	tr := newMTSExitTransport("https://doski.mts-link.ru/boards/board/a569c522-dd03-4073-89f6-70d61957719c")

	if _, ok := tr.(*transport.CompressedTransport); !ok {
		t.Fatalf("mts exit transport is %T, want *transport.CompressedTransport to match the client", tr)
	}
}
