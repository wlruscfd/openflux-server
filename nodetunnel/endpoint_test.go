package nodetunnel

import (
	"testing"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

func TestInjectInboundBeforeAttachDoesNotPanic(t *testing.T) {
	e := NewTunnelLinkEndpoint()
	e.InjectInbound([]byte{0x45, 0x00, 0x00, 0x14})

	if in, _ := e.PacketCounts(); in != 1 {
		t.Errorf("PacketCounts().in = %d, want 1 (still counted, just not dispatched)", in)
	}
}

func TestAttachNilDetachesCleanly(t *testing.T) {
	e := NewTunnelLinkEndpoint()
	e.Attach(fakeDispatcher{})
	if !e.IsAttached() {
		t.Fatalf("expected IsAttached() after Attach(non-nil)")
	}

	e.Attach(nil)
	if e.IsAttached() {
		t.Errorf("expected !IsAttached() after Attach(nil)")
	}

	// Must not panic even though a dispatcher was attached a moment ago.
	e.InjectInbound([]byte{0x45, 0x00, 0x00, 0x14})
}

func TestPacketCounts(t *testing.T) {
	e := NewTunnelLinkEndpoint()
	if in, out := e.PacketCounts(); in != 0 || out != 0 {
		t.Fatalf("PacketCounts() on a fresh endpoint = (%d, %d), want (0, 0)", in, out)
	}

	e.InjectInbound([]byte{0x45, 0x00})
	e.InjectInbound([]byte{0x45, 0x00})
	if in, _ := e.PacketCounts(); in != 2 {
		t.Errorf("PacketCounts().in = %d, want 2", in)
	}
}

// TestInjectInboundRecoversFromDispatcherPanic guards a real production incident: gvisor's NAT/conntrack code panics on some packets, which used to take down the whole exit-node process.
func TestInjectInboundRecoversFromDispatcherPanic(t *testing.T) {
	e := NewTunnelLinkEndpoint()
	e.Attach(panickingDispatcher{})

	e.InjectInbound([]byte{0x45, 0x00, 0x00, 0x14}) // must not panic

	if in, _ := e.PacketCounts(); in != 1 {
		t.Errorf("PacketCounts().in = %d, want 1", in)
	}
}

type fakeDispatcher struct{}

func (fakeDispatcher) DeliverNetworkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer) {}
func (fakeDispatcher) DeliverLinkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer)    {}

type panickingDispatcher struct{}

func (panickingDispatcher) DeliverNetworkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer) {
	panic("unexpected transport protocol = 0")
}
func (panickingDispatcher) DeliverLinkPacket(tcpip.NetworkProtocolNumber, *stack.PacketBuffer) {}
