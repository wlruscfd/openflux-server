package tunnel

import (
	"log"
	"sync"
	"sync/atomic"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/p1neappleXpress/OpenFlux/network"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// defaultMTU matches a full Ethernet frame; SetMTU overrides it to match whatever the real path (a mobile network, a constrained TUN device) can actually carry, so gvisor doesn't build segments this link then can't deliver.
const defaultMTU = 1500

type TunnelLinkEndpoint struct {
	dispatcherMu     sync.RWMutex
	dispatcher       stack.NetworkDispatcher
	onOutgoingPacket func([]byte)
	packetIn         atomic.Uint64
	packetOut        atomic.Uint64
	mtu              atomic.Uint32
}

func NewTunnelLinkEndpoint() *TunnelLinkEndpoint {
	e := &TunnelLinkEndpoint{}
	e.mtu.Store(defaultMTU)
	return e
}

func (e *TunnelLinkEndpoint) PacketCounts() (in, out uint64) {
	return e.packetIn.Load(), e.packetOut.Load()
}

func (e *TunnelLinkEndpoint) SetOutgoingPacketHandler(fn func([]byte)) {
	e.onOutgoingPacket = fn
}

func (e *TunnelLinkEndpoint) InjectInbound(data []byte) {
	e.packetIn.Add(1)
	if utils.IsVerbose() {
		utils.Debugf("<- %d bytes - %s\n", len(data), network.ParsePacketInfo(data))
	}

	// dispatcher is a plain interface value; reading it unsynchronized with Attach's write is a real data race that let a nil dispatcher slip through and crash.
	e.dispatcherMu.RLock()
	dispatcher := e.dispatcher
	e.dispatcherMu.RUnlock()
	if dispatcher == nil {
		return
	}

	// Non-TCP/UDP packets (e.g. ICMP) have no registered handler and nil-pointer-panic the NAT/forwarding code - confirmed in production from a client's OS routing a ping into the tunnel's default route.
	if len(data) < 20 || (data[9] != 6 && data[9] != 17) {
		return
	}
	// Same class of bug fixed in rawsocket_linux.go's reader: a declared IHL bigger than the actual buffer (corrupt data, or a batch/regex decode that let non-packet bytes through) crashes gvisor's own parsing instead of erroring.
	if ihl := int(data[0]&0x0F) * 4; ihl < 20 || ihl > len(data) {
		return
	}

	// gvisor panics on some inputs instead of erroring (seen in production: "unexpected transport protocol = 0"); this runs on a shared goroutine so an unrecovered panic takes the whole process down.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TUNNEL] recovered from a panic dispatching an inbound packet (%d bytes, %s): %v",
				len(data), network.ParsePacketInfo(data), r)
		}
	}()

	pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buffer.MakeWithData(data),
	})
	dispatcher.DeliverNetworkPacket(ipv4.ProtocolNumber, pkt)
}

func (e *TunnelLinkEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	n := 0
	for _, pkt := range pkts.AsSlice() {
		data := pkt.ToView().ToSlice()
		e.packetOut.Add(1)
		if e.onOutgoingPacket != nil {
			e.onOutgoingPacket(data)
		}
		n++
	}
	return n, nil
}

func (e *TunnelLinkEndpoint) MTU() uint32                    { return e.mtu.Load() }
func (e *TunnelLinkEndpoint) MaxHeaderLength() uint16        { return 0 }
func (e *TunnelLinkEndpoint) LinkAddress() tcpip.LinkAddress { return "\x02\x00\x00\x00\x00\x01" }
func (e *TunnelLinkEndpoint) Capabilities() stack.LinkEndpointCapabilities {
	return stack.CapabilityNone
}
func (e *TunnelLinkEndpoint) Attach(dispatcher stack.NetworkDispatcher) {
	e.dispatcherMu.Lock()
	e.dispatcher = dispatcher
	e.dispatcherMu.Unlock()
}
func (e *TunnelLinkEndpoint) IsAttached() bool {
	e.dispatcherMu.RLock()
	defer e.dispatcherMu.RUnlock()
	return e.dispatcher != nil
}
func (e *TunnelLinkEndpoint) Wait()                                   {}
func (e *TunnelLinkEndpoint) ARPHardwareType() header.ARPHardwareType { return header.ARPHardwareNone }
func (e *TunnelLinkEndpoint) AddHeader(*stack.PacketBuffer)           {}
func (e *TunnelLinkEndpoint) Close()                                  {}
func (e *TunnelLinkEndpoint) SetMTU(mtu uint32) {
	if mtu > 0 {
		e.mtu.Store(mtu)
	}
}
func (e *TunnelLinkEndpoint) SetLinkAddress(tcpip.LinkAddress)     {}
func (e *TunnelLinkEndpoint) ParseHeader(*stack.PacketBuffer) bool { return true }
func (e *TunnelLinkEndpoint) SetOnCloseAction(func())              {}
