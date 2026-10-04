package nodetunnel

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/p1neappleXpress/OpenFlux/network"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// rawSocketFwMark tags every packet this process sends via the raw socket - the deploy's iptables RST-drop rule matches on its absence, not on ours (see newRawSocketCore). Must match install.sh's rule exactly.
const rawSocketFwMark = 0x2547

// rawSocketCore is shared process-wide, not per-worker: a SOCK_RAW socket sees every matching packet regardless of destination port, so N independent readLoops would each parse and mostly discard a full copy of every packet.
type rawSocketCore struct {
	sendFd, recvFd, recvUDPFd int
	localIP                   [4]byte

	outgoingSYNs sync.Map // seq uint32 -> time.Time (sent-at, swept by sweepStaleSYNs)
	activePorts  sync.Map // port uint16 -> activePortBinding
	portGen      atomic.Uint64
}

// activePortBinding pairs the owning worker with a generation tag so a delayed FIN/RST eviction can tell "still this connection" from "this port got reused since" and skip the delete in the latter case.
type activePortBinding struct {
	owner *RawSocketEndpoint
	gen   uint64
}

// portCloseGrace: TCP allows half-close - the side that sent FIN can still be waiting on the peer's own data/FIN - so evicting the port the instant we relay our own FIN/RST out would drop a real, still-expected reply as "port not active". Delaying the evict gives that tail traffic a window to still get through.
const portCloseGrace = 2 * time.Second

// syn2Ack is how long an outgoing SYN waits for a reply before sweepStaleSYNs treats it as dead - a refused (RST) or blackholed connect never hits the SYN-ACK delete path, so without this the map leaks one entry per failed dial forever, node-wide.
const synStaleAfter = 30 * time.Second

func (c *rawSocketCore) sweepStaleSYNs() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-synStaleAfter)
		c.outgoingSYNs.Range(func(key, value any) bool {
			if sentAt, ok := value.(time.Time); ok && sentAt.Before(cutoff) {
				c.outgoingSYNs.Delete(key)
			}
			return true
		})
	}
}

var (
	sharedRawCoreOnce sync.Once
	sharedRawCore     *rawSocketCore
	sharedRawCoreErr  error
)

// rawReaderGoroutines is how many goroutines concurrently call Recvfrom on the same shared raw
// socket per protocol - multiple readers on one fd is safe (the kernel hands each blocking call a
// distinct queued packet, same as any other socket type) and is what actually lets per-packet work
// (header parse, checksum, dispatch) use more than one CPU core: a single reader serializes every
// key's traffic through it regardless of core count, which becomes the throughput ceiling for the
// whole node once enough keys are active to saturate one core - the very thing this shared-core
// design was supposed to scale past. TCP handles any resulting reordering the same way it already
// handles real network jitter, so this doesn't trade correctness for throughput.
func rawReaderGoroutines() int {
	n := runtime.NumCPU()
	if n < 2 {
		return 2
	}
	if n > 8 {
		return 8
	}
	return n
}

func getSharedRawCore() (*rawSocketCore, error) {
	sharedRawCoreOnce.Do(func() {
		sharedRawCore, sharedRawCoreErr = newRawSocketCore()
		if sharedRawCoreErr == nil {
			n := rawReaderGoroutines()
			for i := 0; i < n; i++ {
				go sharedRawCore.readLoop(sharedRawCore.recvFd, 6)
			}
			for i := 0; i < n; i++ {
				go sharedRawCore.readLoop(sharedRawCore.recvUDPFd, 17)
			}
			go sharedRawCore.sweepStaleSYNs()
		}
	})
	return sharedRawCore, sharedRawCoreErr
}

func newRawSocketCore() (*rawSocketCore, error) {
	sendFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_RAW)
	if err != nil {
		return nil, fmt.Errorf("send socket failed: %v (need root)", err)
	}

	if err := syscall.SetsockoptInt(sendFd, syscall.IPPROTO_IP, syscall.IP_HDRINCL, 1); err != nil {
		syscall.Close(sendFd)
		return nil, fmt.Errorf("IP_HDRINCL: %v", err)
	}
	// fwMark lets the deploy's iptables RST-drop rule (aimed at the kernel's own auto-RST for a raw socket it doesn't own a real connection for) exempt packets we send ourselves - without this, gvisor's own legitimate RSTs get silently EPERM'd too, leaving the real peer thinking the connection is still open.
	if err := syscall.SetsockoptInt(sendFd, syscall.SOL_SOCKET, syscall.SO_MARK, rawSocketFwMark); err != nil {
		syscall.Close(sendFd)
		return nil, fmt.Errorf("SO_MARK: %v", err)
	}

	recvFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_TCP)
	if err != nil {
		syscall.Close(sendFd)
		return nil, fmt.Errorf("recv socket failed: %v (need root)", err)
	}

	addr := &syscall.SockaddrInet4{
		Addr: [4]byte{0, 0, 0, 0},
		Port: 0,
	}
	if err := syscall.Bind(recvFd, addr); err != nil {
		syscall.Close(sendFd)
		syscall.Close(recvFd)
		return nil, fmt.Errorf("bind failed: %v", err)
	}

	// recvUDPFd is a second receive socket for UDP since IPPROTO_TCP alone never delivered UDP replies (sending uses IPPROTO_RAW, which isn't limited to one protocol).
	recvUDPFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_UDP)
	if err != nil {
		syscall.Close(sendFd)
		syscall.Close(recvFd)
		return nil, fmt.Errorf("udp recv socket failed: %v (need root)", err)
	}
	if err := syscall.Bind(recvUDPFd, addr); err != nil {
		syscall.Close(sendFd)
		syscall.Close(recvFd)
		syscall.Close(recvUDPFd)
		return nil, fmt.Errorf("udp bind failed: %v", err)
	}

	core := &rawSocketCore{
		sendFd:    sendFd,
		recvFd:    recvFd,
		recvUDPFd: recvUDPFd,
	}
	fmt.Sscanf(getLocalIP(), "%d.%d.%d.%d", &core.localIP[0], &core.localIP[1], &core.localIP[2], &core.localIP[3])
	return core, nil
}

// The minimum length is protocol-dependent (40 for TCP+IP, 28 for UDP+IP); a single threshold of 40 would silently drop short datagrams like a 30-byte DNS reply.
func (c *rawSocketCore) readLoop(fd int, wantProto byte) {
	buf := make([]byte, 65535)
	minLen := 40
	if wantProto == 17 {
		minLen = 28
	}

	for {
		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err != nil {
			switch err {
			case syscall.EAGAIN: // == EWOULDBLOCK on Linux; listing both is a duplicate-case compile error
				time.Sleep(10 * time.Millisecond)
				continue
			case syscall.EINTR:
				// SIGURG goroutine-preemption can interrupt this blocking syscall harmlessly; returning here (as this used to) silently killed traffic for every key on the node at once.
				continue
			default:
				// Any other read error is also retried, not returned, since giving up here is process-wide, not per-key; always logged since this otherwise vanishes without --debug.
				log.Printf("[RAW/%d] read error, retrying: %v", wantProto, err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
		if n < minLen {
			continue
		}

		protocol := buf[9]

		if protocol == wantProto && bytes.Equal(buf[16:20], c.localIP[:]) {
			ipHeaderLenIn := int(buf[0]&0x0F) * 4
			// This raw socket sees every TCP/UDP packet on the host, not just tunnel traffic - a
			// stray or spoofed packet declaring an IHL bigger than what actually arrived would
			// otherwise panic the slice below and take down the whole process, every key at once.
			if ipHeaderLenIn < 20 || n-ipHeaderLenIn < 20 {
				continue
			}
			l4In := buf[ipHeaderLenIn:n]
			dstPort := uint16(l4In[2])<<8 | uint16(l4In[3])

			v, active := c.activePorts.Load(dstPort)
			if !active {
				continue
			}
			owner := v.(activePortBinding).owner

			if protocol == 6 && l4In[13] == 0x12 {
				ackNum := uint32(l4In[8])<<24 | uint32(l4In[9])<<16 | uint32(l4In[10])<<8 | uint32(l4In[11])
				synSeq := ackNum - 1

				if _, ok := c.outgoingSYNs.Load(synSeq); !ok {
					continue
				}
				c.outgoingSYNs.Delete(synSeq)
			}

			pktCopy := make([]byte, n)
			copy(pktCopy, buf[:n])

			copy(pktCopy[16:20], []byte{10, 10, 10, 2})

			pktCopy[10] = 0
			pktCopy[11] = 0
			ipChecksumVal := network.IPChecksum(pktCopy[:20])
			pktCopy[10] = byte(ipChecksumVal >> 8)
			pktCopy[11] = byte(ipChecksumVal & 0xFF)

			ipHeaderLen := int(pktCopy[0]&0x0F) * 4
			srcIPBytes := [4]byte{pktCopy[12], pktCopy[13], pktCopy[14], pktCopy[15]}
			dstIPBytes := [4]byte{pktCopy[16], pktCopy[17], pktCopy[18], pktCopy[19]}
			rewriteL4Checksum(pktCopy[ipHeaderLen:], protocol, srcIPBytes, dstIPBytes)

			owner.packetIn.Add(1)
			owner.mu.Lock()
			cb := owner.sendToTransport
			owner.mu.Unlock()
			if cb != nil {
				cb(pktCopy)
			}
		}
	}
}

type RawSocketEndpoint struct {
	core       *rawSocketCore
	dispatcher stack.NetworkDispatcher
	nicID      tcpip.NICID
	packetIn   atomic.Uint64
	packetOut  atomic.Uint64

	mu              sync.Mutex
	sendToTransport func([]byte)
}

func NewRawSocketEndpoint(nicID tcpip.NICID) (*RawSocketEndpoint, error) {
	core, err := getSharedRawCore()
	if err != nil {
		return nil, err
	}
	return &RawSocketEndpoint{core: core, nicID: nicID}, nil
}

func (e *RawSocketEndpoint) SetTransportSender(sendFunc func([]byte)) {
	e.mu.Lock()
	e.sendToTransport = sendFunc
	e.mu.Unlock()
}

func (e *RawSocketEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	n := 0
	for _, pkt := range pkts.AsSlice() {
		ipPacket := pkt.ToView().ToSlice()
		// 28 = IP(20)+UDP(8): the threshold follows the shortest protocol carried; the former 40 (IP+TCP) would silently drop short client datagrams.
		if len(ipPacket) < 28 {
			continue
		}

		pktCopy := make([]byte, len(ipPacket))
		copy(pktCopy, ipPacket)

		copy(pktCopy[12:16], e.core.localIP[:])

		pktCopy[10] = 0
		pktCopy[11] = 0
		ipChecksumVal := network.IPChecksum(pktCopy[:20])
		pktCopy[10] = byte(ipChecksumVal >> 8)
		pktCopy[11] = byte(ipChecksumVal & 0xFF)

		proto := pktCopy[9]
		ipHeaderLen := int(pktCopy[0]&0x0F) * 4
		l4 := pktCopy[ipHeaderLen:]
		srcIPBytes := [4]byte{pktCopy[12], pktCopy[13], pktCopy[14], pktCopy[15]}
		dstIPBytes := [4]byte{pktCopy[16], pktCopy[17], pktCopy[18], pktCopy[19]}
		rewriteL4Checksum(l4, proto, srcIPBytes, dstIPBytes)

		srcPort := uint16(l4[0])<<8 | uint16(l4[1])

		switch proto {
		case 6:
			if l4[13]&0x02 != 0 {
				seqNum := uint32(l4[4])<<24 | uint32(l4[5])<<16 | uint32(l4[6])<<8 | uint32(l4[7])
				e.core.outgoingSYNs.Store(seqNum, time.Now())
				e.core.activePorts.Store(srcPort, activePortBinding{owner: e, gen: e.core.portGen.Add(1)})
			}
			if l4[13]&0x01 != 0 || l4[13]&0x04 != 0 {
				// Delayed, generation-checked: an immediate delete would drop a still-expected reply on a half-closed connection (FIN sent, still waiting on the peer). CompareAndDelete no-ops if this port got reused for a new connection before the grace period elapsed.
				if v, ok := e.core.activePorts.Load(srcPort); ok {
					binding := v.(activePortBinding)
					core := e.core
					time.AfterFunc(portCloseGrace, func() {
						core.activePorts.CompareAndDelete(srcPort, binding)
					})
				}
			}
		case 17:
			// UDP has no handshake or FIN/RST, so the port entry lives until the worker closes or the process exits; no reaper, since there's no way to know how long a quiet UDP flow stays interesting.
			e.core.activePorts.Store(srcPort, activePortBinding{owner: e, gen: e.core.portGen.Add(1)})
		}

		var dst [4]byte
		copy(dst[:], pktCopy[16:20])

		addr := &syscall.SockaddrInet4{
			Addr: dst,
			Port: 0,
		}

		if err := syscall.Sendto(e.core.sendFd, pktCopy, 0, addr); err != nil {
			utils.Debugf("[RAW-NIC%d] Sendto failed: %v", e.nicID, err)
			continue
		}

		e.packetOut.Add(1)
		n++
	}
	return n, nil
}

func (e *RawSocketEndpoint) MTU() uint32                    { return 1500 }
func (e *RawSocketEndpoint) MaxHeaderLength() uint16        { return 0 }
func (e *RawSocketEndpoint) LinkAddress() tcpip.LinkAddress { return "" }
func (e *RawSocketEndpoint) Capabilities() stack.LinkEndpointCapabilities {
	return stack.CapabilityNone
}
func (e *RawSocketEndpoint) Attach(dispatcher stack.NetworkDispatcher) {
	e.dispatcher = dispatcher
}
func (e *RawSocketEndpoint) IsAttached() bool                        { return e.dispatcher != nil }
func (e *RawSocketEndpoint) Wait()                                   {}
func (e *RawSocketEndpoint) ARPHardwareType() header.ARPHardwareType { return header.ARPHardwareNone }
func (e *RawSocketEndpoint) AddHeader(*stack.PacketBuffer)           {}

// Close only sweeps this worker's own port registrations - the shared OS sockets outlive it - since a stale entry could otherwise point at a dead worker indefinitely (harmless for TCP, an unbounded leak for UDP).
func (e *RawSocketEndpoint) Close() {
	e.core.activePorts.Range(func(key, value any) bool {
		if value.(activePortBinding).owner == e {
			e.core.activePorts.Delete(key)
		}
		return true
	})
}
func (e *RawSocketEndpoint) SetMTU(uint32)                        {}
func (e *RawSocketEndpoint) SetLinkAddress(tcpip.LinkAddress)     {}
func (e *RawSocketEndpoint) ParseHeader(*stack.PacketBuffer) bool { return true }
func (e *RawSocketEndpoint) SetOnCloseAction(func())              {}
