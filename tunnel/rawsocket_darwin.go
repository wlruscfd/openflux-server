package tunnel

import (
	"fmt"
	"net"
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

type RawSocketEndpoint struct {
	dispatcher stack.NetworkDispatcher
	sendFd     int
	recvFd     int
	// recvUDPFd is a second receive socket for UDP: SOCK_RAW delivers only the protocol it was opened with, and IPPROTO_RAW is send-only.
	recvUDPFd       int
	nicID           tcpip.NICID
	packetIn        atomic.Uint64
	packetOut       atomic.Uint64
	outgoingSYNs    sync.Map
	activePorts     sync.Map // port uint16 -> generation uint64
	portGen         atomic.Uint64
	sendToTransport func([]byte)
}

// portCloseGrace: TCP allows half-close - the side that sent FIN can still be waiting on the peer's own data/FIN - so evicting the port the instant we relay our own FIN/RST out would drop a real, still-expected reply as "port not active". Delaying the evict gives that tail traffic a window to still get through.
const portCloseGrace = 2 * time.Second

func NewRawSocketEndpoint(nicID tcpip.NICID) (*RawSocketEndpoint, error) {
	sendFd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_RAW)
	if err != nil {
		return nil, fmt.Errorf("send socket failed: %v (need root)", err)
	}

	if err := syscall.SetsockoptInt(sendFd, syscall.IPPROTO_IP, syscall.IP_HDRINCL, 1); err != nil {
		syscall.Close(sendFd)
		return nil, fmt.Errorf("IP_HDRINCL: %v", err)
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

	ep := &RawSocketEndpoint{
		sendFd:    sendFd,
		recvFd:    recvFd,
		recvUDPFd: recvUDPFd,
		nicID:     nicID,
	}

	go ep.readLoop(ep.recvFd, 6)
	go ep.readLoop(ep.recvUDPFd, 17)
	return ep, nil
}

func (e *RawSocketEndpoint) SetTransportSender(sendFunc func([]byte)) {
	e.sendToTransport = sendFunc
}

func (e *RawSocketEndpoint) readLoop(fd int, wantProto byte) {
	buf := make([]byte, 65535)
	minLen := 40
	if wantProto == 17 {
		minLen = 28
	}

	for {
		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			utils.Debugf("[RAW-NIC%d/%d] Read error: %v", e.nicID, wantProto, err)
			return
		}
		if n < minLen {
			continue
		}

		protocol := buf[9]
		dstIP := net.IP(buf[16:20])
		localIP := getLocalIP()

		if protocol == wantProto && dstIP.String() == localIP {
			ipHeaderLenIn := int(buf[0]&0x0F) * 4
			// This raw socket sees every TCP/UDP packet on the host, not just tunnel traffic - a
			// stray or spoofed packet declaring an IHL bigger than what actually arrived would
			// otherwise panic the slice below and take down the whole process.
			if ipHeaderLenIn < 20 || n-ipHeaderLenIn < 20 {
				continue
			}
			l4In := buf[ipHeaderLenIn:n]
			dstPort := uint16(l4In[2])<<8 | uint16(l4In[3])

			if _, active := e.activePorts.Load(dstPort); !active {
				continue
			}

			// A SYN-ACK is matched against our own SYN; UDP has no connection, nothing to match.
			if protocol == 6 && l4In[13] == 0x12 {
				ackNum := uint32(l4In[8])<<24 | uint32(l4In[9])<<16 | uint32(l4In[10])<<8 | uint32(l4In[11])
				synSeq := ackNum - 1

				if _, ok := e.outgoingSYNs.Load(synSeq); !ok {
					continue
				}
				e.outgoingSYNs.Delete(synSeq)
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

			if e.sendToTransport != nil {
				e.sendToTransport(pktCopy)
			}
		}
	}
}

func (e *RawSocketEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	n := 0
	for _, pkt := range pkts.AsSlice() {
		ipPacket := pkt.ToView().ToSlice()
		// 28 = IP(20) + UDP(8): the threshold follows the shortest protocol we carry (see linux).
		if len(ipPacket) < 28 {
			continue
		}

		pktCopy := make([]byte, len(ipPacket))
		copy(pktCopy, ipPacket)

		localIP := getLocalIP()
		var localIPBytes [4]byte
		fmt.Sscanf(localIP, "%d.%d.%d.%d", &localIPBytes[0], &localIPBytes[1], &localIPBytes[2], &localIPBytes[3])
		copy(pktCopy[12:16], localIPBytes[:])

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
				e.outgoingSYNs.Store(seqNum, true)
				e.activePorts.Store(srcPort, e.portGen.Add(1))
			}
			if l4[13]&0x01 != 0 || l4[13]&0x04 != 0 {
				// Delayed, generation-checked: an immediate delete would drop a still-expected reply on a half-closed connection (FIN sent, still waiting on the peer). CompareAndDelete no-ops if this port got reused for a new connection before the grace period elapsed.
				if gen, ok := e.activePorts.Load(srcPort); ok {
					time.AfterFunc(portCloseGrace, func() {
						e.activePorts.CompareAndDelete(srcPort, gen)
					})
				}
			}
		case 17:
			// UDP has neither a handshake nor FIN/RST: the first datagram opens the port.
			e.activePorts.Store(srcPort, e.portGen.Add(1))
		}

		var dst [4]byte
		copy(dst[:], pktCopy[16:20])

		addr := &syscall.SockaddrInet4{
			Addr: dst,
			Port: 0,
		}

		if err := syscall.Sendto(e.sendFd, pktCopy, 0, addr); err != nil {
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
func (e *RawSocketEndpoint) Close() {
	syscall.Close(e.sendFd)
	syscall.Close(e.recvFd)
	syscall.Close(e.recvUDPFd)
}
func (e *RawSocketEndpoint) SetMTU(uint32)                        {}
func (e *RawSocketEndpoint) SetLinkAddress(tcpip.LinkAddress)     {}
func (e *RawSocketEndpoint) ParseHeader(*stack.PacketBuffer) bool { return true }
func (e *RawSocketEndpoint) SetOnCloseAction(func())              {}
