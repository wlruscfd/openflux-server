package tunnel

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"

	"universal-bypass-tool/network"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

type ExitMode int

const (
	// ExitModeRaw's protocol-agnostic forwarding is also what let an unexpected ICMP packet crash gvisor's NAT code before InjectInbound started filtering non-TCP packets out.
	ExitModeRaw ExitMode = iota
	// ExitModeProxy terminates each TCP flow locally and re-dials, avoiding gvisor's NAT code entirely (so the ICMP crash class can't recur here); not yet the default since it only forwards TCP.
	ExitModeProxy
)

func (m ExitMode) String() string {
	if m == ExitModeProxy {
		return "proxy"
	}
	return "raw"
}

// ParseExitMode parses the --mode flag's value.
func ParseExitMode(s string) (ExitMode, error) {
	switch s {
	case "", "raw":
		return ExitModeRaw, nil
	case "proxy":
		return ExitModeProxy, nil
	default:
		return ExitModeRaw, fmt.Errorf("unknown mode %q (want raw|proxy)", s)
	}
}

type TCPTunnel struct {
	gvisorStack *stack.Stack
	tunnelEP    *TunnelLinkEndpoint
	transport   transport.Transport
	isExitNode  bool
	exitMode    ExitMode
	rawEP       *RawSocketEndpoint
	startTime   time.Time
	packetCount atomic.Uint64
	stopStats   chan struct{}

	// Per-packet logging is far too noisy across the hundreds of workers one node runs, so it
	// is opt-in per worker while a transport path is being diagnosed.
	tracePackets atomic.Bool
}

func (t *TCPTunnel) SetPacketTrace(on bool) {
	t.tracePackets.Store(on)
}

// hexHead dumps the first bytes of a packet verbatim. ParsePacketInfo can only report what it
// decodes, so a header that is shifted or misaligned looks like plausible-but-wrong addresses;
// the raw bytes are what actually settle where the corruption starts.
func hexHead(data []byte) string {
	n := len(data)
	if n > 24 {
		n = 24
	}
	if n == 0 {
		return "(empty)"
	}
	return fmt.Sprintf("% x", data[:n])
}

func NewTCPTunnel(trans transport.Transport, isExitNode bool) *TCPTunnel {
	return NewTCPTunnelMode(trans, isExitNode, ExitModeRaw)
}

func NewTCPTunnelMode(trans transport.Transport, isExitNode bool, mode ExitMode) *TCPTunnel {
	t := &TCPTunnel{
		transport:  trans,
		isExitNode: isExitNode,
		exitMode:   mode,
		startTime:  time.Now(),
		stopStats:  make(chan struct{}),
	}

	utils.Debugf("[TUNNEL] Net stack init...")
	t.gvisorStack = stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})

	// Max bounds throughput at Max*8/RTT; the old 1MB capped this tunnel's ~200-400ms RTT connections to ~25-30 Mbit/s from window exhaustion alone.
	if err := t.gvisorStack.SetTransportProtocolOption(tcp.ProtocolNumber,
		&tcpip.TCPReceiveBufferSizeRangeOption{Min: 65536, Default: 262144, Max: 8 * 1024 * 1024}); err != nil {
		utils.Debugf("[TUNNEL] Failed to set recv buffer: %v", err)
	}
	if err := t.gvisorStack.SetTransportProtocolOption(tcp.ProtocolNumber,
		&tcpip.TCPSendBufferSizeRangeOption{Min: 65536, Default: 262144, Max: 8 * 1024 * 1024}); err != nil {
		utils.Debugf("[TUNNEL] Failed to set send buffer: %v", err)
	}
	// gvisor's default MinRTO (200ms) is shorter than a typical round trip through this covert channel, so without raising the floor gvisor's TCP mistakes ordinary latency for loss and retransmits data still in flight.
	minRTO := tcpip.TCPMinRTOOption(1500 * time.Millisecond)
	if err := t.gvisorStack.SetTransportProtocolOption(tcp.ProtocolNumber, &minRTO); err != nil {
		utils.Debugf("[TUNNEL] Failed to set min RTO: %v", err)
	}

	tunnelEP := NewTunnelLinkEndpoint()
	tunnelEP.SetOutgoingPacketHandler(func(data []byte) {
		if t.tracePackets.Load() {
			if err := trans.Send(data); err != nil {
				utils.Debugf("[TUNNEL->] send to peer failed (%d bytes): %v", len(data), err)
				return
			}
			utils.Debugf("[TUNNEL->] %d bytes to peer", len(data))
		} else {
			trans.Send(data)
		}
	})
	t.tunnelEP = tunnelEP

	tunnelNIC := tcpip.NICID(1)
	if err := t.gvisorStack.CreateNIC(tunnelNIC, tunnelEP); err != nil {
		utils.Debugf("[TUNNEL] CreateNIC tunnel error: %v", err)
	}

	if isExitNode {
		if mode == ExitModeProxy {
			t.setupExitNodeProxy(tunnelNIC)
		} else {
			t.setupExitNodeRaw(tunnelNIC)
		}
	} else {
		t.setupClient(tunnelNIC)
	}

	trans.Receive(func(data []byte) {
		if t.tracePackets.Load() {
			utils.Debugf("[TUNNEL<-] %d bytes - %s | head=%s", len(data), network.ParsePacketInfo(data), hexHead(data))
		}
		tunnelEP.InjectInbound(data)
	})

	go t.printStats()
	return t
}

// setupExitNodeProxy uses SetPromiscuousMode+SetSpoofing so this NIC can accept and reply to a SYN for any destination IP without gvisor rejecting it as foreign traffic.
func (t *TCPTunnel) setupExitNodeProxy(tunnelNIC tcpip.NICID) {
	utils.Debugf("[TUNNEL] EXIT NODE - proxy mode (no raw socket, no root)")

	if err := t.gvisorStack.SetPromiscuousMode(tunnelNIC, true); err != nil {
		utils.Debugf("[TUNNEL] SetPromiscuousMode error: %v", err)
	}
	if err := t.gvisorStack.SetSpoofing(tunnelNIC, true); err != nil {
		utils.Debugf("[TUNNEL] SetSpoofing error: %v", err)
	}
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         tunnelNIC,
	})

	fwd := tcp.NewForwarder(t.gvisorStack, 0, 8192, t.handleExitTCP)
	t.gvisorStack.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
}

// exitTCPMaxFlows caps proxy-mode flows, since a burst of destinations that accept a connection then go silent would otherwise accumulate goroutines/sockets without bound.
const exitTCPMaxFlows = 4096

// exitTCPIdleTimeout closes a silent proxy-mode flow, since net.DialTimeout only bounds the initial connect and a blackholed peer would otherwise leak resources for the tunnel's whole lifetime.
const exitTCPIdleTimeout = 5 * time.Minute

var exitTCPSemaphore = make(chan struct{}, exitTCPMaxFlows)

func (t *TCPTunnel) handleExitTCP(r *tcp.ForwarderRequest) {
	id := r.ID()
	dest := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprintf("%d", id.LocalPort))

	var wq waiter.Queue
	ep, tErr := r.CreateEndpoint(&wq)
	if tErr != nil {
		utils.Debugf("[EXIT] proxy CreateEndpoint %s: %v", dest, tErr)
		r.Complete(true)
		return
	}
	r.Complete(false)
	local := gonet.NewTCPConn(&wq, ep)

	select {
	case exitTCPSemaphore <- struct{}{}:
	default:
		utils.Debugf("[EXIT] proxy %s rejected: too many concurrent flows (%d)", dest, exitTCPMaxFlows)
		local.Close()
		return
	}

	go func() {
		defer func() { <-exitTCPSemaphore }()

		remote, err := net.DialTimeout("tcp", dest, 10*time.Second)
		if err != nil {
			utils.Debugf("[EXIT] proxy dial %s failed: %v", dest, err)
			local.Close()
			return
		}
		if tc, ok := remote.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
		}
		utils.Debugf("[EXIT] proxy %s connected", dest)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			idleCopy(remote, local, exitTCPIdleTimeout)
		}()
		go func() {
			defer wg.Done()
			idleCopy(local, remote, exitTCPIdleTimeout)
		}()
		wg.Wait()
		local.Close()
		remote.Close()
	}()
}

// idleCopy gives up if src.Read/dst.Write make no progress within idleTimeout, instead of blocking forever on a peer that's gone silent without closing.
func idleCopy(dst, src net.Conn, idleTimeout time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		_ = src.SetReadDeadline(time.Now().Add(idleTimeout))
		n, rerr := src.Read(buf)
		if n > 0 {
			_ = dst.SetWriteDeadline(time.Now().Add(idleTimeout))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				utils.Debugf("[EXIT] proxy idle copy read error: %v", rerr)
			}
			return
		}
	}
}

// setupExitNodeRaw needs root; the kernel's own TCP stack sends a real RST on every reply unless the deploy also runs the RST-drop iptables rule.
func (t *TCPTunnel) setupExitNodeRaw(tunnelNIC tcpip.NICID) {
	localIP := getLocalIP()
	utils.Debugf("[TUNNEL] EXIT NODE - raw mode, local IP: %s", localIP)

	rawEP, err := NewRawSocketEndpoint(tcpip.NICID(2))
	if err != nil {
		// Falling back to proxy mode (rather than a half-configured raw-mode tunnel) means forgetting sudo degrades to a working, UDP-relay-less exit node instead of a silently broken one.
		utils.Debugf("[TUNNEL] raw socket error (mode raw needs root): %v", err)
		utils.Debugf("[TUNNEL] falling back to proxy mode")
		t.exitMode = ExitModeProxy
		t.setupExitNodeProxy(tunnelNIC)
		return
	}

	t.rawEP = rawEP
	rawEP.SetTransportSender(func(data []byte) {
		t.transport.Send(data)
	})

	internetNIC := tcpip.NICID(2)
	if err := t.gvisorStack.CreateNIC(internetNIC, rawEP); err != nil {
		utils.Debugf("[TUNNEL] CreateNIC internet error: %v", err)
		return
	}

	var ipBytes [4]byte
	fmt.Sscanf(localIP, "%d.%d.%d.%d", &ipBytes[0], &ipBytes[1], &ipBytes[2], &ipBytes[3])
	internetAddr := tcpip.AddrFrom4(ipBytes)
	t.gvisorStack.AddProtocolAddress(internetNIC, tcpip.ProtocolAddress{
		Protocol: ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   internetAddr,
			PrefixLen: 24,
		},
	}, stack.AddressProperties{})

	t.gvisorStack.SetForwardingDefaultAndAllNICs(ipv4.ProtocolNumber, true)
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         internetNIC,
	})

	tunnelSubnet := tcpip.AddressWithPrefix{
		Address:   tcpip.AddrFrom4([4]byte{10, 10, 10, 0}),
		PrefixLen: 24,
	}.Subnet()
	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: tunnelSubnet,
		NIC:         tunnelNIC,
	})
}

func (t *TCPTunnel) setupClient(tunnelNIC tcpip.NICID) {
	clientAddr := tcpip.AddrFrom4([4]byte{10, 10, 10, 2})
	t.gvisorStack.AddProtocolAddress(tunnelNIC, tcpip.ProtocolAddress{
		Protocol: ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{
			Address:   clientAddr,
			PrefixLen: 24,
		},
	}, stack.AddressProperties{})

	t.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         tunnelNIC,
	})
}

// ExitMode reports the mode actually running, which can differ from what was requested if raw-socket setup failed and silently fell back to proxy mode.
func (t *TCPTunnel) ExitMode() ExitMode {
	return t.exitMode
}

// SetMTU overrides the tunnel virtual link's default 1500 - a caller relaying through a constrained real path (mobile network, a smaller TUN device MTU) should match it here too, or gvisor keeps building segments this link can't actually get through.
func (t *TCPTunnel) SetMTU(mtu uint32) {
	t.tunnelEP.SetMTU(mtu)
}

// SetPortRange gives every worker a disjoint port range: since all raw-mode workers share one real IP and raw socket, overlapping ranges could deliver one key's real traffic into another's tunnel.
func (t *TCPTunnel) SetPortRange(start, end uint16) {
	if err := t.gvisorStack.SetPortRange(start, end); err != nil {
		utils.Debugf("[TUNNEL] SetPortRange(%d-%d) failed: %v", start, end, err)
	}
}

func (t *TCPTunnel) DialTCP(address string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}

	ip := tcpAddr.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("IPv6 not supported")
	}

	nic := tcpip.NICID(1)
	if t.isExitNode && t.exitMode == ExitModeRaw {
		nic = tcpip.NICID(2)
	}

	conn, err := gonet.DialTCP(t.gvisorStack, tcpip.FullAddress{
		NIC:  nic,
		Addr: tcpip.AddrFrom4([4]byte{ip[0], ip[1], ip[2], ip[3]}),
		Port: uint16(tcpAddr.Port),
	}, ipv4.ProtocolNumber)

	return conn, err
}

func (t *TCPTunnel) DialUDP(address string) (net.Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}

	ip := udpAddr.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("IPv6 not supported")
	}

	nic := tcpip.NICID(1)
	if t.isExitNode && t.exitMode == ExitModeRaw {
		nic = tcpip.NICID(2)
	}

	remote := tcpip.FullAddress{
		NIC:  nic,
		Addr: tcpip.AddrFrom4([4]byte{ip[0], ip[1], ip[2], ip[3]}),
		Port: uint16(udpAddr.Port),
	}
	return gonet.DialUDP(t.gvisorStack, nil, &remote, ipv4.ProtocolNumber)
}

func (t *TCPTunnel) ListenTCP(port uint16) (net.Listener, error) {
	return gonet.ListenTCP(t.gvisorStack, tcpip.FullAddress{
		NIC:  1,
		Port: port,
	}, ipv4.ProtocolNumber)
}

func (t *TCPTunnel) printStats() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopStats:
			return
		case <-ticker.C:
			stats := t.gvisorStack.Stats()
			utils.Debugf("[STATS] uptime=%v mode=%s packets=%d connected=%d established=%d retrans=%d",
				time.Since(t.startTime).Round(time.Second),
				t.exitMode.String(),
				t.packetCount.Load(),
				stats.TCP.CurrentConnected.Value(),
				stats.TCP.CurrentEstablished.Value(),
				stats.TCP.Retransmits.Value(),
			)
		}
	}
}

func (t *TCPTunnel) Close() {
	close(t.stopStats)
	t.gvisorStack.Destroy()
	// rawEP.Close sweeps this worker's port registrations out of the shared activePorts map; skipping this used to leak entries permanently for UDP, which has no other cleanup path.
	if t.rawEP != nil {
		t.rawEP.Close()
	}
}

// localIPOverride should point at a dedicated alias IP so the RST-drop iptables rule can be scoped to `-s <ip>` instead of dropping every outbound RST on the host, which would make every closed port look "filtered" to a scan.
var localIPOverride string

func SetLocalIP(ip string) { localIPOverride = ip }

func getLocalIP() string {
	if localIPOverride != "" {
		return localIPOverride
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "192.168.1.100"
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
