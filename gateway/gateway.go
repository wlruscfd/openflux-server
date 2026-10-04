// Package gateway turns a raw TUN file descriptor into TCP connections dialed through a Dialer, the same role socks5.SOCKS5Server plays for desktop clients.
package gateway

import (
	"crypto/tls"
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

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/nodetunnel"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

type Dialer interface {
	DialTCP(address string) (net.Conn, error)
	DialUDP(address string) (net.Conn, error)
}

// udpIdleTimeout closes a relayed UDP flow after this long idle, since UDP has no FIN/close signal of its own.
const udpIdleTimeout = 60 * time.Second

const gatewayNIC = tcpip.NICID(1)

// Server runs a gvisor stack in transparent-proxy mode; any IPv6 (unregistered) is left unhandled, failing closed rather than leaking outside the nodetunnel.
type Server struct {
	dialer               Dialer
	dnsUpstreamCfg       dnsUpstreamConfig
	dnsUpstreamTLSConfig *tls.Config
	directDialer         *net.Dialer

	sitePolicy      *SitePolicy
	dnsCache        *dnsCache
	directResolvers []string

	gvisorStack *stack.Stack
	linkEP      *nodetunnel.TunnelLinkEndpoint
	closed      atomic.Bool
	mtu         uint32
}

// SetMTU overrides the gateway NIC's default 1500 - call before Start(). Should match whatever real device interface is feeding this gateway's TUN reader, or gvisor here builds segments that link can't actually carry.
func (s *Server) SetMTU(mtu uint32) {
	s.mtu = mtu
}

func NewServer(dialer Dialer, dnsUpstream string) *Server {
	return NewServerWithPolicy(dialer, dnsUpstream, nil)
}

func NewServerWithPolicy(dialer Dialer, dnsUpstream string, policy *SitePolicy) *Server {
	cfg := parseDNSUpstream(dnsUpstream)
	return &Server{
		dialer:               dialer,
		dnsUpstreamCfg:       cfg,
		dnsUpstreamTLSConfig: &tls.Config{ServerName: cfg.host},
		directDialer:         transport.ProtectedDialer(),
		sitePolicy:           policy,
		dnsCache:             newDNSCache(),
		directResolvers:      defaultDirectResolvers(cfg),
	}
}

// defaultDirectResolvers combines the profile's own upstream with the transport's bootstrap resolvers, since dnsQueryDirect only ever speaks plain UDP/TCP:53.
func defaultDirectResolvers(upstream dnsUpstreamConfig) []string {
	var out []string
	seen := make(map[string]struct{})
	add := func(host string) {
		if _, _, err := net.SplitHostPort(host); err != nil {
			host = net.JoinHostPort(host, "53")
		}
		if _, dup := seen[host]; dup {
			return
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	if upstream.kind == dnsUpstreamPlain {
		add(upstream.addr)
	}
	for _, s := range transport.BootstrapDNSServers() {
		add(s)
	}
	return out
}

func (s *Server) Start(tunReader io.Reader, tunWriter io.Writer) error {
	s.gvisorStack = stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})

	linkEP := nodetunnel.NewTunnelLinkEndpoint()
	if s.mtu > 0 {
		linkEP.SetMTU(s.mtu)
	}
	linkEP.SetOutgoingPacketHandler(func(data []byte) {
		if _, err := tunWriter.Write(data); err != nil {
			utils.Debugf("[GATEWAY] tun write error: %v", err)
		}
	})
	s.linkEP = linkEP

	if err := s.gvisorStack.CreateNIC(gatewayNIC, linkEP); err != nil {
		return fmt.Errorf("create gateway NIC: %v", err)
	}
	if err := s.gvisorStack.SetPromiscuousMode(gatewayNIC, true); err != nil {
		return fmt.Errorf("enable promiscuous mode: %v", err)
	}
	if err := s.gvisorStack.SetSpoofing(gatewayNIC, true); err != nil {
		return fmt.Errorf("enable spoofing: %v", err)
	}
	// Promiscuous+spoofing lets an incoming packet be accepted with no local address, but a route is still what tells gvisor which NIC to send the SYN-ACK reply out of.
	s.gvisorStack.AddRoute(tcpip.Route{
		Destination: header.IPv4EmptySubnet,
		NIC:         gatewayNIC,
	})

	tcpForwarder := tcp.NewForwarder(s.gvisorStack, 0, 2048, s.handleTCP)
	s.gvisorStack.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpForwarder.HandlePacket)

	udpForwarder := udp.NewForwarder(s.gvisorStack, s.handleUDP)
	s.gvisorStack.SetTransportProtocolHandler(udp.ProtocolNumber, udpForwarder.HandlePacket)

	go s.readTunLoop(tunReader)

	return nil
}

// Close does not close tunReader/tunWriter - the caller owns that file's lifecycle.
func (s *Server) Close() {
	s.closed.Store(true)
	if s.gvisorStack != nil {
		s.gvisorStack.Destroy()
	}
}

func (s *Server) readTunLoop(r io.Reader) {
	buf := make([]byte, 1500+64)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if !s.closed.Load() {
				utils.Debugf("[GATEWAY] tun read error: %v", err)
			}
			return
		}
		if s.closed.Load() {
			return
		}
		if n == 0 {
			continue
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		s.linkEP.InjectInbound(pkt)
	}
}

func (s *Server) handleTCP(r *tcp.ForwarderRequest) {
	id := r.ID()

	var wq waiter.Queue
	ep, err := r.CreateEndpoint(&wq)
	if err != nil {
		utils.Debugf("[GATEWAY] tcp handshake failed for %s:%d: %v", id.LocalAddress, id.LocalPort, err)
		r.Complete(true)
		return
	}
	r.Complete(false)

	localConn := gonet.NewTCPConn(&wq, ep)
	dest := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprint(id.LocalPort))

	go s.relayTCP(localConn, dest)
}

func (s *Server) relayTCP(localConn net.Conn, dest string) {
	defer localConn.Close()

	body := localConn
	if s.sitePolicy != nil && s.sitePolicy.Enabled() {
		var peeked []byte
		peeked, body = sniffClientHello(localConn)
		if s.shouldBypassConnection(peeked, dest) {
			utils.Debugf("[GATEWAY] site split: %s bypasses the tunnel", s.connectionLabel(peeked, dest))
			s.relayDirect(body, dest)
			return
		}
	}

	remoteConn, err := s.dialer.DialTCP(dest)
	if err != nil {
		utils.Debugf("[GATEWAY] dial %s failed: %v", dest, err)
		return
	}
	defer remoteConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer remoteConn.Close()
		io.Copy(remoteConn, body)
	}()
	go func() {
		defer wg.Done()
		defer localConn.Close()
		io.Copy(localConn, remoteConn)
	}()

	wg.Wait()
}

// relayDirect dials the destination with the transport's protected dialer so the bypass socket can't be re-captured by the tunnel it's meant to go around.
func (s *Server) relayDirect(localConn net.Conn, dest string) {
	remoteConn, err := s.directDialer.Dial("tcp", dest)
	if err != nil {
		utils.Debugf("[GATEWAY] direct dial %s failed: %v", dest, err)
		return
	}
	defer remoteConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer remoteConn.Close()
		io.Copy(remoteConn, localConn)
	}()
	go func() {
		defer wg.Done()
		defer localConn.Close()
		io.Copy(localConn, remoteConn)
	}()

	wg.Wait()
}

// shouldBypassConnection checks SNI, destination IP, and the DNS cache's reverse mapping; ambiguity always resolves toward the nodetunnel.
func (s *Server) shouldBypassConnection(peeked []byte, dest string) bool {
	var candidates []string
	if domain := sniServerName(peeked); domain != "" {
		candidates = append(candidates, domain)
	}
	host, _, err := net.SplitHostPort(dest)
	if err == nil && s.dnsCache != nil {
		candidates = append(candidates, s.dnsCache.lookup(host)...)
	}
	return s.sitePolicy.ShouldBypassDest(host, candidates)
}

func (s *Server) connectionLabel(peeked []byte, dest string) string {
	if domain := sniServerName(peeked); domain != "" {
		return domain
	}
	host, _, err := net.SplitHostPort(dest)
	if err != nil || s.dnsCache == nil {
		return dest
	}
	if domains := s.dnsCache.lookup(host); len(domains) > 0 {
		return domains[0]
	}
	return dest
}

func (s *Server) handleUDP(r *udp.ForwarderRequest) bool {
	id := r.ID()

	var wq waiter.Queue
	ep, err := r.CreateEndpoint(&wq)
	if err != nil {
		utils.Debugf("[GATEWAY] udp endpoint failed for %s:%d: %v", id.LocalAddress, id.LocalPort, err)
		return false
	}

	localConn := gonet.NewUDPConn(&wq, ep)
	if id.LocalPort == 53 {
		go s.relayDNS(localConn)
		return true
	}

	dest := net.JoinHostPort(id.LocalAddress.String(), fmt.Sprint(id.LocalPort))
	go s.relayUDP(localConn, dest)
	return true
}

// relayUDP bridges a flow through the tunnel until either side errors, closes, or goes silent past udpIdleTimeout.
func (s *Server) relayUDP(localConn net.Conn, dest string) {
	defer localConn.Close()

	remoteConn, err := s.dialer.DialUDP(dest)
	if err != nil {
		utils.Debugf("[GATEWAY] udp dial %s failed: %v", dest, err)
		return
	}
	defer remoteConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer remoteConn.Close()
		copyDatagrams(remoteConn, localConn)
	}()
	go func() {
		defer wg.Done()
		defer localConn.Close()
		copyDatagrams(localConn, remoteConn)
	}()

	wg.Wait()
}

// copyDatagrams resets the read deadline after every datagram, unlike io.Copy, so an idle (not closed) flow can time out.
func copyDatagrams(dst, src net.Conn) {
	buf := make([]byte, 65535)
	for {
		src.SetReadDeadline(time.Now().Add(udpIdleTimeout))
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}
