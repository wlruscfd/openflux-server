package gateway

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

const (
	dnsQueryTimeout   = 8 * time.Second
	maxDNSMessageSize = 65535
)

// relayDNS answers a query directly (bypassing the tunnel) when site policy excludes it, and always feeds replies to the DNS cache for later SNI-less classification.
func (s *Server) relayDNS(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(dnsQueryTimeout))

	query := make([]byte, maxDNSMessageSize)
	n, err := conn.Read(query)
	if err != nil {
		utils.Debugf("[GATEWAY] dns query read failed: %v", err)
		return
	}
	query = query[:n]

	if s.sitePolicy != nil && s.sitePolicy.Enabled() {
		if qname, ok := dnsQuestionName(query); ok && s.sitePolicy.ShouldBypass(qname) {
			if reply := s.resolveDirect(query); reply != nil {
				s.recordDNS(reply)
				if _, err := conn.Write(reply); err != nil {
					utils.Debugf("[GATEWAY] dns reply write failed: %v", err)
				}
				return
			}
			utils.Debugf("[GATEWAY] direct DNS for %s failed, falling back to tunneled", qname)
		}
	}

	reply, err := s.queryUpstream(query)
	if err != nil {
		utils.Debugf("[GATEWAY] dns upstream query failed: %v", err)
		return
	}

	s.recordDNS(reply)

	if _, err := conn.Write(reply); err != nil {
		utils.Debugf("[GATEWAY] dns reply write failed: %v", err)
	}
}

// recordDNS feeds a DNS reply to the IP->domain cache when one is present.
func (s *Server) recordDNS(reply []byte) {
	if s.dnsCache != nil {
		s.dnsCache.record(dnsARecords(reply))
	}
}

func (s *Server) resolveDirect(query []byte) []byte {
	for _, server := range s.directResolvers {
		if reply, ok := s.dnsQueryDirect(server, query); ok {
			return reply
		}
	}
	return nil
}

func (s *Server) dnsQueryDirect(server string, query []byte) ([]byte, bool) {
	d := *s.directDialer

	if conn, err := d.Dial("udp", server); err == nil {
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(dnsQueryTimeout))
		if _, err := conn.Write(query); err == nil {
			reply := make([]byte, maxDNSMessageSize)
			if n, err := conn.Read(reply); err == nil {
				return reply[:n], true
			}
		}
	}

	if conn, err := d.Dial("tcp", server); err == nil {
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(dnsQueryTimeout))
		if err := writeDNSOverTCP(conn, query); err == nil {
			reply, err := readDNSOverTCP(conn)
			if err == nil {
				return reply, true
			}
		}
	}
	return nil, false
}

func writeDNSOverTCP(w io.Writer, msg []byte) error {
	if len(msg) > maxDNSMessageSize {
		return fmt.Errorf("dns message too large: %d bytes", len(msg))
	}
	var lenPrefix [2]byte
	binary.BigEndian.PutUint16(lenPrefix[:], uint16(len(msg)))
	if _, err := w.Write(lenPrefix[:]); err != nil {
		return err
	}
	_, err := w.Write(msg)
	return err
}

func readDNSOverTCP(r io.Reader) ([]byte, error) {
	var lenPrefix [2]byte
	if _, err := io.ReadFull(r, lenPrefix[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint16(lenPrefix[:])
	msg := make([]byte, size)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}
