package socks5

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

type Dialer interface {
	DialTCP(address string) (net.Conn, error)
}

type SOCKS5Server struct {
	listenAddr string
	dialer     Dialer

	mu       sync.Mutex
	listener net.Listener
}

func NewSOCKS5Server(addr string, dialer Dialer) *SOCKS5Server {
	return &SOCKS5Server{listenAddr: addr, dialer: dialer}
}

func (s *SOCKS5Server) Start() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	defer listener.Close()

	utils.Debugf("[SOCKS5] Listening on %s", s.listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Stop() closing the listener surfaces as an Accept error too; tell that apart from a transient one so a deliberate shutdown returns cleanly instead of logging in a tight loop.
			s.mu.Lock()
			stopped := s.listener == nil
			s.mu.Unlock()
			if stopped {
				return nil
			}
			utils.Debugf("[SOCKS5] Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *SOCKS5Server) Stop() error {
	s.mu.Lock()
	l := s.listener
	s.listener = nil
	s.mu.Unlock()
	if l == nil {
		return nil
	}
	return l.Close()
}

// handleConnection parses the handshake with io.ReadFull, not single Read calls - TCP is a byte
// stream, and a request split across multiple packets (common on mobile networks) would otherwise
// be silently truncated or misparsed.
func (s *SOCKS5Server) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()
	r := bufio.NewReader(clientConn)

	greeting := make([]byte, 2)
	if _, err := io.ReadFull(r, greeting); err != nil || greeting[0] != 0x05 {
		return
	}
	if _, err := io.ReadFull(r, make([]byte, greeting[1])); err != nil {
		return
	}
	clientConn.Write([]byte{0x05, 0x00})

	reqHeader := make([]byte, 4)
	if _, err := io.ReadFull(r, reqHeader); err != nil {
		return
	}
	if reqHeader[1] != 0x01 {
		clientConn.Write([]byte{0x05, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // command not supported
		return
	}

	var targetAddr string
	switch reqHeader[3] {
	case 0x01:
		addr := make([]byte, 6)
		if _, err := io.ReadFull(r, addr); err != nil {
			return
		}
		targetAddr = fmt.Sprintf("%d.%d.%d.%d:%d", addr[0], addr[1], addr[2], addr[3], uint16(addr[4])<<8|uint16(addr[5]))
	case 0x03:
		lenByte := make([]byte, 1)
		if _, err := io.ReadFull(r, lenByte); err != nil {
			return
		}
		rest := make([]byte, int(lenByte[0])+2)
		if _, err := io.ReadFull(r, rest); err != nil {
			return
		}
		targetAddr = fmt.Sprintf("%s:%d", string(rest[:lenByte[0]]), uint16(rest[lenByte[0]])<<8|uint16(rest[lenByte[0]+1]))
	default:
		clientConn.Write([]byte{0x05, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // address type not supported
		return
	}

	utils.Debugf("[SOCKS5] CONNECT %s", targetAddr)

	targetConn, err := s.dialer.DialTCP(targetAddr)
	if err != nil {
		utils.Debugf("[SOCKS5] Dial failed: %v", err)
		clientConn.Write([]byte{0x05, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
		return
	}
	defer targetConn.Close()

	clientConn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer targetConn.Close()
		// r, not clientConn: any bytes a pipelining client already sent past the CONNECT
		// request are sitting in r's buffer, not yet visible to a raw Read on clientConn.
		io.Copy(targetConn, r)
	}()

	go func() {
		defer wg.Done()
		defer clientConn.Close()
		io.Copy(clientConn, targetConn)
	}()

	wg.Wait()
}
