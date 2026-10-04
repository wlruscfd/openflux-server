package transport

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

// UDPRelayTransport is the fast hop of a cascade (entry node -> final-exit node) - plain, encrypted
// UDP between two servers we control. Only the client-facing hop needs to look like something else;
// server-to-server traffic never goes near Yandex and doesn't pay its batching/size overhead.
//
// The initiator (entry node) knows the final-exit node's address up front and dials it from an
// ephemeral local port. The final-exit node can't know that ephemeral port in advance, so it listens
// on its own well-known port instead and learns the peer's address off the first packet that
// actually decrypts - a forged source address can make it briefly forget the real peer, but can't
// inject data without the shared secret, and the next real packet corrects it.
type UDPRelayTransport struct {
	*BaseTransport

	isInitiator bool
	localAddr   string // initiator: ":0" for an ephemeral port; final-exit: the well-known listen address
	remoteAddr  string // initiator only: the peer's known address

	send, recv [chacha20poly1305.KeySize]byte
	sendCtr    atomic.Uint64

	conn atomic.Pointer[net.UDPConn]
	peer atomic.Pointer[net.UDPAddr] // final-exit only: learned from the first valid packet
}

const udpRelayMaxPacket = 65535

// NewUDPRelayTransport derives directional keys from sharedSecret exactly like EncryptedTransport does from a key's token - isInitiator picks which side of that split this end uses, and must be opposite on the two ends of one link.
func NewUDPRelayTransport(localAddr, remoteAddr, sharedSecret string, isInitiator bool, cfg TransportConfig) *UDPRelayTransport {
	t := &UDPRelayTransport{
		BaseTransport: NewBaseTransport(cfg),
		isInitiator:   isInitiator,
		localAddr:     localAddr,
		remoteAddr:    remoteAddr,
	}
	t.send, t.recv = DeriveDirectionalKeys(sharedSecret, !isInitiator, " udprelay")
	return t
}

func (t *UDPRelayTransport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}

	var conn *net.UDPConn
	if t.isInitiator {
		remote, err := net.ResolveUDPAddr("udp", t.remoteAddr)
		if err != nil {
			return fmt.Errorf("resolve remote addr %q: %w", t.remoteAddr, err)
		}
		conn, err = net.DialUDP("udp", nil, remote)
		if err != nil {
			return fmt.Errorf("dial %s: %w", t.remoteAddr, err)
		}
	} else {
		local, err := net.ResolveUDPAddr("udp", t.localAddr)
		if err != nil {
			return fmt.Errorf("resolve local addr %q: %w", t.localAddr, err)
		}
		conn, err = net.ListenUDP("udp", local)
		if err != nil {
			return fmt.Errorf("listen %s: %w", t.localAddr, err)
		}
	}

	t.conn.Store(conn)
	t.SetConnected(true)
	t.EmitEvent(EventConnected, "")
	go t.readLoop(conn)
	return nil
}

func (t *UDPRelayTransport) Stop() error {
	t.BaseTransport.Stop()
	if conn := t.conn.Load(); conn != nil {
		conn.Close()
	}
	return nil
}

func (t *UDPRelayTransport) Send(data []byte) error {
	conn := t.conn.Load()
	if conn == nil {
		return fmt.Errorf("udprelay: not started")
	}
	sealed, err := Seal(t.send, t.sendCtr.Add(1), data)
	if err != nil {
		return err
	}

	if t.isInitiator {
		_, err = conn.Write(sealed)
	} else {
		peer := t.peer.Load()
		if peer == nil {
			return fmt.Errorf("udprelay: peer not learned yet")
		}
		_, err = conn.WriteToUDP(sealed, peer)
	}
	if err != nil {
		return err
	}
	t.RecordSend(len(data))
	return nil
}

// readLoop uses a read deadline only to re-check IsRunning periodically - UDP has no keepalive of its own, so a timeout here means "quiet", not "dead".
func (t *UDPRelayTransport) readLoop(conn *net.UDPConn) {
	buf := make([]byte, udpRelayMaxPacket)
	for t.IsRunning() {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if t.IsRunning() {
				utils.Debugf("[UDPRELAY] read error: %v", err)
			}
			t.SetConnected(false)
			return
		}

		plain, err := Open(t.recv, buf[:n])
		if err != nil {
			utils.Debugf("[UDPRELAY] decrypt failed - dropping packet: %v", err)
			continue
		}
		if !t.isInitiator {
			t.peer.Store(from)
		}
		t.RecordReceive(len(plain))
		t.CallReceive(plain)
	}
}
