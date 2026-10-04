package transport

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"

	"github.com/p1neappleXpress/OpenFlux/utils"
)

// EncryptedTransport is strict both ways - anything that doesn't decrypt is dropped - because a lenient auto-detect used to leave e2e_encryption purely advisory when a client silently didn't encrypt.
type EncryptedTransport struct {
	Transport
	send    [chacha20poly1305.KeySize]byte
	recv    [chacha20poly1305.KeySize]byte
	sendCtr atomic.Uint64
}

func NewEncryptedTransport(inner Transport, token string, isExitNode bool) *EncryptedTransport {
	send, recv := DeriveDirectionalKeys(token, isExitNode, "")
	return &EncryptedTransport{Transport: inner, send: send, recv: recv}
}

func NewEncryptedTransportForStream(inner Transport, token string, isExitNode bool, streamIndex int) *EncryptedTransport {
	send, recv := DeriveDirectionalKeys(token, isExitNode, fmt.Sprintf(" stream %d", streamIndex))
	return &EncryptedTransport{Transport: inner, send: send, recv: recv}
}

func DeriveDirectionalKeys(token string, isExitNode bool, infoSuffix string) (send, recv [chacha20poly1305.KeySize]byte) {
	base := sha256.Sum256([]byte(token))
	c2s := deriveKey(base[:], "openflux c2s"+infoSuffix)
	s2c := deriveKey(base[:], "openflux s2c"+infoSuffix)
	if isExitNode {
		return s2c, c2s
	}
	return c2s, s2c
}

func deriveKey(base []byte, info string) [chacha20poly1305.KeySize]byte {
	var out [chacha20poly1305.KeySize]byte
	io.ReadFull(hkdf.New(sha256.New, base, nil, []byte(info)), out[:])
	return out
}

// Seal's nonce is a monotonic counter prepended in the clear so Open doesn't need packets in order; exported so transport/yandex can reuse this exact, already-audited algorithm.
func Seal(key [chacha20poly1305.KeySize]byte, counter uint64, data []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	var nonce [chacha20poly1305.NonceSize]byte
	binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], counter)

	out := make([]byte, 8, 8+len(data)+aead.Overhead())
	binary.BigEndian.PutUint64(out, counter)
	return aead.Seal(out, nonce[:], data, nil), nil
}

func Open(key [chacha20poly1305.KeySize]byte, data []byte) ([]byte, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("too short to carry a nonce counter: %d bytes", len(data))
	}
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	var nonce [chacha20poly1305.NonceSize]byte
	binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], binary.BigEndian.Uint64(data[:8]))
	return aead.Open(nil, nonce[:], data[8:], nil)
}

func (e *EncryptedTransport) Send(data []byte) error {
	out, err := Seal(e.send, e.sendCtr.Add(1), data)
	if err != nil {
		return err
	}
	return e.Transport.Send(out)
}

// Receive drops anything that doesn't decrypt with this key rather than passing it through, matching the strict, no-longer-lenient contract on the exit-node side.
func (e *EncryptedTransport) Receive(callback func([]byte)) {
	e.Transport.Receive(func(data []byte) {
		plain, err := Open(e.recv, data)
		if err != nil {
			utils.Debugf("[CRYPT] decrypt failed - dropping packet: %v", err)
			return
		}
		callback(plain)
	})
}
