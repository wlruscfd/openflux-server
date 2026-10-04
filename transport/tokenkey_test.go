package transport

import "testing"

type pipeTransport struct {
	Transport
	peer *pipeTransport
	cb   func([]byte)
}

func (p *pipeTransport) Send(data []byte) error {
	if p.peer != nil && p.peer.cb != nil {
		p.peer.cb(data)
	}
	return nil
}

func (p *pipeTransport) Receive(callback func([]byte)) { p.cb = callback }

func newPipe() (a, b *pipeTransport) {
	a, b = &pipeTransport{}, &pipeTransport{}
	a.peer, b.peer = b, a
	return
}

func TestTokenEncryptedTransportRoundTrip(t *testing.T) {
	rawA, rawB := newPipe()
	client := NewTokenEncryptedTransport(rawA, "shared-secret-token", false)
	node := NewTokenEncryptedTransport(rawB, "shared-secret-token", true)

	var gotAtNode, gotAtClient []byte
	node.Receive(func(d []byte) { gotAtNode = d })
	client.Receive(func(d []byte) { gotAtClient = d })

	if err := client.Send([]byte("hello from client")); err != nil {
		t.Fatalf("client.Send: %v", err)
	}
	if string(gotAtNode) != "hello from client" {
		t.Errorf("node received %q, want %q", gotAtNode, "hello from client")
	}

	if err := node.Send([]byte("hello from node")); err != nil {
		t.Fatalf("node.Send: %v", err)
	}
	if string(gotAtClient) != "hello from node" {
		t.Errorf("client received %q, want %q", gotAtClient, "hello from node")
	}
}

func TestTokenEncryptedTransportManyPacketsInOrder(t *testing.T) {
	rawA, rawB := newPipe()
	client := NewTokenEncryptedTransport(rawA, "shared-secret-token", false)
	node := NewTokenEncryptedTransport(rawB, "shared-secret-token", true)

	var got [][]byte
	node.Receive(func(d []byte) { got = append(got, append([]byte(nil), d...)) })

	for i := 0; i < 50; i++ {
		if err := client.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	if len(got) != 50 {
		t.Fatalf("got %d packets, want 50", len(got))
	}
	for i, p := range got {
		if len(p) != 1 || p[0] != byte(i) {
			t.Errorf("packet %d = %v, want [%d]", i, p, i)
		}
	}
}

func TestTokenEncryptedTransportWrongTokenFailsToDecrypt(t *testing.T) {
	rawA, rawB := newPipe()
	node := NewTokenEncryptedTransport(rawA, "token-one", true)
	client := NewTokenEncryptedTransport(rawB, "token-two", false)

	var got []byte
	client.Receive(func(d []byte) { got = d })
	node.Send([]byte("secret"))

	if got != nil {
		t.Errorf("decrypted with the wrong token: %v", got)
	}
}

// An old app build never wraps its transport in TokenEncryptedTransport, so unable-to-decrypt here means the two ends disagree about e2e_encryption - dropping instead of passing through makes the setting mandatory.
func TestTokenEncryptedTransportExitNodeDropsUnencryptedPeer(t *testing.T) {
	rawA, rawB := newPipe()
	node := NewTokenEncryptedTransport(rawA, "shared-secret-token", true)

	var got []byte
	node.Receive(func(d []byte) { got = d })
	rawB.Send([]byte("plaintext from a client that isn't encrypting"))

	if got != nil {
		t.Errorf("got %q, want the unencrypted packet dropped", got)
	}
}

func TestTokenEncryptedTransportDirectionKeysDiffer(t *testing.T) {
	inner, _ := newPipe()
	client := NewTokenEncryptedTransport(inner, "shared-secret-token", false)
	node := NewTokenEncryptedTransport(inner, "shared-secret-token", true)

	if client.send == node.send {
		t.Errorf("client and node ended up with the same send key")
	}
	if client.send != node.recv || client.recv != node.send {
		t.Errorf("client/node send-recv keys don't line up: client.send=%v node.recv=%v", client.send, node.recv)
	}
}
