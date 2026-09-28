package yandex

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"universal-bypass-tool/transport"
)

// --- Send ---------------------------------------------------------------

func TestSendQueuesEvenWhileDisconnected(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.session = &DocSession{WriteQueue: make(chan []byte, 4)}

	if err := tr.Send([]byte("hello")); err != nil {
		t.Fatalf("Send while disconnected but with a session = %v, want nil", err)
	}

	select {
	case got := <-tr.session.WriteQueue:
		if string(got) != "hello" {
			t.Errorf("queued %q, want %q", got, "hello")
		}
	default:
		t.Fatalf("Send returned nil but nothing was queued")
	}
}

func TestSendFailsWithNoSessionAtAll(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	if err := tr.Send([]byte("hello")); err == nil {
		t.Fatalf("expected an error before any session has ever been established")
	}
}

// --- self-echo filtering -------------------------------------------------

func TestHandleMessageDropsOwnEcho(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())

	sent := []byte("own outgoing packet bytes")
	tr.markSent(sent)

	var received [][]byte
	tr.SetEventCallback(func(string, string) {})
	tr.Receive(func(data []byte) { received = append(received, data) })

	echoMsg := `42["message",{"type":"cursor","cursor":"18;` + b64(sent) + `"}]`
	tr.handleMessage(nil, []byte(echoMsg))

	if len(received) != 0 {
		t.Fatalf("handleMessage delivered our own echoed packet to CallReceive: %v", received)
	}
}

// The echo filter must not swallow data this transport never sent.
func TestHandleMessageDeliversRealPeerData(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, data) })

	peerData := []byte("genuine data from the other side")
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(peerData) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 1 || string(received[0]) != string(peerData) {
		t.Fatalf("CallReceive got %v, want [%q]", received, peerData)
	}
}

func b64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// --- batching -------------------------------------------------------------

func buildBatch(t *testing.T, packets ...[]byte) []byte {
	t.Helper()
	var blob bytes.Buffer
	blob.WriteByte(batchMarker)
	var lenBuf [2]byte
	for _, p := range packets {
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(p)))
		blob.Write(lenBuf[:])
		blob.Write(p)
	}
	return blob.Bytes()
}

func TestHandleMessageUnbatchesMultiPacketPayload(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, append([]byte(nil), data...)) })

	pkt1 := []byte("first packet")
	pkt2 := []byte("second, a bit longer packet")
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(buildBatch(t, pkt1, pkt2)) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 2 || string(received[0]) != string(pkt1) || string(received[1]) != string(pkt2) {
		t.Fatalf("received = %v, want [%q %q]", received, pkt1, pkt2)
	}
}

func TestHandleMessageStillHandlesUnbatchedLegacyPayload(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, data) })

	legacy := []byte{0x00, 'h', 'i'} // "stored" marker, not batchMarker
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(legacy) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 1 || string(received[0]) != string(legacy) {
		t.Fatalf("received = %v, want [%q] delivered whole, unsplit", received, legacy)
	}
}

// Self-echo dedup must hash the whole framed batch, not individual packets.
func TestHandleMessageDropsOwnEchoedBatch(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())

	batch := buildBatch(t, []byte("packet a"), []byte("packet b"))
	tr.markSent(batch)

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, data) })

	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(batch) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 0 {
		t.Fatalf("handleMessage delivered our own echoed batch: %v", received)
	}
}

func TestWriterLoopBatchesMultiplePacketsIntoOneMessage(t *testing.T) {
	received := make(chan []byte, 1)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- msg
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	tr.peerBatches.Store(true)
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}

	go tr.writerLoop(queue)

	want := [][]byte{[]byte("aaa"), []byte("bb"), []byte("ccccc")}
	for _, p := range want {
		queue <- p
	}

	select {
	case msg := <-received:
		base64Str := tr.extractBase64String(string(msg))
		if base64Str == "" {
			t.Fatalf("could not extract a base64 payload from %q", msg)
		}
		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(decoded) == 0 || decoded[0] != batchMarker {
			t.Fatalf("expected a batchMarker-prefixed payload, got %v", decoded)
		}
		got := decodeBatch(decoded[1:])
		if len(got) != len(want) {
			t.Fatalf("got %d packets, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if string(got[i]) != string(want[i]) {
				t.Errorf("packet[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server never received the batched message")
	}
}

// writerLoop must not drain a queued backlog before IsConnected() is true (regression: see connectToDoc).
func TestWriterLoopWaitsForConnectedBeforeDraining(t *testing.T) {
	received := make(chan []byte, 1)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- msg
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}
	queue <- []byte("queued-before-auth")

	go tr.writerLoop(queue)

	select {
	case msg := <-received:
		t.Fatalf("writerLoop sent %q before IsConnected() was ever true", msg)
	case <-time.After(150 * time.Millisecond):
	}

	tr.SetConnected(true)

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatalf("writerLoop never drained the queue after IsConnected() became true")
	}
}

func TestWriterLoopDoesNotBatchByDefault(t *testing.T) {
	received := make(chan []byte, 10)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			received <- msg
		}
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}

	go tr.writerLoop(queue)

	want := [][]byte{[]byte("aaa"), []byte("bb")}
	for _, p := range want {
		queue <- p
	}

	for i, w := range want {
		select {
		case msg := <-received:
			base64Str := tr.extractBase64String(string(msg))
			decoded, err := base64.StdEncoding.DecodeString(base64Str)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(decoded) > 0 && decoded[0] == batchMarker {
				t.Fatalf("packet %d went out batched with no peer capability proven", i)
			}
			if string(decoded) != string(w) {
				t.Errorf("packet %d = %q, want %q", i, decoded, w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("server never received packet %d", i)
		}
	}
}

func TestHandleMessageLearnsPeerBatchingFromKeepalive(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())

	tr.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;---KA---"}]`))
	if tr.peerBatches.Load() {
		t.Fatalf("peerBatches = true after a legacy keepalive with no capability token")
	}

	tr.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;---KA---`+kaBatchCapabilityToken+`"}]`))
	if !tr.peerBatches.Load() {
		t.Fatalf("peerBatches = false after a keepalive carrying kaBatchCapabilityToken")
	}
}

func TestHandleMessageLearnsZstdBatchCapabilityFromKeepalive(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())

	tr.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;---KA---"}]`))
	if tr.peerZstdBatches.Load() {
		t.Fatalf("peerZstdBatches = true after a legacy keepalive with no capability token")
	}

	tr.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;---KA---`+kaBatchCapabilityToken+`"}]`))
	if tr.peerZstdBatches.Load() {
		t.Fatalf("peerZstdBatches = true after a keepalive carrying only the older kaBatchCapabilityToken")
	}
	if !tr.peerBatches.Load() {
		t.Fatalf("peerBatches = false after a keepalive carrying kaBatchCapabilityToken")
	}

	tr2 := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr2.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;---KA---`+kaBatchCapabilityToken+kaZstdBatchCapabilityToken+`"}]`))
	if !tr2.peerBatches.Load() || !tr2.peerZstdBatches.Load() {
		t.Fatalf("peerBatches=%v peerZstdBatches=%v after a keepalive carrying both tokens, want both true",
			tr2.peerBatches.Load(), tr2.peerZstdBatches.Load())
	}
}

func TestSelfCompressReproducesLegacySingleFormatExactly(t *testing.T) {
	received := make(chan []byte, 10)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			received <- msg
		}
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableSelfCompression()
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	// Neither peerBatches nor peerZstdBatches set.
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}
	go tr.writerLoop(queue)

	raw := []byte("a raw tunnel packet, not yet compressed by anything")
	queue <- raw
	wantWire := transport.Compress(raw)

	select {
	case msg := <-received:
		base64Str := tr.extractBase64String(string(msg))
		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !bytes.Equal(decoded, wantWire) {
			t.Fatalf("self-compress fallback wire bytes = %v, want exactly transport.Compress's output %v", decoded, wantWire)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the packet")
	}
}

func TestSelfCompressReproducesLegacyBatchFormatExactly(t *testing.T) {
	received := make(chan []byte, 1)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- msg
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableSelfCompression()
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	tr.peerBatches.Store(true) // old capability confirmed, new one not
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}
	go tr.writerLoop(queue)

	pkts := [][]byte{[]byte("first raw packet"), []byte("a second, slightly longer raw packet")}
	for _, p := range pkts {
		queue <- p
	}

	var wantBlob bytes.Buffer
	wantBlob.WriteByte(batchMarker)
	var lenBuf [2]byte
	for _, p := range pkts {
		compressed := transport.Compress(p)
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(compressed)))
		wantBlob.Write(lenBuf[:])
		wantBlob.Write(compressed)
	}

	select {
	case msg := <-received:
		base64Str := tr.extractBase64String(string(msg))
		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !bytes.Equal(decoded, wantBlob.Bytes()) {
			t.Fatalf("self-compress legacy-batch wire bytes = %v, want exactly %v", decoded, wantBlob.Bytes())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the batch")
	}
}

func TestSelfCompressUsesZstdBatchOnceBothTokensConfirmed(t *testing.T) {
	received := make(chan []byte, 1)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- msg
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableSelfCompression()
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	tr.peerBatches.Store(true)
	tr.peerZstdBatches.Store(true)
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}
	go tr.writerLoop(queue)

	pkts := [][]byte{[]byte("raw packet one"), []byte("raw packet two, a bit longer this time")}
	for _, p := range pkts {
		queue <- p
	}

	select {
	case msg := <-received:
		base64Str := tr.extractBase64String(string(msg))
		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(decoded) == 0 || decoded[0] != zstdBatchMarker {
			t.Fatalf("expected a zstdBatchMarker-prefixed payload, got %v", decoded)
		}
		got, err := transport.DecodeBatch(decoded[1:])
		if err != nil {
			t.Fatalf("transport.DecodeBatch: %v", err)
		}
		if len(got) != len(pkts) {
			t.Fatalf("got %d packets, want %d", len(got), len(pkts))
		}
		for i := range pkts {
			if !bytes.Equal(got[i], pkts[i]) {
				t.Errorf("packet[%d] = %q, want %q (raw, no per-packet compression)", i, got[i], pkts[i])
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the zstd batch")
	}
}

func TestHandleMessageSelfCompressDecodesZstdBatch(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableSelfCompression()

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, append([]byte(nil), data...)) })

	pkt1 := []byte("first raw packet")
	pkt2 := []byte("second raw packet")
	framed := append([]byte{zstdBatchMarker}, transport.EncodeBatch([][]byte{pkt1, pkt2})...)
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(framed) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 2 || string(received[0]) != string(pkt1) || string(received[1]) != string(pkt2) {
		t.Fatalf("received = %v, want [%q %q]", received, pkt1, pkt2)
	}
}

func TestHandleMessageSelfCompressDecodesLegacySingleAndBatch(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableSelfCompression()

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, append([]byte(nil), data...)) })

	raw := []byte("a raw packet from an old-format peer")
	compressed := transport.Compress(raw)
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(compressed) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 1 || string(received[0]) != string(raw) {
		t.Fatalf("single-item: received = %v, want [%q] (decompressed)", received, raw)
	}

	received = nil
	rawA, rawB := []byte("legacy batch packet A"), []byte("legacy batch packet B, a little longer")
	legacyBatch := buildBatch(t, transport.Compress(rawA), transport.Compress(rawB))
	msg2 := `42["message",{"type":"cursor","cursor":"18;` + b64(legacyBatch) + `"}]`
	tr.handleMessage(nil, []byte(msg2))

	if len(received) != 2 || string(received[0]) != string(rawA) || string(received[1]) != string(rawB) {
		t.Fatalf("legacy batch: received = %v, want [%q %q] (decompressed)", received, rawA, rawB)
	}
}

func TestNewYandexDocsTransportDefaultsToNotSelfCompressing(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	if tr.selfCompress {
		t.Fatalf("selfCompress = true on a freshly constructed transport, want false")
	}
}

func TestHandleMessageLearnsEncSelfCompressCapabilityFromKeepalive(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;---KA---`+kaBatchCapabilityToken+kaZstdBatchCapabilityToken+kaEncSelfCompressToken+`"}]`))
	if !tr.peerEncSelfCompress.Load() {
		t.Fatalf("peerEncSelfCompress = false after a keepalive carrying kaEncSelfCompressToken")
	}

	tr2 := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr2.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;---KA---`+kaBatchCapabilityToken+kaZstdBatchCapabilityToken+`"}]`))
	if tr2.peerEncSelfCompress.Load() {
		t.Fatalf("peerEncSelfCompress = true after a keepalive with no kaEncSelfCompressToken")
	}
}

func TestKeepAliveOnlySendsEncTokenWhenEncrypted(t *testing.T) {
	plain := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	plain.EnableSelfCompression()
	if plain.encrypted {
		t.Fatalf("EnableSelfCompression must not set encrypted")
	}

	enc := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	enc.EnableEncryptedSelfCompression("shared-secret-token", false)
	if !enc.encrypted {
		t.Fatalf("EnableEncryptedSelfCompression must set encrypted")
	}
}

func TestEncryptedSelfCompressReproducesLegacySingleFormatExactly(t *testing.T) {
	received := make(chan []byte, 10)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			received <- msg
		}
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	const token = "shared-secret-token"
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableEncryptedSelfCompression(token, false)
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}
	go tr.writerLoop(queue)

	raw := []byte("a raw tunnel packet for an e2e_encryption key")
	queue <- raw

	send, _ := transport.DeriveDirectionalKeys(token, false, "")

	select {
	case msg := <-received:
		base64Str := tr.extractBase64String(string(msg))
		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		plain, err := transport.Open(send, decoded)
		if err != nil {
			t.Fatalf("an unmodified peer (deriving the same key) must be able to decrypt this: %v", err)
		}
		wantPlain := transport.Compress(raw)
		if !bytes.Equal(plain, wantPlain) {
			t.Fatalf("decrypted content = %v, want exactly transport.Compress's output %v", plain, wantPlain)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the packet")
	}
}

func TestEncryptedSelfCompressReproducesLegacyBatchFormatExactly(t *testing.T) {
	received := make(chan []byte, 1)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- msg
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	const token = "shared-secret-token"
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableEncryptedSelfCompression(token, false)
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	tr.peerBatches.Store(true) // legacy batching confirmed, enc-self-compress not
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}
	go tr.writerLoop(queue)

	pkts := [][]byte{[]byte("first raw packet"), []byte("second, slightly longer raw packet")}
	for _, p := range pkts {
		queue <- p
	}

	send, _ := transport.DeriveDirectionalKeys(token, false, "")

	select {
	case msg := <-received:
		base64Str := tr.extractBase64String(string(msg))
		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(decoded) == 0 || decoded[0] != batchMarker {
			t.Fatalf("expected an UNENCRYPTED batchMarker-prefixed outer frame, got %v", decoded)
		}
		items := decodeBatch(decoded[1:])
		if len(items) != len(pkts) {
			t.Fatalf("got %d items, want %d", len(items), len(pkts))
		}
		for i, item := range items {
			plain, err := transport.Open(send, item)
			if err != nil {
				t.Fatalf("item %d: an unmodified peer must be able to decrypt this: %v", i, err)
			}
			want := transport.Compress(pkts[i])
			if !bytes.Equal(plain, want) {
				t.Errorf("item %d decrypted = %v, want %v", i, plain, want)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the batch")
	}
}

func TestEncryptedSelfCompressUsesEncryptedZstdBatchOnceConfirmed(t *testing.T) {
	received := make(chan []byte, 1)
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- msg
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	const token = "shared-secret-token"
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableEncryptedSelfCompression(token, false)
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tr.SetConnected(true)
	tr.peerBatches.Store(true)
	tr.peerZstdBatches.Store(true)
	tr.peerEncSelfCompress.Store(true)
	queue := make(chan []byte, 10)
	tr.session = &DocSession{Conn: conn, WriteQueue: queue}
	go tr.writerLoop(queue)

	pkts := [][]byte{[]byte("raw packet one"), []byte("raw packet two, a bit longer")}
	for _, p := range pkts {
		queue <- p
	}

	send, _ := transport.DeriveDirectionalKeys(token, false, "")

	select {
	case msg := <-received:
		base64Str := tr.extractBase64String(string(msg))
		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(decoded) > 0 && decoded[0] == batchMarker {
			t.Fatalf("outer frame must be fully opaque ciphertext, not a visible batchMarker: %v", decoded)
		}
		plain, err := transport.Open(send, decoded)
		if err != nil {
			t.Fatalf("an unmodified peer with the same key must be able to decrypt this: %v", err)
		}
		if len(plain) == 0 || plain[0] != zstdBatchMarker {
			t.Fatalf("decrypted content should start with zstdBatchMarker, got %v", plain)
		}
		got, err := transport.DecodeBatch(plain[1:])
		if err != nil {
			t.Fatalf("transport.DecodeBatch: %v", err)
		}
		if len(got) != len(pkts) {
			t.Fatalf("got %d packets, want %d", len(got), len(pkts))
		}
		for i := range pkts {
			if !bytes.Equal(got[i], pkts[i]) {
				t.Errorf("packet[%d] = %q, want %q (raw, no per-packet compression)", i, got[i], pkts[i])
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the encrypted zstd batch")
	}
}

func TestHandleEncryptedMessageDecodesLegacySingleAndBatch(t *testing.T) {
	const token = "shared-secret-token"
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableEncryptedSelfCompression(token, true) // exit-node role, decrypts what a client sent

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, append([]byte(nil), data...)) })

	peerSend, _ := transport.DeriveDirectionalKeys(token, false, "")

	raw := []byte("a raw packet from an unmodified encrypted peer")
	sealed, err := transport.Seal(peerSend, 1, transport.Compress(raw))
	if err != nil {
		t.Fatalf("transport.Seal: %v", err)
	}
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(sealed) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 1 || string(received[0]) != string(raw) {
		t.Fatalf("single-item: received = %v, want [%q]", received, raw)
	}

	received = nil
	rawA, rawB := []byte("legacy batch packet A"), []byte("legacy batch packet B, a little longer")
	sealedA, _ := transport.Seal(peerSend, 2, transport.Compress(rawA))
	sealedB, _ := transport.Seal(peerSend, 3, transport.Compress(rawB))
	legacyBatch := buildBatch(t, sealedA, sealedB)
	msg2 := `42["message",{"type":"cursor","cursor":"18;` + b64(legacyBatch) + `"}]`
	tr.handleMessage(nil, []byte(msg2))

	if len(received) != 2 || string(received[0]) != string(rawA) || string(received[1]) != string(rawB) {
		t.Fatalf("legacy batch: received = %v, want [%q %q]", received, rawA, rawB)
	}
}

func TestHandleEncryptedMessageDecodesNewFormat(t *testing.T) {
	const token = "shared-secret-token"
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableEncryptedSelfCompression(token, true)

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, append([]byte(nil), data...)) })

	peerSend, _ := transport.DeriveDirectionalKeys(token, false, "")
	pkt1, pkt2 := []byte("raw one"), []byte("raw two")
	plaintext := append([]byte{zstdBatchMarker}, transport.EncodeBatch([][]byte{pkt1, pkt2})...)
	sealed, err := transport.Seal(peerSend, 1, plaintext)
	if err != nil {
		t.Fatalf("transport.Seal: %v", err)
	}
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(sealed) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 2 || string(received[0]) != string(pkt1) || string(received[1]) != string(pkt2) {
		t.Fatalf("received = %v, want [%q %q]", received, pkt1, pkt2)
	}
}

// Security-critical: anything that fails to decrypt must be dropped, never passed to CallReceive as if it were valid plaintext.
func TestHandleEncryptedMessageDropsUndecryptableData(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.EnableEncryptedSelfCompression("shared-secret-token", true)

	var received [][]byte
	tr.Receive(func(data []byte) { received = append(received, data) })

	// Sealed with a DIFFERENT token - authentication must fail.
	wrongKeySend, _ := transport.DeriveDirectionalKeys("a-different-token", false, "")
	sealed, _ := transport.Seal(wrongKeySend, 1, transport.Compress([]byte("should never be delivered")))
	msg := `42["message",{"type":"cursor","cursor":"18;` + b64(sealed) + `"}]`
	tr.handleMessage(nil, []byte(msg))

	if len(received) != 0 {
		t.Fatalf("handleMessage delivered data that failed to authenticate: %v", received)
	}

	// Plain garbage, too short to even carry a nonce counter.
	tr.handleMessage(nil, []byte(`42["message",{"type":"cursor","cursor":"18;`+b64([]byte("hi"))+`"}]`))
	if len(received) != 0 {
		t.Fatalf("handleMessage delivered undersized garbage: %v", received)
	}
}

// --- normalizeDocURL -------------------------------------------------

func TestNormalizeDocURLRewritesDiskShareLinks(t *testing.T) {
	cases := map[string]string{
		"https://disk.yandex.ru/i/AbCdEfGh123":         "https://docs.yandex.ru/i/AbCdEfGh123",
		"http://disk.yandex.ru/i/xyz?foo=bar":          "http://docs.yandex.ru/i/xyz?foo=bar",
		"https://DISK.YANDEX.RU/i/CaseInsensitiveHost": "https://docs.yandex.ru/i/CaseInsensitiveHost",
		// Already a docs.yandex.ru link - must pass through byte-for-byte.
		"https://docs.yandex.ru/docs/edit?url=abc": "https://docs.yandex.ru/docs/edit?url=abc",
		"https://disk.yandex.ru/d/FolderShareLink": "https://disk.yandex.ru/d/FolderShareLink",
		// Malformed - returned unchanged rather than dropped.
		"not a url at all": "not a url at all",
	}
	for in, want := range cases {
		if got := normalizeDocURL(in); got != want {
			t.Errorf("normalizeDocURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewYandexDocsTransportNormalizesDiskShareLink(t *testing.T) {
	tr := NewYandexDocsTransport("https://disk.yandex.ru/i/AbCdEfGh123", transport.DefaultConfig())
	if tr.url != "https://docs.yandex.ru/i/AbCdEfGh123" {
		t.Errorf("tr.url = %q, want the disk.yandex.ru link rewritten to docs.yandex.ru", tr.url)
	}
}

// --- backoffDelay -----------------------------------------------------

func TestBackoffDelayGrowsAndCaps(t *testing.T) {
	tr := NewYandexDocsTransport("http://example.invalid", transport.TransportConfig{
		ReconnectDelay:      100 * time.Millisecond,
		ReconnectMultiplier: 2,
		MaxReconnectDelay:   1 * time.Second,
	})

	assertInJitterRange(t, tr.backoffDelay(0), 100*time.Millisecond)
	assertInJitterRange(t, tr.backoffDelay(1), 200*time.Millisecond)
	assertInJitterRange(t, tr.backoffDelay(2), 400*time.Millisecond)

	if gotCapped := tr.backoffDelay(10); gotCapped != 1*time.Second {
		t.Errorf("backoffDelay(10) = %v, want capped at 1s even with jitter", gotCapped)
	}
}

func assertInJitterRange(t *testing.T, got, base time.Duration) {
	t.Helper()
	max := time.Duration(float64(base) * 1.5)
	if got < base || got > max {
		t.Errorf("got %v, want in [%v, %v] (base + 0-50%% jitter)", got, base, max)
	}
}

func TestBackoffDelayZeroWhenDisabled(t *testing.T) {
	tr := NewYandexDocsTransport("http://example.invalid", transport.TransportConfig{
		ReconnectDelay: 0,
	})
	if got := tr.backoffDelay(5); got != 0 {
		t.Errorf("backoffDelay with ReconnectDelay=0 = %v, want 0", got)
	}
}

// --- scheduleReconnect: reports a retrying event before backing off -----

func TestScheduleReconnectEmitsRetryingEvent(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.TransportConfig{
		ReconnectDelay:       5 * time.Millisecond,
		ReconnectMultiplier:  1,
		MaxReconnectAttempts: 999,
	})
	tr.BaseTransport.Start() // marks it running without spawning connectToDoc/keepAliveLoop

	var gotCode, gotDetail string
	tr.SetEventCallback(func(code, detail string) {
		gotCode, gotDetail = code, detail
		tr.Stop()
	})

	tr.scheduleReconnect(2, reasonReadError, errors.New("websocket: close 1006 (abnormal closure)"))

	if gotCode != transport.EventRetrying {
		t.Fatalf("code = %q, want %q", gotCode, transport.EventRetrying)
	}
	const want = "3|0|read_error|websocket: close 1006 (abnormal closure)"
	if gotDetail != want {
		t.Errorf("detail = %q, want %q (attempt|delaySeconds|reason|cause)", gotDetail, want)
	}
}

func TestScheduleReconnectStripsNewlinesFromCause(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.TransportConfig{
		ReconnectDelay:       time.Millisecond,
		ReconnectMultiplier:  1,
		MaxReconnectAttempts: 999,
	})
	tr.BaseTransport.Start()

	var gotDetail string
	tr.SetEventCallback(func(code, detail string) {
		gotDetail = detail
		tr.Stop()
	})

	tr.scheduleReconnect(0, reasonFetchFailed, errors.New("line one\nline two"))

	if strings.Contains(gotDetail, "\n") {
		t.Errorf("detail = %q, should not contain a literal newline", gotDetail)
	}
}

func TestProvideCookiesIgnoresIdenticalJar(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	tr.BaseTransport.Start()
	defer tr.Stop()

	tr.ProvideCookies("a=1; b=2")
	afterFirst := tr.captchaCookieGeneration.Load()

	tr.ProvideCookies("a=1; b=2")
	if got := tr.captchaCookieGeneration.Load(); got != afterFirst {
		t.Errorf("generation = %d after a repeated identical push, want %d - a no-op push must not force a reconnect", got, afterFirst)
	}

	tr.ProvideCookies("a=1; b=3")
	if got := tr.captchaCookieGeneration.Load(); got == afterFirst {
		t.Error("generation unchanged after a genuinely different jar")
	}
}

func TestCaptchaCooldownSurvivesForceReconnect(t *testing.T) {
	// A solved-captcha cookie push calls ForceReconnect. If that can close the backoff
	// wake channel for a captcha failure, the cooldown is skipped and the captcha prompt
	// re-opens on every retry (~1s) instead of every 3 minutes.
	assertNoWakeChannel := func(t *testing.T, minDelay time.Duration) {
		t.Helper()
		tr := NewYandexDocsTransport("http://unused.invalid", transport.TransportConfig{
			ReconnectDelay:       time.Millisecond,
			ReconnectMultiplier:  1,
			MaxReconnectAttempts: 999,
		})
		tr.BaseTransport.Start()
		defer tr.Stop()

		retrying := make(chan struct{})
		var once sync.Once
		tr.SetEventCallback(func(code, detail string) {
			if code == transport.EventRetrying {
				once.Do(func() { close(retrying) })
			}
		})

		go tr.scheduleReconnectWithMinDelay(0, reasonCaptchaBlocked, errors.New("captcha"), minDelay)
		select {
		case <-retrying:
		case <-time.After(5 * time.Second):
			t.Fatal("no retrying event")
		}

		tr.Mu.Lock()
		wake := tr.wakeReconnect
		tr.Mu.Unlock()

		if minDelay > 0 && wake != nil {
			t.Error("captcha retry registered a wake channel, so ForceReconnect can cut the cooldown short")
		}
		if minDelay == 0 && wake == nil {
			t.Error("plain backoff registered no wake channel, so ForceReconnect cannot cut it short")
		}
	}

	t.Run("cooldown floor is not interruptible", func(t *testing.T) {
		assertNoWakeChannel(t, captchaCooldown)
	})
	t.Run("plain backoff stays interruptible", func(t *testing.T) {
		assertNoWakeChannel(t, 0)
	})
}

func TestScheduleReconnectDoesNothingWhenNotRunning(t *testing.T) {
	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	// Never started - IsRunning() is false.

	called := false
	tr.SetEventCallback(func(code, detail string) { called = true })

	tr.scheduleReconnect(0, reasonDialFailed, errors.New("connection refused"))

	if called {
		t.Errorf("scheduleReconnect emitted an event even though the transport was never running")
	}
}

// --- fetchDocInfo: must never panic on a malformed page ----------------

func TestFetchDocInfoMalformedConfigReturnsErrorNotPanic(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"no client-config script at all", `<html><body>error page</body></html>`},
		{"invalid json", `<script id="client-config">{not json`},
		{"missing officeActionData", `<script id="client-config">{"foo":1}</script>`},
		{"officeActionData wrong type", `<script id="client-config">{"officeActionData":"nope"}</script>`},
		{"nil editor_config", `<script id="client-config">{"officeActionData":{"editor_config":null}}</script>`},
		{
			"missing balancer_url",
			`<script id="client-config">{"officeActionData":{"editor_config":{"document":{"key":"k"},"token":"t"}}}</script>`,
		},
		{
			"document wrong type",
			`<script id="client-config">{"officeActionData":{"balancer_url":"https://x","editor_config":{"document":"nope","token":"t"}}}</script>`,
		},
		{
			"missing token",
			`<script id="client-config">{"officeActionData":{"balancer_url":"https://x","editor_config":{"document":{"key":"k"}}}}</script>`,
		},
		{
			"missing document key",
			`<script id="client-config">{"officeActionData":{"balancer_url":"https://x","editor_config":{"document":{},"token":"t"}}}</script>`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(c.body))
			}))
			defer srv.Close()

			tr := NewYandexDocsTransport(srv.URL, transport.DefaultConfig())

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("fetchDocInfo panicked: %v", r)
				}
			}()

			_, err := tr.fetchDocInfo(srv.URL, "user1")
			if err == nil {
				t.Fatalf("expected an error for malformed config, got nil")
			}
		})
	}
}

func TestFetchDocInfoValidConfig(t *testing.T) {
	body := `<script id="client-config">{"officeActionData":{"balancer_url":"https://balancer.example",` +
		`"editor_config":{"token":"tok123","document":{"key":"doc-key-1","fileType":"docx","url":"https://x/doc","title":"T"}}}}</script>`

	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.Write([]byte(body))
	}))
	defer srv.Close()

	tr := NewYandexDocsTransport(srv.URL, transport.DefaultConfig())
	info, err := tr.fetchDocInfo(srv.URL, "user1")
	if err != nil {
		t.Fatalf("fetchDocInfo: %v", err)
	}
	if info.Token != "tok123" {
		t.Errorf("Token = %q, want tok123", info.Token)
	}
	if info.DocID != "doc-key-1" {
		t.Errorf("DocID = %q, want doc-key-1", info.DocID)
	}
	if info.Host != "balancer.example" {
		t.Errorf("Host = %q, want balancer.example", info.Host)
	}
	if !strings.Contains(info.WsURL, "doc-key-1") {
		t.Errorf("WsURL = %q, want it to contain the doc key", info.WsURL)
	}

	if ua := gotHeaders.Get("User-Agent"); ua != browserUserAgent {
		t.Errorf("User-Agent = %q, want %q", ua, browserUserAgent)
	}
	if gotHeaders.Get("Accept") == "" {
		t.Error("Accept header missing - real browsers always send one")
	}
	if gotHeaders.Get("Accept-Language") == "" {
		t.Error("Accept-Language header missing - real browsers always send one")
	}
}

// --- performHandshake: exercises the actual protocol sequencing fix ----

var upgrader = websocket.Upgrader{}

func serveHandshakeServer(t *testing.T, respond func(conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("server upgrade: %v", err)
			return
		}
		defer conn.Close()
		respond(conn)
	}))
}

func dialTestServer(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func TestPerformHandshakeSuccessWaitsForAck(t *testing.T) {
	const token = "expected-token-123"
	ackSentAfterConnect := make(chan struct{}, 1)

	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		// 1. engine.io open packet, with explicit ping settings.
		open, _ := json.Marshal(map[string]int{"pingInterval": 25000, "pingTimeout": 5000})
		conn.WriteMessage(websocket.TextMessage, append([]byte("0"), open...))

		// Must receive the namespace-connect BEFORE sending the ack - this is exactly the ordering the original bug violated.
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("server read: %v", err)
			return
		}
		if !strings.HasPrefix(string(msg), "40") {
			t.Errorf("expected a 40<json> namespace-connect frame, got %q", msg)
			return
		}
		var payload struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(msg[2:], &payload); err != nil {
			t.Errorf("server: bad namespace-connect payload: %v", err)
			return
		}
		if payload.Token != token {
			t.Errorf("namespace-connect token = %q, want %q", payload.Token, token)
		}

		conn.WriteMessage(websocket.TextMessage, []byte("2"))
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, pong, err := conn.ReadMessage()
		if err != nil || string(pong) != "3" {
			t.Errorf("expected a pong (%q) in response to the mid-handshake ping, got %q, err=%v", "3", pong, err)
		}

		// 3. Only now send the ack.
		close(ackSentAfterConnect)
		conn.WriteMessage(websocket.TextMessage, []byte(`40{"sid":"server-sid"}`))

		time.Sleep(50 * time.Millisecond) // let the client finish reading before we close
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	readTimeout, err := tr.performHandshake(conn, token)
	if err != nil {
		t.Fatalf("performHandshake: %v", err)
	}
	if readTimeout != 30*time.Second {
		t.Errorf("readTimeout = %v, want 30s (25000+5000ms from the open packet)", readTimeout)
	}

	select {
	case <-ackSentAfterConnect:
	default:
		t.Fatalf("server never reached the point of sending the ack - performHandshake returned too early")
	}
}

func TestPerformHandshakeFailsOnConnectError(t *testing.T) {
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		open, _ := json.Marshal(map[string]int{"pingInterval": 25000, "pingTimeout": 20000})
		conn.WriteMessage(websocket.TextMessage, append([]byte("0"), open...))
		conn.ReadMessage() // the namespace-connect frame
		conn.WriteMessage(websocket.TextMessage, []byte(`44{"message":"not authorized"}`))
		time.Sleep(50 * time.Millisecond)
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	_, err := tr.performHandshake(conn, "any-token")
	if err == nil {
		t.Fatalf("expected an error for a socket.io connect-error response")
	}
}

func TestPerformHandshakeDoesNotSendConnectBeforeOpen(t *testing.T) {
	srv := serveHandshakeServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if _, _, err := conn.ReadMessage(); err == nil {
			t.Errorf("client sent a frame before the server's open packet was ever written")
		}
	})
	defer srv.Close()

	conn := dialTestServer(t, srv)
	defer conn.Close()

	tr := NewYandexDocsTransport("http://unused.invalid", transport.DefaultConfig())
	_, _ = tr.performHandshake(conn, "any-token") // expected to fail once the server closes; that's fine here
}
