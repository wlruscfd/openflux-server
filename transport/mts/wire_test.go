package mts

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

// The one-way mts fault that cost the most time was a client that wrapped its transport in
// CompressedTransport and an exit that did not, so every packet reached the tunnel with the
// compression marker still attached and the IPv4 header at offset 1. These tests walk the real
// wire path in both directions, through the same helpers the transport uses, so an asymmetry
// between the two ends fails here instead of in production.

func mtsSamplePacket(seed byte) []byte {
	p := make([]byte, 60)
	p[0] = 0x45
	p[1] = 0x00
	p[2] = 0x00
	p[3] = 0x3c
	p[8] = 0x40
	p[9] = 0x06
	p[12] = 10
	p[13] = 10
	p[14] = 10
	p[15] = 2
	for i := 20; i < len(p); i++ {
		p[i] = seed + byte(i)
	}
	return p
}

func wireRoundTrip(t *testing.T, packets [][]byte, exitUsesCompressor bool) [][]byte {
	t.Helper()

	frame := transport.EncodeBatch(packets)

	// The client wraps its transport, so the frame is compressed before it reaches the cursor.
	wire := transport.Compress(frame)
	msg := buildCursorMessage("sender-1", "guest", "token", wire)
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	encoded, sender, ok := parseCursorPayload(raw)
	if !ok {
		t.Fatal("the exit rejected a message the client produced")
	}
	if sender != "sender-1" {
		t.Errorf("sender = %q, want sender-1", sender)
	}

	received, err := decodeCursorFrame(encoded, exitUsesCompressor)
	if err != nil {
		t.Fatalf("exit decode: %v", err)
	}
	return received
}

func TestMTSWirePathRoundTripsPackets(t *testing.T) {
	packets := [][]byte{mtsSamplePacket(1), mtsSamplePacket(9), mtsSamplePacket(77)}

	got := wireRoundTrip(t, packets, true)
	if len(got) != len(packets) {
		t.Fatalf("decoded %d packets, want %d", len(got), len(packets))
	}
	for i := range packets {
		if !bytes.Equal(got[i], packets[i]) {
			t.Errorf("packet %d = % x, want % x", i, got[i], packets[i])
		}
	}
}

func TestMTSWirePathPreservesOrder(t *testing.T) {
	var packets [][]byte
	for i := 0; i < 12; i++ {
		packets = append(packets, mtsSamplePacket(byte(i*3+1)))
	}
	got := wireRoundTrip(t, packets, true)
	if len(got) != len(packets) {
		t.Fatalf("decoded %d packets, want %d", len(got), len(packets))
	}
	for i := range packets {
		if !bytes.Equal(got[i], packets[i]) {
			t.Fatalf("packet %d arrived out of order or altered", i)
		}
	}
}

// A packet that went through the compressor must not reach the tunnel still carrying the marker.
// This is the exact failure that made mts one-way, expressed as a property of the wire.
func TestCompressedFrameDoesNotLeakMarkerIntoPackets(t *testing.T) {
	packet := mtsSamplePacket(3)

	got := wireRoundTrip(t, [][]byte{packet}, true)
	if len(got) != 1 {
		t.Fatalf("decoded %d packets, want 1", len(got))
	}
	if got[0][0] != 0x45 {
		t.Errorf("first byte = 0x%02x, want 0x45: the compression marker leaked into the packet", got[0][0])
	}
	if got[0][9] != 0x06 {
		t.Errorf("protocol byte = 0x%02x, want 0x06: header is shifted", got[0][9])
	}
}

// An exit that forgets the compressor wrapper does not fail loudly. With a small frame the two
// happen to be compatible, because the marker byte and the batch version are both zero and the
// inner flags are zero too, so the shift cancels out. Once the batch is large enough to actually
// compress, the flags stop being zero, the shift stops cancelling, and what reaches the tunnel is
// a corrupt packet with its header moved. That is the exact shape of the one-way fault.
func TestMissingCompressorWrapperCorruptsCompressedFrames(t *testing.T) {
	packets := make([][]byte, 0, 8)
	for i := 0; i < 8; i++ {
		p := make([]byte, 1400)
		p[0] = 0x45
		p[1] = 0x00
		p[2] = 0x05
		p[3] = 0x78
		p[8] = 0x40
		p[9] = 0x06
		p[12] = 10
		p[13] = 10
		p[14] = 10
		p[15] = 2
		for j := 20; j < len(p); j++ {
			p[j] = byte(i + j%7)
		}
		packets = append(packets, p)
	}

	frame := transport.EncodeBatch(packets)
	if len(frame) == 0 {
		t.Fatal("empty frame")
	}
	wire := transport.Compress(frame)
	encoded, ok := payloadOf(wire)
	if !ok {
		t.Fatal("build payload")
	}

	// The path production actually uses must always recover the packets intact.
	right, err := decodeCursorFrame(encoded, true)
	if err != nil {
		t.Fatalf("wrapped decode: %v", err)
	}
	if len(right) != len(packets) {
		t.Fatalf("decoded %d packets, want %d", len(right), len(packets))
	}
	for i := range packets {
		if !bytes.Equal(right[i], packets[i]) {
			t.Fatalf("packet %d differs after a wrapped round trip", i)
		}
	}

	// The exit that forgot the wrapper: it must not hand the tunnel something that merely looks
	// like an IP packet. Either it errors, or it produces bytes that are not the originals.
	wrong, werr := decodeCursorFrame(encoded, false)
	if werr != nil {
		return
	}
	for i, p := range wrong {
		if len(p) >= 20 && p[0] == 0x45 && (p[9] == 6 || p[9] == 17) {
			t.Errorf("packet %d survived the missing wrapper intact, so this test no longer proves the failure mode", i)
		}
	}
}

func payloadOf(frame []byte) (string, bool) {
	msg := buildCursorMessage("sender-1", "guest", "token", frame)
	raw, err := json.Marshal(msg)
	if err != nil {
		return "", false
	}
	encoded, _, ok := parseCursorPayload(raw)
	return encoded, ok
}
