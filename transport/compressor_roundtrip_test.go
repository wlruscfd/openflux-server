package transport

import (
	"bytes"
	"testing"
)

// The mts client wraps the transport in CompressedTransport, which prefixes every packet with a
// marker byte. An exit worker that does not wrap the same way receives packets that still carry
// that byte, so the IPv4 header sits at offset 1 and the tunnel drops everything. This test
// pins the contract: Compress/Decompress must round-trip, and the wire form must really differ
// from the raw packet, so a missing wrapper on either side cannot go unnoticed.
func TestCompressAddsMarkerThatDecompressStrips(t *testing.T) {
	packet := []byte{0x45, 0x00, 0x00, 0x3c, 0x00, 0x00, 0x40, 0x00, 0x40, 0x06, 0xd1, 0x50}

	wire := Compress(packet)
	if bytes.Equal(wire, packet) {
		t.Fatal("Compress returned the packet unchanged, so a peer without the wrapper would still work")
	}
	if wire[0] == packet[0] {
		t.Errorf("wire[0] = 0x%02x, want a marker byte distinct from the packet's first byte 0x%02x", wire[0], packet[0])
	}

	back, err := Decompress(wire)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(back, packet) {
		t.Errorf("round trip = % x, want % x", back, packet)
	}
}

func TestDecompressRejectsUnmarkedPacket(t *testing.T) {
	// A bare packet arriving from a peer that never wrapped its transport is not valid input.
	packet := []byte{0x45, 0x00, 0x00, 0x3c, 0x00, 0x00, 0x40, 0x00, 0x40, 0x06, 0xd1, 0x50}
	if _, err := Decompress(packet); err == nil {
		t.Error("Decompress accepted an unmarked packet, so a missing wrapper would pass silently")
	}
}
