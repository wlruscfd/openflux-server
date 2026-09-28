package transport

import (
	"strings"
	"testing"
)

func TestDecodeBatchRejectsOversizedFrame(t *testing.T) {
	oversized := make([]byte, maxFrameBytes+3)
	oversized[0] = batchFormatVersion
	if _, err := DecodeBatch(oversized); err == nil {
		t.Fatal("a frame larger than the cap must be rejected before decoding")
	} else if !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeBatchRejectsUnknownFlags(t *testing.T) {
	frame := []byte{batchFormatVersion, batchFlagZstd | 0x80, 0x00}
	if _, err := DecodeBatch(frame); err == nil {
		t.Fatal("unknown flag bits must be rejected")
	} else if !strings.Contains(err.Error(), "unknown batch flags") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeBatchRejectsTooManyRecords(t *testing.T) {
	framed := make([]byte, 0, (maxFrameRecords+1)*3)
	for i := 0; i <= maxFrameRecords; i++ {
		framed = append(framed, 0x00, 0x01, 0xAA)
	}
	frame := append([]byte{batchFormatVersion, 0}, framed...)
	if _, err := DecodeBatch(frame); err == nil {
		t.Fatalf("a frame with %d records must be rejected", maxFrameRecords+1)
	} else if !strings.Contains(err.Error(), "record limit") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeBatchStillAcceptsNormalBatches(t *testing.T) {
	pkts := [][]byte{[]byte("a"), []byte("bb"), make([]byte, 1400)}
	got, err := DecodeBatch(EncodeBatch(pkts))
	if err != nil {
		t.Fatalf("normal batch rejected: %v", err)
	}
	if len(got) != len(pkts) {
		t.Fatalf("got %d packets, want %d", len(got), len(pkts))
	}
	for i := range pkts {
		if string(got[i]) != string(pkts[i]) {
			t.Fatalf("packet %d = %q, want %q", i, got[i], pkts[i])
		}
	}
}
