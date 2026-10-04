package transport

import "fmt"

func EncodeBatch(pkts [][]byte) []byte { return encodeBatch(pkts) }

func DecodeBatch(data []byte) ([][]byte, error) { return decodeBatch(data) }

func IsBatchFrame(data []byte) bool {
	return len(data) >= 2 && data[0] == batchFormatVersion
}

func compress(data []byte) []byte { return Compress(data) }

func decompress(data []byte) ([]byte, error) { return Decompress(data) }

func WrapCodec(inner Transport, codec string) (Transport, error) {
	switch codec {
	case "", "legacy":
		return NewCompressedTransport(inner), nil
	case "batched":
		return NewBatchedTransport(inner), nil
	default:
		return nil, fmt.Errorf("unknown codec %q (want legacy|batched)", codec)
	}
}
