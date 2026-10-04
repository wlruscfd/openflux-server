package tunnel

import "github.com/p1neappleXpress/OpenFlux/network"

// rewriteL4Checksum must match across both raw-socket implementations (linux/darwin) and both NAT paths - a packet with a wrong checksum is silently dropped by the receiver.
func rewriteL4Checksum(l4 []byte, proto byte, srcIP, dstIP [4]byte) {
	switch proto {
	case 6:
		if len(l4) < 18 {
			return
		}
		l4[16], l4[17] = 0, 0
		sum := network.TCPChecksum(l4, srcIP, dstIP)
		l4[16], l4[17] = byte(sum>>8), byte(sum&0xFF)
	case 17:
		if len(l4) < 8 {
			return
		}
		l4[6], l4[7] = 0, 0
		sum := network.UDPChecksum(l4, srcIP, dstIP)
		// A zero UDP checksum means "not computed", so a computed zero is transmitted as 0xFFFF (RFC 768) or the receiver thinks there is nothing to verify.
		if sum == 0 {
			sum = 0xFFFF
		}
		l4[6], l4[7] = byte(sum>>8), byte(sum&0xFF)
	}
}
