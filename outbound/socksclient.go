package outbound

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

const (
	socks5Version   = 0x05
	socksNoAuth     = 0x00
	socksCmdConnect = 0x01
	socksAtypIPv4   = 0x01
	socksAtypDomain = 0x03
	socksAtypIPv6   = 0x04

	socksReplySuccess             = 0x00
	socksReplyGeneralFailure      = 0x01
	socksReplyHostUnreachable     = 0x04
	socksReplyConnRefused         = 0x05
	socksReplyTTLExpired          = 0x06
	socksReplyCommandNotSupported = 0x07
	socksReplyAddrNotSupported    = 0x08
)

var socksReplyText = map[byte]string{
	socksReplyGeneralFailure:      "general SOCKS server failure",
	socksReplyHostUnreachable:     "host unreachable",
	socksReplyConnRefused:         "connection refused",
	socksReplyTTLExpired:          "TTL expired",
	socksReplyCommandNotSupported: "command not supported",
	socksReplyAddrNotSupported:    "address type not supported",
}

type socksDialer struct {
	addr    string
	direct  Dialer
	timeout time.Duration
}

func newSocksDialer(addr string, direct Dialer, timeout time.Duration) *socksDialer {
	return &socksDialer{addr: addr, direct: direct, timeout: timeout}
}

func (d *socksDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("proxy: network %q is not supported, carriers dial tcp", network)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := d.direct.DialContext(ctx, "tcp", d.addr)
	if err != nil {
		return nil, fmt.Errorf("proxy %s: %w", d.addr, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else if d.timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(d.timeout))
	}
	if err := d.handshake(conn, address); err != nil {
		conn.Close()
		return nil, fmt.Errorf("proxy %s: %w", d.addr, err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func (d *socksDialer) handshake(conn net.Conn, address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return fmt.Errorf("bad target port %q", portText)
	}
	greeting := []byte{socks5Version, 1, socksNoAuth}
	if _, err := conn.Write(greeting); err != nil {
		return fmt.Errorf("greeting: %w", err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("greeting reply: %w", err)
	}
	if reply[0] != socks5Version {
		return fmt.Errorf("unexpected SOCKS version %d", reply[0])
	}
	if reply[1] != socksNoAuth {
		return fmt.Errorf("SOCKS server wants auth method %d", reply[1])
	}

	request := []byte{socks5Version, socksCmdConnect, 0x00}
	request = append(request, encodeSocksAddress(host, port)...)
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("connect request: %w", err)
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return fmt.Errorf("connect reply: %w", err)
	}
	if head[0] != socks5Version {
		return fmt.Errorf("unexpected SOCKS version %d in reply", head[0])
	}
	if head[1] != socksReplySuccess {
		if text, ok := socksReplyText[head[1]]; ok {
			return fmt.Errorf("%s (%s:%d)", text, host, port)
		}
		return fmt.Errorf("SOCKS connect to %s:%d failed with code %d", host, port, head[1])
	}
	if _, err := decodeSocksAddress(conn, head[3]); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil {
		return err
	}
	return nil
}

func encodeSocksAddress(host string, port int) []byte {
	var out []byte
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			out = append(append([]byte{socksAtypIPv4}, v4...), 0, 0)
			out[len(out)-2], out[len(out)-1] = byte(port>>8), byte(port)
			return out
		}
		out = append(append([]byte{socksAtypIPv6}, ip.To16()...), 0, 0)
		out[len(out)-2], out[len(out)-1] = byte(port>>8), byte(port)
		return out
	}
	if len(host) > 255 {
		host = host[:255]
	}
	out = append([]byte{socksAtypDomain, byte(len(host))}, []byte(host)...)
	return append(out, byte(port>>8), byte(port))
}

func decodeSocksAddress(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case socksAtypIPv4:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return net.IP(buf).String(), nil
	case socksAtypIPv6:
		buf := make([]byte, 16)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return net.IP(buf).String(), nil
	case socksAtypDomain:
		n := make([]byte, 1)
		if _, err := io.ReadFull(r, n); err != nil {
			return "", err
		}
		buf := make([]byte, n[0])
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf), nil
	default:
		return "", fmt.Errorf("unknown SOCKS address type %d", atyp)
	}
}
