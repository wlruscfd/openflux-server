package gateway

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

func generateSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestParseDNSUpstream(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantKind dnsUpstreamKind
		wantAddr string
		wantHost string
		wantPath string
	}{
		{"empty defaults to yandex dns", "", dnsUpstreamPlain, "77.88.8.8:53", "", ""},
		{"bare host gets :53 appended", "9.9.9.9", dnsUpstreamPlain, "9.9.9.9:53", "", ""},
		{"host:port left untouched", "8.8.8.8:5353", dnsUpstreamPlain, "8.8.8.8:5353", "", ""},
		{"tls scheme, bare host gets :853", "tls://1.1.1.1", dnsUpstreamDoT, "1.1.1.1:853", "1.1.1.1", ""},
		{"tls scheme, explicit port kept", "tls://dns.example:8853", dnsUpstreamDoT, "dns.example:8853", "dns.example", ""},
		{"https scheme, default port and path", "https://1.1.1.1", dnsUpstreamDoH, "1.1.1.1:443", "1.1.1.1", "/dns-query"},
		{"https scheme, explicit port and path", "https://dns.example:8443/resolve", dnsUpstreamDoH, "dns.example:8443", "dns.example", "/resolve"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseDNSUpstream(c.input)
			if got.kind != c.wantKind {
				t.Errorf("kind = %v, want %v", got.kind, c.wantKind)
			}
			if got.addr != c.wantAddr {
				t.Errorf("addr = %q, want %q", got.addr, c.wantAddr)
			}
			if got.host != c.wantHost {
				t.Errorf("host = %q, want %q", got.host, c.wantHost)
			}
			if got.path != c.wantPath {
				t.Errorf("path = %q, want %q", got.path, c.wantPath)
			}
		})
	}
}

type tcpDialerToAddr struct{ addr string }

func (d tcpDialerToAddr) DialTCP(string) (net.Conn, error) { return net.Dial("tcp", d.addr) }
func (d tcpDialerToAddr) DialUDP(string) (net.Conn, error) {
	return nil, fmt.Errorf("not implemented")
}

func selfSignedTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	cert := generateSelfSignedCert(t)
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

func TestQueryDoTRoundTrip(t *testing.T) {
	ln, err := tls.Listen("tcp", "127.0.0.1:0", selfSignedTLSConfig(t))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	wantReply := []byte("a fake dns reply")
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		query, err := readDNSOverTCP(conn)
		if err != nil {
			t.Errorf("server: read query: %v", err)
			return
		}
		if string(query) != "a fake dns query" {
			t.Errorf("server saw query %q", query)
		}
		if err := writeDNSOverTCP(conn, wantReply); err != nil {
			t.Errorf("server: write reply: %v", err)
		}
	}()

	s := &Server{
		dialer:               tcpDialerToAddr{addr: ln.Addr().String()},
		dnsUpstreamCfg:       dnsUpstreamConfig{kind: dnsUpstreamDoT, addr: ln.Addr().String(), host: "127.0.0.1"},
		dnsUpstreamTLSConfig: &tls.Config{InsecureSkipVerify: true},
	}

	reply, err := s.queryUpstream([]byte("a fake dns query"))
	if err != nil {
		t.Fatalf("queryUpstream: %v", err)
	}
	if string(reply) != string(wantReply) {
		t.Errorf("reply = %q, want %q", reply, wantReply)
	}
}

func TestQueryDoHRoundTrip(t *testing.T) {
	ln, err := tls.Listen("tcp", "127.0.0.1:0", selfSignedTLSConfig(t))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	wantReply := []byte("a fake doh reply")
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			t.Errorf("server: read request: %v", err)
			return
		}
		req := string(buf[:n])
		if want := "POST /dns-query HTTP/1.1\r\n"; len(req) < len(want) || req[:len(want)] != want {
			t.Errorf("server saw request line %q, want prefix %q", req, want)
		}
		body := wantReply
		fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/dns-message\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", len(body))
		conn.Write(body)
		io.Copy(io.Discard, conn)
	}()

	s := &Server{
		dialer:               tcpDialerToAddr{addr: ln.Addr().String()},
		dnsUpstreamCfg:       dnsUpstreamConfig{kind: dnsUpstreamDoH, addr: ln.Addr().String(), host: "127.0.0.1", path: "/dns-query"},
		dnsUpstreamTLSConfig: &tls.Config{InsecureSkipVerify: true},
	}

	reply, err := s.queryUpstream([]byte("a fake dns query"))
	if err != nil {
		t.Fatalf("queryUpstream: %v", err)
	}
	if string(reply) != string(wantReply) {
		t.Errorf("reply = %q, want %q", reply, wantReply)
	}
}

func TestQueryDoHRejectsNonOKStatus(t *testing.T) {
	ln, err := tls.Listen("tcp", "127.0.0.1:0", selfSignedTLSConfig(t))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 4096)
		if _, err := conn.Read(buf); err != nil {
			t.Errorf("server: read request: %v", err)
			return
		}
		conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
	}()

	s := &Server{
		dialer:               tcpDialerToAddr{addr: ln.Addr().String()},
		dnsUpstreamCfg:       dnsUpstreamConfig{kind: dnsUpstreamDoH, addr: ln.Addr().String(), host: "127.0.0.1", path: "/dns-query"},
		dnsUpstreamTLSConfig: &tls.Config{InsecureSkipVerify: true},
	}

	if _, err := s.queryUpstream([]byte("q")); err == nil {
		t.Fatalf("expected an error for a non-200 DoH response")
	}
}
