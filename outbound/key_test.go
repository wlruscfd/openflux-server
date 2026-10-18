package outbound

import (
	"encoding/base64"
	"testing"
)

func TestParseShadowsocksSIP002(t *testing.T) {
	k, err := ParseLink("ss://YWVzLTI1Ni1nY206c2VjcmV0@1.2.3.4:8388#node-a")
	if err != nil {
		t.Fatal(err)
	}
	if k.Kind != KindShadowsocks || k.Server != "1.2.3.4" || k.Port != 8388 {
		t.Fatalf("got %+v", k)
	}
	if k.Method != "aes-256-gcm" || k.Password != "secret" {
		t.Fatalf("got %+v", k)
	}
	if k.Name != "node-a" {
		t.Fatalf("name %q", k.Name)
	}
	if err := k.Valid(); err != nil {
		t.Fatal(err)
	}
}

func TestParseShadowsocksPlainUserinfo(t *testing.T) {
	k, err := ParseLink("ss://chacha20-ietf-poly1305:pw@host.example:443#plain")
	if err != nil {
		t.Fatal(err)
	}
	if k.Method != "chacha20-ietf-poly1305" || k.Password != "pw" || k.Port != 443 {
		t.Fatalf("got %+v", k)
	}
}

func TestParseShadowsocksLegacyWholeBodyBase64(t *testing.T) {
	plain := "aes-256-gcm:pass@1.2.3.4:8388"
	k, err := ParseLink("ss://" + base64.StdEncoding.EncodeToString([]byte(plain)) + "#legacy")
	if err != nil {
		t.Fatal(err)
	}
	if k.Method != "aes-256-gcm" || k.Password != "pass" || k.Server != "1.2.3.4" || k.Port != 8388 {
		t.Fatalf("got %+v", k)
	}
}

func TestParseVLESSReality(t *testing.T) {
	link := "vless://6f1c1e2a-1111-2222-3333-444455556666@ru.example:443" +
		"?encryption=none&security=reality&pbk=PUBKEY&sid=ab12&sni=www.microsoft.com&fp=chrome&flow=xtls-rprx-vision#vless-reality"
	k, err := ParseLink(link)
	if err != nil {
		t.Fatal(err)
	}
	if k.Kind != KindVLESS || k.UUID != "6f1c1e2a-1111-2222-3333-444455556666" {
		t.Fatalf("got %+v", k)
	}
	if !k.Reality || k.PublicKey != "PUBKEY" || k.ShortID != "ab12" {
		t.Fatalf("reality fields: %+v", k)
	}
	if k.SNI != "www.microsoft.com" || k.Fingerprint != "chrome" || k.Flow != "xtls-rprx-vision" {
		t.Fatalf("got %+v", k)
	}
	if err := k.Valid(); err != nil {
		t.Fatal(err)
	}
}

func TestParseVLESSRealityNeedsPublicKey(t *testing.T) {
	link := "vless://uuid@ru.example:443?security=reality&sni=x.com"
	k, err := ParseLink(link)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.Valid(); err == nil {
		t.Fatal("reality without a public key must not validate")
	}
}

func TestParseHysteria2(t *testing.T) {
	k, err := ParseLink("hysteria2://mypass@tr.example:8443?sni=tr.example&insecure=1#hy2")
	if err != nil {
		t.Fatal(err)
	}
	if k.Kind != KindHysteria2 || k.Password != "mypass" || k.Server != "tr.example" || k.Port != 8443 {
		t.Fatalf("got %+v", k)
	}
	if !k.Insecure || k.SNI != "tr.example" {
		t.Fatalf("got %+v", k)
	}
	if err := k.Valid(); err != nil {
		t.Fatal(err)
	}
}

func TestParseVLESSSecurityNoneIsPlaintext(t *testing.T) {
	k, err := ParseLink("vless://uuid-1@1.2.3.4:443?security=none&type=ws&path=%2F&host=cdn.example")
	if err != nil {
		t.Fatal(err)
	}
	if !k.NoTLS || k.Reality {
		t.Fatalf("got %+v", k)
	}
	if err := k.Valid(); err != nil {
		t.Fatalf("a plaintext vless link is legitimate: %v", err)
	}
}

func TestParseUnescapesHTMLSeparators(t *testing.T) {
	link := "vless://uuid-2@1.2.3.4:443?security=reality&amp;pbk=KEY&amp;sni=www.microsoft.com&amp;fp=chrome"
	k, err := ParseLink(link)
	if err != nil {
		t.Fatal(err)
	}
	if !k.Reality || k.PublicKey != "KEY" || k.SNI != "www.microsoft.com" {
		t.Fatalf("escaped separators lost the options: %+v", k)
	}
}

func TestParseVLESSTransportAndTLSFingerprint(t *testing.T) {
	link := "vless://uuid-3@1.2.3.4:443?path=%2Fws&security=tls&fp=chrome&type=ws" +
		"&host=Appxdn.global.ssl.fastly.net&alpn=h2%2Chttp%2F1.1"
	k, err := ParseLink(link)
	if err != nil {
		t.Fatal(err)
	}
	if k.TransportType != "ws" || k.Path != "/ws" || k.Host != "Appxdn.global.ssl.fastly.net" {
		t.Fatalf("got %+v", k)
	}
	if k.Fingerprint != "chrome" || len(k.ALPN) != 2 {
		t.Fatalf("got %+v", k)
	}
	raw, err := singboxOutbound(k, "t")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{`"transport"`, `"type":"ws"`, `"Host"`, `"utls"`, `"alpn"`} {
		if !contains(text, want) {
			t.Fatalf("config is missing %s: %s", want, text)
		}
	}
}

func TestSingboxTransportGRPC(t *testing.T) {
	k, err := ParseLink("vless://uuid-4@1.2.3.4:443?security=tls&type=grpc&serviceName=gg&mode=gun")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := singboxOutbound(k, "t")
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(raw), `"service_name":"gg"`) {
		t.Fatalf("grpc service name lost: %s", raw)
	}
}

func TestPlainTCPVLESSHasNoTransport(t *testing.T) {
	k, err := ParseLink("vless://uuid-5@1.2.3.4:443?security=reality&pbk=K&sni=x.com")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := singboxOutbound(k, "t")
	if err != nil {
		t.Fatal(err)
	}
	if contains(string(raw), `"transport"`) {
		t.Fatalf("a bare tcp link must not gain a transport block: %s", raw)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestParseRejectsUnknownScheme(t *testing.T) {
	if _, err := ParseLink("trojan://a@b:1"); err == nil {
		t.Fatal("trojan links are not supported")
	}
	if _, err := ParseLink(""); err == nil {
		t.Fatal("empty link must fail")
	}
}

func TestEndpointFormatsIPv6(t *testing.T) {
	k, err := ParseLink("hysteria2://pw@[2001:db8::1]:8443")
	if err != nil {
		t.Fatal(err)
	}
	if got := k.Endpoint(); got != "[2001:db8::1]:8443" {
		t.Fatalf("endpoint %q", got)
	}
}
