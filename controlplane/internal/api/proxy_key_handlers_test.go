package api

import "testing"

func TestParseProxyLinkAcceptsEverySupportedScheme(t *testing.T) {
	cases := []struct {
		link   string
		kind   string
		server string
		port   int
	}{
		{"ss://YWVzLTI1Ni1nY206cGFzcw@1.2.3.4:8388#node-a", "shadowsocks", "1.2.3.4", 8388},
		{"vless://uuid@ru.example:443?security=reality&pbk=K&sni=x.com", "vless", "ru.example", 443},
		{"hysteria2://pw@tr.example:8443?sni=tr.example", "hysteria2", "tr.example", 8443},
		{"hy2://pw@tr2.example:9443", "hysteria2", "tr2.example", 9443},
	}
	for _, tc := range cases {
		kind, server, port, name, err := parseProxyLink(tc.link)
		if err != nil {
			t.Fatalf("%s: %v", tc.link, err)
		}
		if kind != tc.kind || server != tc.server || port != tc.port {
			t.Fatalf("%s -> %s %s:%d", tc.link, kind, server, port)
		}
		if kind == "shadowsocks" && name != "node-a" {
			t.Fatalf("name %q", name)
		}
	}
}

func TestParseProxyLinkRejectsWhatTheNodeCannotDial(t *testing.T) {
	for _, link := range []string{
		"", "trojan://a@b:1", "vmess://a", "ss://YWVzLTI1Ni1nY206cGFzcw@1.2.3.4",
		"not a url at all",
	} {
		if _, _, _, _, err := parseProxyLink(link); err == nil {
			t.Fatalf("%q must be rejected", link)
		}
	}
}

func TestParseProxyLinkKeepsIPv6Server(t *testing.T) {
	kind, server, port, _, err := parseProxyLink("hysteria2://pw@[2001:db8::1]:8443")
	if err != nil {
		t.Fatal(err)
	}
	if kind != "hysteria2" || server != "2001:db8::1" || port != 8443 {
		t.Fatalf("got %s %s:%d", kind, server, port)
	}
}

func TestCleanCarriersDropsBlanksAndRejectsTypos(t *testing.T) {
	got, err := cleanCarriers([]string{"yandex", "", "  ", "mailru", "yandex"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "yandex" || got[1] != "mailru" {
		t.Fatalf("got %v", got)
	}
	if _, err := cleanCarriers([]string{"yandex", "volga"}); err == nil {
		t.Fatal("a carrier the node cannot build a pool for must be rejected")
	}
}

func TestCleanCarriersReturnsAnEmptySliceNotNil(t *testing.T) {
	got, err := cleanCarriers(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("the panel needs an empty list, not null")
	}
}