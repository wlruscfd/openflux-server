package outbound

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type Kind string

const (
	KindShadowsocks Kind = "shadowsocks"
	KindVLESS       Kind = "vless"
	KindHysteria2   Kind = "hysteria2"
)

func Kinds() []Kind { return []Kind{KindShadowsocks, KindVLESS, KindHysteria2} }

type Key struct {
	ID       string
	Name     string
	Link     string
	Kind     Kind
	Priority int

	Server   string
	Port     int
	Method   string
	Password string
	UUID     string
	Flow     string

	SNI         string
	Fingerprint string
	PublicKey   string
	ShortID     string
	Reality     bool
	Insecure    bool
	// NoTLS is a vless link that asked for security=none: the whole point of that mode is a
	// plaintext hop, so sing-box must be told not to wrap it.
	NoTLS bool

	// Transport mirrors the link's own type= parameter. Leaving it out is what makes a working
	// server look dead: the client opens a bare TCP stream to something expecting WebSocket.
	TransportType string
	Path          string
	Host          string
	ServiceName   string
	Mode          string
	HeaderType    string
	ALPN          []string
}

func (k Key) Endpoint() string { return net.JoinHostPort(k.Server, strconv.Itoa(k.Port)) }

func (k Key) Valid() error {
	if k.Server == "" {
		return fmt.Errorf("no server address")
	}
	if k.Port <= 0 || k.Port > 65535 {
		return fmt.Errorf("bad server port %d", k.Port)
	}
	switch k.Kind {
	case KindShadowsocks:
		if k.Method == "" || k.Password == "" {
			return fmt.Errorf("shadowsocks link without a method and a password")
		}
	case KindVLESS:
		if k.UUID == "" {
			return fmt.Errorf("vless link without a uuid")
		}
		if k.Reality && (k.PublicKey == "" || k.SNI == "") {
			return fmt.Errorf("vless reality link without a public key and an sni")
		}
	case KindHysteria2:
		if k.Password == "" {
			return fmt.Errorf("hysteria2 link without a password")
		}
	default:
		return fmt.Errorf("unsupported proxy scheme %q", k.Kind)
	}
	return nil
}

func ParseLink(raw string) (Key, error) {
	link := strings.TrimSpace(raw)
	if link == "" {
		return Key{}, fmt.Errorf("empty link")
	}
	// Lists published as HTML leave their separators escaped, so "&amp;security=tls" arrives as one
	// parameter name and every option after it is lost.
	link = strings.ReplaceAll(link, "&amp;", "&")
	u, err := url.Parse(link)
	if err != nil {
		return Key{}, fmt.Errorf("proxy link: %w", err)
	}
	var k Key
	switch u.Scheme {
	case "ss":
		k, err = parseShadowsocks(link, u)
	case "vless":
		k, err = parseVLESS(u)
	case "hysteria2", "hy2":
		k, err = parseHysteria2(u)
	default:
		return Key{}, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	if err != nil {
		return Key{}, err
	}
	k.Link = link
	k.Name = fragmentName(u)
	return k, nil
}

func fragmentName(u *url.URL) string {
	name := u.Fragment
	if i := strings.IndexByte(name, '?'); i >= 0 {
		name = name[:i]
	}
	return name
}

func decodeBase64(s string) (string, error) {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "-", "+")
	s = strings.ReplaceAll(s, "_", "/")
	if pad := len(s) % 4; pad != 0 {
		s += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func hostPort(u *url.URL) (string, int, error) {
	host, port := u.Hostname(), u.Port()
	if host == "" {
		return "", 0, fmt.Errorf("link without a host")
	}
	if port == "" {
		return "", 0, fmt.Errorf("link without a port")
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, fmt.Errorf("bad port %q", port)
	}
	return host, n, nil
}

func parseShadowsocks(raw string, u *url.URL) (Key, error) {
	if u.User == nil {
		payload := raw
		payload = strings.TrimPrefix(payload, "ss://")
		payload = strings.SplitN(payload, "#", 2)[0]
		dec, err := decodeBase64(payload)
		if err != nil {
			return Key{}, fmt.Errorf("shadowsocks link: %w", err)
		}
		inner, err := url.Parse("ss://" + dec)
		if err != nil {
			return Key{}, fmt.Errorf("shadowsocks link: %w", err)
		}
		u = inner
	}
	server, port, err := hostPort(u)
	if err != nil {
		return Key{}, fmt.Errorf("shadowsocks: %w", err)
	}
	k := Key{Kind: KindShadowsocks, Server: server, Port: port}
	if u.User == nil {
		return Key{}, fmt.Errorf("shadowsocks link without credentials")
	}
	password, hasPassword := u.User.Password()
	userinfo := u.User.Username()
	if dec, err := decodeBase64(userinfo); err == nil && strings.Contains(dec, ":") {
		k.Method, k.Password, _ = strings.Cut(dec, ":")
	} else if hasPassword {
		k.Method, k.Password = userinfo, password
	}
	return k, nil
}

func parseVLESS(u *url.URL) (Key, error) {
	server, port, err := hostPort(u)
	if err != nil {
		return Key{}, fmt.Errorf("vless: %w", err)
	}
	q := u.Query()
	k := Key{
		Kind:     KindVLESS,
		Server:   server,
		Port:     port,
		UUID:     u.User.Username(),
		Flow:     q.Get("flow"),
		SNI:      firstNonEmpty(q.Get("sni"), q.Get("host")),
		Insecure: q.Get("allowInsecure") == "1" || q.Get("insecure") == "1",
	}
	switch strings.ToLower(q.Get("security")) {
	case "reality":
		k.Reality = true
		k.PublicKey = q.Get("pbk")
		k.ShortID = q.Get("sid")
	case "tls", "xtls":
	case "none", "":
		k.NoTLS = true
	default:
		return Key{}, fmt.Errorf("vless security %q is not supported", q.Get("security"))
	}
	k.Fingerprint = firstNonEmpty(q.Get("fp"), "chrome")
	k.ALPN = splitALPN(q.Get("alpn"))

	// Without the transport the client speaks plain VLESS over TCP while the server waits for
	// WebSocket, gRPC or an HTTP upgrade, and the handshake reads back as noise. Almost every
	// published link sets one, so it is not an exotic case.
	k.TransportType = strings.ToLower(firstNonEmpty(q.Get("type"), "tcp"))
	k.Path = q.Get("path")
	k.Host = firstNonEmpty(q.Get("host"), q.Get("sni"))
	k.ServiceName = firstNonEmpty(q.Get("serviceName"), q.Get("servicename"))
	k.Mode = q.Get("mode")
	k.HeaderType = strings.ToLower(q.Get("headerType"))
	return k, nil
}

func splitALPN(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseHysteria2(u *url.URL) (Key, error) {
	server, port, err := hostPort(u)
	if err != nil {
		return Key{}, fmt.Errorf("hysteria2: %w", err)
	}
	q := u.Query()
	password, _ := u.User.Password()
	k := Key{
		Kind:     KindHysteria2,
		Server:   server,
		Port:     port,
		Password: password,
		SNI:      firstNonEmpty(q.Get("sni"), q.Get("host")),
		Insecure: q.Get("insecure") == "1" || q.Get("allowInsecure") == "1",
	}
	if k.Password == "" {
		k.Password = u.User.Username()
	}
	return k, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
