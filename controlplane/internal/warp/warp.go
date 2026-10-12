package warp

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

var apiBase = "https://api.cloudflareclient.com/v0a5641"

const (
	userAgent     = "1.1.1.1/6.38.9-5641 (Android 16.0.0)"
	clientVersion = "a-6.38.9-5641"
)

type Account struct {
	DeviceID      string
	PrivateKey    string
	PeerPublicKey string
	Endpoint      string
	AddressV4     string
	AddressV6     string
}

type registerRequest struct {
	Key        string `json:"key"`
	InstallID  string `json:"install_id"`
	FcmToken   string `json:"fcm_token"`
	Tos        string `json:"tos"`
	Model      string `json:"model"`
	Serial     string `json:"serial_number"`
	OsVersion  string `json:"os_version"`
	KeyType    string `json:"key_type"`
	TunnelType string `json:"tunnel_type"`
	Locale     string `json:"locale"`
}

type registerResponse struct {
	ID     string `json:"id"`
	Config struct {
		Peers []struct {
			PublicKey string `json:"public_key"`
			Endpoint  struct {
				Host  string `json:"host"`
				V4    string `json:"v4"`
				Ports []int  `json:"ports"`
			} `json:"endpoint"`
		} `json:"peers"`
		Interface struct {
			Addresses struct {
				V4 string `json:"v4"`
				V6 string `json:"v6"`
			} `json:"addresses"`
		} `json:"interface"`
	} `json:"config"`
}

// Register opens a fresh anonymous WARP account the way Cloudflare's own mobile client does and returns
// what a plain WireGuard peer needs; the private key never leaves this process except as the result.
func Register(ctx context.Context, client *http.Client) (Account, error) {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return Account{}, fmt.Errorf("generate key: %w", err)
	}
	body, _ := json.Marshal(registerRequest{
		Key:        base64.StdEncoding.EncodeToString(priv.PublicKey().Bytes()),
		Tos:        time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Model:      "PC",
		KeyType:    "curve25519",
		TunnelType: "wireguard",
		Locale:     "en_US",
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/reg", bytes.NewReader(body))
	if err != nil {
		return Account{}, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("CF-Client-Version", clientVersion)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	resp, err := client.Do(req)
	if err != nil {
		return Account{}, fmt.Errorf("register with Cloudflare: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return Account{}, fmt.Errorf("Cloudflare answered %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	var r registerResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return Account{}, fmt.Errorf("parse Cloudflare answer: %w", err)
	}
	if len(r.Config.Peers) == 0 || r.Config.Peers[0].PublicKey == "" || r.Config.Interface.Addresses.V4 == "" {
		return Account{}, errors.New("Cloudflare answer carries no WireGuard peer or address")
	}
	p := r.Config.Peers[0]
	return Account{
		DeviceID:      r.ID,
		PrivateKey:    base64.StdEncoding.EncodeToString(priv.Bytes()),
		PeerPublicKey: p.PublicKey,
		Endpoint:      endpointOf(p.Endpoint.V4, p.Endpoint.Host, p.Endpoint.Ports),
		AddressV4:     r.Config.Interface.Addresses.V4,
		AddressV6:     r.Config.Interface.Addresses.V6,
	}, nil
}

// endpointOf prefers the literal IPv4 address so a node never depends on resolving Cloudflare's name.
func endpointOf(v4, host string, ports []int) string {
	port := 2408
	if len(ports) > 0 && ports[0] > 0 {
		port = ports[0]
	}
	if ip, _, err := net.SplitHostPort(v4); err == nil && ip != "" {
		return net.JoinHostPort(ip, strconv.Itoa(port))
	}
	if h, _, err := net.SplitHostPort(host); err == nil && h != "" {
		return net.JoinHostPort(h, strconv.Itoa(port))
	}
	return net.JoinHostPort("engage.cloudflareclient.com", strconv.Itoa(port))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
