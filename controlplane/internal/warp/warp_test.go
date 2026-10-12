package warp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterBuildsAWireGuardPeer(t *testing.T) {
	var got registerRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/reg" || r.Method != http.MethodPost {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("CF-Client-Version") == "" || r.Header.Get("User-Agent") == "" {
			t.Error("the client headers are missing")
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"dev-1","config":{"peers":[{"public_key":"peerkey=","endpoint":{"host":"engage.cloudflareclient.com:2408","v4":"162.159.192.8:0","ports":[2408,500]}}],"interface":{"addresses":{"v4":"172.16.0.2","v6":"2606:4700::1"}}}}`))
	}))
	defer srv.Close()
	old := apiBase
	apiBase = srv.URL
	defer func() { apiBase = old }()

	acc, err := Register(context.Background(), srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if acc.DeviceID != "dev-1" || acc.PeerPublicKey != "peerkey=" || acc.AddressV4 != "172.16.0.2" || acc.AddressV6 == "" {
		t.Fatalf("account = %+v", acc)
	}
	if acc.Endpoint != "162.159.192.8:2408" {
		t.Fatalf("endpoint = %q, want the literal IPv4 with the first port", acc.Endpoint)
	}
	priv, err := base64.StdEncoding.DecodeString(acc.PrivateKey)
	if err != nil || len(priv) != 32 {
		t.Fatalf("private key is not 32 base64 bytes: %v", err)
	}
	if got.KeyType != "curve25519" || got.TunnelType != "wireguard" || got.Key == "" {
		t.Fatalf("request = %+v", got)
	}
}

func TestRegisterReportsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	old := apiBase
	apiBase = srv.URL
	defer func() { apiBase = old }()
	if _, err := Register(context.Background(), srv.Client()); err == nil {
		t.Fatal("a refusal must be an error")
	}
}
