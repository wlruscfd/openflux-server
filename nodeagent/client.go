package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ControlClient struct {
	baseURL    string
	nodeToken  string
	httpClient *http.Client
}

func NewControlClient(baseURL, nodeToken string) *ControlClient {
	return &ControlClient{
		baseURL:   baseURL,
		nodeToken: nodeToken,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

type RemoteKey struct {
	ID                string   `json:"id"`
	DocURL            string   `json:"doc_url"`
	DocURLs           []string `json:"doc_urls,omitempty"`
	Transport         string   `json:"transport"`
	TrafficLimitBytes *int64   `json:"traffic_limit_bytes,omitempty"`
	BytesUsedTotal    int64    `json:"bytes_used_total"`
	Token             string   `json:"token,omitempty"`
	// E2EEncryption mirrors the key's e2e_encryption setting; startWorker only wraps the transport in EncryptedTransport when this is true, making the setting binding rather than advisory.
	E2EEncryption bool `json:"e2e_encryption,omitempty"`
	// RelayHost/RelayPort set means this key is cascaded: relay to that address over UDP instead of dialing the real internet on this node.
	RelayHost *string `json:"relay_host,omitempty"`
	RelayPort *int    `json:"relay_port,omitempty"`
}

func (k RemoteKey) IsRelayed() bool {
	return k.RelayHost != nil && k.RelayPort != nil
}

type RemoteKeyCookie struct {
	KeyID     string `json:"key_id"`
	Transport string `json:"transport"`
	Cookies   string `json:"cookies"`
}

func (c *ControlClient) ListKeys(ctx context.Context) ([]RemoteKey, error) {
	var out []RemoteKey
	if err := c.do(ctx, http.MethodGet, "/v1/nodes/keys", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListKeyCookies returns the jars the client pushed for this node's keys. A failed fetch is
// not fatal: the reconcile that calls it must keep working on the keys that have no jar.
func (c *ControlClient) ListKeyCookies(ctx context.Context) ([]RemoteKeyCookie, error) {
	var out []RemoteKeyCookie
	if err := c.do(ctx, http.MethodGet, "/v1/nodes/cookies", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// RelayExitKey is the final-exit half of a cascade - see RemoteKey.RelayHost/RelayPort on the entry side.
type RelayExitKey struct {
	ID        string `json:"id"`
	Token     string `json:"token"`
	RelayPort int    `json:"relay_port"`
}

func (c *ControlClient) ListRelayExitKeys(ctx context.Context) ([]RelayExitKey, error) {
	var out []RelayExitKey
	if err := c.do(ctx, http.MethodGet, "/v1/nodes/relay-keys", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

type UsageDelta struct {
	KeyID              string `json:"key_id"`
	BytesSentDelta     int64  `json:"bytes_sent_delta"`
	BytesReceivedDelta int64  `json:"bytes_received_delta"`
}

type reportUsageRequest struct {
	Deltas []UsageDelta `json:"deltas"`
}

type reportUsageResponse struct {
	DisabledNow []string `json:"disabled_now"`
}

func (c *ControlClient) ReportUsage(ctx context.Context, deltas []UsageDelta) ([]string, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	var out reportUsageResponse
	if err := c.do(ctx, http.MethodPost, "/v1/nodes/me/usage", reportUsageRequest{Deltas: deltas}, &out); err != nil {
		return nil, err
	}
	return out.DisabledNow, nil
}

func (c *ControlClient) Heartbeat(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/nodes/me/heartbeat", nil, nil)
}

func (c *ControlClient) do(ctx context.Context, method, path string, body, out interface{}) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.nodeToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, string(respBody))
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response for %s %s: %w", method, path, err)
	}
	return nil
}
