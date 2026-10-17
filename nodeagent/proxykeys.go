package nodeagent

import (
	"context"
	"net/http"
)

type RemoteProxyKey struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Link string `json:"link"`
	Kind string `json:"kind"`
	// Priority weights a key inside its carrier's pool: the balancer prefers the lowest
	// latency weighted by it, so a fast paid line wins over a slow free one.
	Priority int      `json:"priority,omitempty"`
	Carriers []string `json:"carriers"`
}

type RemoteProxyKeys struct {
	Keys []RemoteProxyKey `json:"keys"`
	// Assignments is where this node left each subject last time. A carrier connection the
	// provider's check was passed through must keep leaving from the same proxy, so the pin
	// survives a restart instead of being decided again from scratch.
	Assignments map[string]string `json:"assignments,omitempty"`
}

func (c *ControlClient) ListProxyKeys(ctx context.Context) (RemoteProxyKeys, error) {
	var out RemoteProxyKeys
	if err := c.do(ctx, http.MethodGet, "/v1/nodes/proxy-keys", nil, &out); err != nil {
		return RemoteProxyKeys{}, err
	}
	return out, nil
}

func (c *ControlClient) SaveProxyAssignments(ctx context.Context, assignments map[string]string) error {
	if len(assignments) == 0 {
		return nil
	}
	body := map[string]any{"assignments": assignments}
	return c.do(ctx, http.MethodPost, "/v1/nodes/proxy-assignments", body, nil)
}