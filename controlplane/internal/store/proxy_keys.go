package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type ProxyKey struct {
	ID              string
	Name            string
	LinkEnc         []byte
	Kind            string
	Server          string
	Port            int
	Priority        int
	Enabled         bool
	CreatedAt       time.Time
	Carriers        []string
	AssignmentCount int
}

// NodeProxyKey is one key as a node receives it: the sealed link plus the carriers it balances for.
type NodeProxyKey struct {
	ID       string
	Name     string
	LinkEnc  []byte
	Kind     string
	Priority int
	Carriers []string
}

func (s *Store) CreateProxyKey(ctx context.Context, p ProxyKey) (ProxyKey, error) {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO proxy_keys (name, link_enc, kind, server, port, priority, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at
	`, p.Name, p.LinkEnc, p.Kind, p.Server, p.Port, p.Priority, p.Enabled).Scan(&p.ID, &p.CreatedAt)
	if err != nil {
		return ProxyKey{}, fmt.Errorf("create proxy key: %w", err)
	}
	if err := s.SetProxyKeyCarriers(ctx, p.ID, p.Carriers); err != nil {
		return ProxyKey{}, err
	}
	return p, nil
}

func (s *Store) SetProxyKeyCarriers(ctx context.Context, id string, carriers []string) error {
	if len(carriers) == 0 {
		_, err := s.pool.Exec(ctx, `DELETE FROM proxy_key_carriers WHERE proxy_key_id = $1`, id)
		if err != nil {
			return fmt.Errorf("clear proxy key carriers: %w", err)
		}
		return nil
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM proxy_key_carriers WHERE proxy_key_id = $1`, id)
	if err != nil {
		return fmt.Errorf("clear proxy key carriers: %w", err)
	}
	rows := make([]string, 0, len(carriers))
	args := make([]any, 0, len(carriers)+1)
	args = append(args, id)
	for _, c := range carriers {
		args = append(args, c)
		rows = append(rows, fmt.Sprintf("($1, $%d)", len(args)))
	}
	q := `INSERT INTO proxy_key_carriers (proxy_key_id, carrier) VALUES ` + joinRows(rows) + ` ON CONFLICT DO NOTHING`
	if _, err := s.pool.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("set proxy key carriers: %w", err)
	}
	return nil
}

func (s *Store) SetProxyKeyLink(ctx context.Context, id string, linkEnc []byte) error {
	tag, err := s.pool.Exec(ctx, `UPDATE proxy_keys SET link_enc = $2, updated_at = now() WHERE id = $1`, id, linkEnc)
	if err != nil {
		return fmt.Errorf("set proxy key link: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func joinRows(rows []string) string { return strings.Join(rows, ", ") }

func (s *Store) ListProxyKeys(ctx context.Context) ([]ProxyKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT p.id, p.name, p.kind, p.server, p.port, p.priority, p.enabled, p.created_at,
		       COALESCE(array_agg(c.carrier) FILTER (WHERE c.carrier IS NOT NULL), '{}'),
		       (SELECT count(*) FROM proxy_assignments a WHERE a.proxy_key_id = p.id)
		FROM proxy_keys p
		LEFT JOIN proxy_key_carriers c ON c.proxy_key_id = p.id
		GROUP BY p.id
		ORDER BY p.created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("list proxy keys: %w", err)
	}
	defer rows.Close()
	var out []ProxyKey
	for rows.Next() {
		var p ProxyKey
		if err := rows.Scan(&p.ID, &p.Name, &p.Kind, &p.Server, &p.Port, &p.Priority, &p.Enabled,
			&p.CreatedAt, &p.Carriers, &p.AssignmentCount); err != nil {
			return nil, fmt.Errorf("scan proxy key: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) GetProxyKey(ctx context.Context, id string) (ProxyKey, error) {
	var p ProxyKey
	err := s.pool.QueryRow(ctx, `
		SELECT p.id, p.name, p.kind, p.server, p.port, p.priority, p.enabled, p.created_at,
		       COALESCE((SELECT array_agg(carrier) FROM proxy_key_carriers WHERE proxy_key_id = p.id), '{}')
		FROM proxy_keys p WHERE p.id = $1
	`, id).Scan(&p.ID, &p.Name, &p.Kind, &p.Server, &p.Port, &p.Priority, &p.Enabled, &p.CreatedAt, &p.Carriers)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProxyKey{}, ErrNotFound
	}
	if err != nil {
		return ProxyKey{}, fmt.Errorf("get proxy key: %w", err)
	}
	return p, nil
}

func (s *Store) UpdateProxyKey(ctx context.Context, id string, name, kind, server string, port, priority int, enabled bool) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE proxy_keys SET name = $2, kind = $3, server = $4, port = $5,
		       priority = $6, enabled = $7, updated_at = now()
		WHERE id = $1
	`, id, name, kind, server, port, priority, enabled)
	if err != nil {
		return fmt.Errorf("update proxy key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteProxyKey(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM proxy_keys WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete proxy key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListProxyKeysForNode hands a node every enabled key, each with the carriers it balances for. A key with no
// carrier is not sent: the panel saves links before they are put to work, and a key nobody claims is not yet routing.
func (s *Store) ListProxyKeysForNode(ctx context.Context) ([]NodeProxyKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT p.id, p.name, p.link_enc, p.kind, p.priority,
		       array_agg(c.carrier ORDER BY c.carrier)
		FROM proxy_keys p
		JOIN proxy_key_carriers c ON c.proxy_key_id = p.id
		WHERE p.enabled = true
		GROUP BY p.id
		ORDER BY p.created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("list proxy keys for node: %w", err)
	}
	defer rows.Close()
	var out []NodeProxyKey
	for rows.Next() {
		var p NodeProxyKey
		if err := rows.Scan(&p.ID, &p.Name, &p.LinkEnc, &p.Kind, &p.Priority, &p.Carriers); err != nil {
			return nil, fmt.Errorf("scan node proxy key: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SaveProxyAssignments replaces what a node remembers about where it put each subject.
func (s *Store) SaveProxyAssignments(ctx context.Context, nodeID string, assignments map[string]string) error {
	if len(assignments) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin assignments: %w", err)
	}
	defer tx.Rollback(ctx)
	for subject, proxyKeyID := range assignments {
		var known bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM proxy_keys WHERE id = $1)`, proxyKeyID).Scan(&known); err != nil {
			return fmt.Errorf("check proxy key: %w", err)
		}
		if !known {
			continue
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO proxy_assignments (node_id, subject, proxy_key_id, assigned_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (node_id, subject) DO UPDATE SET proxy_key_id = EXCLUDED.proxy_key_id, assigned_at = now()
		`, nodeID, subject, proxyKeyID)
		if err != nil {
			return fmt.Errorf("save assignment: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) LoadProxyAssignments(ctx context.Context, nodeID string) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT subject, proxy_key_id FROM proxy_assignments WHERE node_id = $1`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("load proxy assignments: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var subject, proxyKeyID string
		if err := rows.Scan(&subject, &proxyKeyID); err != nil {
			return nil, fmt.Errorf("scan assignment: %w", err)
		}
		out[subject] = proxyKeyID
	}
	return out, rows.Err()
}
