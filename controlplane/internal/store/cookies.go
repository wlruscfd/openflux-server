package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"openflux-control/internal/model"
)

type KeyCookie struct {
	KeyID     string
	UpdatedAt time.Time
}

// SetKeyCookies stores the jar for one key, encrypted at rest by the caller.
func (s *Store) SetKeyCookies(ctx context.Context, keyID string, enc []byte) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO key_cookies (key_id, cookies_enc, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (key_id) DO UPDATE SET cookies_enc = EXCLUDED.cookies_enc, updated_at = now()
	`, keyID, enc)
	if err != nil {
		return fmt.Errorf("set key cookies: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteKeyCookies(ctx context.Context, keyID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM key_cookies WHERE key_id = $1`, keyID)
	if err != nil {
		return fmt.Errorf("delete key cookies: %w", err)
	}
	return nil
}

// KeyCookieStatus is what admin surfaces: never the jar itself, only whether one is set.
func (s *Store) KeyCookieStatus(ctx context.Context, keyID string) (bool, *time.Time, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM keys WHERE id = $1 AND deleted_at IS NULL)`, keyID).Scan(&exists); err != nil {
		return false, nil, fmt.Errorf("key cookie status: %w", err)
	}
	if !exists {
		return false, nil, ErrNotFound
	}

	var updated *time.Time
	err := s.pool.QueryRow(ctx, `SELECT updated_at FROM key_cookies WHERE key_id = $1`, keyID).Scan(&updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("key cookie status: %w", err)
	}
	return true, updated, nil
}

// ListNodeKeyCookies returns the decrypted jars for every key assigned to one node.
// Only the node's own keys, and only over the node-token channel.
func (s *Store) ListNodeKeyCookies(ctx context.Context, nodeID string) ([]model.NodeKeyCookie, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT k.id, k.transport, c.cookies_enc
		FROM key_cookies c
		JOIN keys k ON k.id = c.key_id
		WHERE k.assigned_node_id = $1
		  AND k.enabled = true
		  AND k.deleted_at IS NULL
		ORDER BY k.id
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list node key cookies: %w", err)
	}
	defer rows.Close()

	var out []model.NodeKeyCookie
	for rows.Next() {
		var c model.NodeKeyCookie
		if err := rows.Scan(&c.KeyID, &c.Transport, &c.CookiesEnc); err != nil {
			return nil, fmt.Errorf("scan node key cookie: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
