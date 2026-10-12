package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"openflux-control/internal/model"
)

type WarpProfile struct {
	ID            string
	NodeID        *string
	Name          string
	DeviceID      string
	PrivateKeyEnc []byte
	PeerPublicKey string
	Endpoint      string
	AddressV4     string
	AddressV6     string
	CreatedAt     time.Time
	KeyCount      int
}

// ErrWarpWithCascade means the key already relays through a final-exit node; WARP and a node cascade are alternatives.
var ErrWarpWithCascade = errors.New("key already cascades through a final-exit node")

func (s *Store) CreateWarpProfile(ctx context.Context, p WarpProfile) (WarpProfile, error) {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO warp_profiles (name, device_id, private_key_enc, peer_public_key, endpoint, address_v4, address_v6, node_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, created_at
	`, p.Name, p.DeviceID, p.PrivateKeyEnc, p.PeerPublicKey, p.Endpoint, p.AddressV4, p.AddressV6, p.NodeID).Scan(&p.ID, &p.CreatedAt)
	if err != nil {
		return WarpProfile{}, fmt.Errorf("create warp profile: %w", err)
	}
	return p, nil
}

func (s *Store) ListWarpProfiles(ctx context.Context) ([]WarpProfile, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT w.id, w.node_id, w.name, w.device_id, w.peer_public_key, w.endpoint, w.address_v4, w.address_v6, w.created_at,
		       (SELECT count(*) FROM keys k WHERE k.warp_profile_id = w.id AND k.deleted_at IS NULL)
		FROM warp_profiles w
		ORDER BY w.created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("list warp profiles: %w", err)
	}
	defer rows.Close()
	var out []WarpProfile
	for rows.Next() {
		var p WarpProfile
		if err := rows.Scan(&p.ID, &p.NodeID, &p.Name, &p.DeviceID, &p.PeerPublicKey, &p.Endpoint, &p.AddressV4, &p.AddressV6, &p.CreatedAt, &p.KeyCount); err != nil {
			return nil, fmt.Errorf("scan warp profile: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) DeleteWarpProfile(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM warp_profiles WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete warp profile: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetKeyWarp points a key at a WARP profile (profileID != nil) or back at a direct exit.
func (s *Store) SetKeyWarp(ctx context.Context, keyID string, profileID *string) (model.Key, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Key{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if profileID != nil {
		var cascaded bool
		if err := tx.QueryRow(ctx, `SELECT final_exit_node_id IS NOT NULL FROM keys WHERE id = $1 AND deleted_at IS NULL`, keyID).Scan(&cascaded); errors.Is(err, pgx.ErrNoRows) {
			return model.Key{}, ErrNotFound
		} else if err != nil {
			return model.Key{}, fmt.Errorf("check key: %w", err)
		}
		if cascaded {
			return model.Key{}, ErrWarpWithCascade
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM warp_profiles WHERE id = $1)`, *profileID).Scan(&exists); err != nil {
			return model.Key{}, fmt.Errorf("check warp profile: %w", err)
		}
		if !exists {
			return model.Key{}, ErrNotFound
		}
	}

	k, err := scanKey(tx.QueryRow(ctx, `
		UPDATE keys SET warp_profile_id = $1, updated_at = now()
		WHERE id = $2 AND deleted_at IS NULL
		RETURNING `+keyColumns, profileID, keyID))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("set key warp: %w", err)
	}
	return k, tx.Commit(ctx)
}

// NodeNeedsWarp reports whether this node runs a key that goes through WARP but has no WARP identity of its own yet.
func (s *Store) NodeNeedsWarp(ctx context.Context, nodeID string) (bool, error) {
	var need bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM keys k
			WHERE k.assigned_node_id = $1 AND k.enabled = true AND k.deleted_at IS NULL AND k.warp_profile_id IS NOT NULL
		) AND NOT EXISTS (SELECT 1 FROM warp_profiles WHERE node_id = $1)
	`, nodeID).Scan(&need)
	if err != nil {
		return false, fmt.Errorf("check node warp: %w", err)
	}
	return need, nil
}
