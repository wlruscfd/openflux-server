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
	LinkHash        []byte
	State           string
	StateCheckedAt  *time.Time
	StateLatencyMS  *int
	StateError      string
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

// CreateProxyKeys stores a whole list of keys in one round trip.
//
// A public list is thousands of links long, and one insert per link turns an import into
// tens of thousands of round trips. The same link is met again in the next import, so the
// hash of the link is what the row is keyed by: the link keeps its row and only gains the
// carriers it did not have.
func (s *Store) CreateProxyKeys(ctx context.Context, ps []ProxyKey) (int, error) {
	if len(ps) == 0 {
		return 0, nil
	}
	values := make([]string, 0, len(ps))
	args := make([]any, 0, len(ps)*7)
	for i, p := range ps {
		base := i*8 + 1
		values = append(values, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7))
		args = append(args, p.Name, p.LinkEnc, p.Kind, p.Server, p.Port, p.Priority, p.Enabled, p.LinkHash)
	}
	q := `INSERT INTO proxy_keys (name, link_enc, kind, server, port, priority, enabled, link_hash)
	      VALUES ` + joinRows(values) + `
	      ON CONFLICT (link_hash) WHERE link_hash IS NOT NULL DO UPDATE SET
	        priority = GREATEST(proxy_keys.priority, EXCLUDED.priority),
	        updated_at = now()
	      RETURNING id`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("create proxy keys: %w", err)
	}
	ids := make([]string, 0, len(ps))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan proxy key id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read proxy key ids: %w", err)
	}

	carrierValues := make([]string, 0, len(ps)*len(ps))
	carrierArgs := make([]any, 0, len(ps)*2)
	for i, id := range ids {
		for _, c := range ps[i].Carriers {
			carrierValues = append(carrierValues, fmt.Sprintf("($%d,$%d)", len(carrierArgs)+1, len(carrierArgs)+2))
			carrierArgs = append(carrierArgs, id, c)
		}
	}
	if len(carrierValues) > 0 {
		cq := `INSERT INTO proxy_key_carriers (proxy_key_id, carrier) VALUES ` + joinRows(carrierValues) + ` ON CONFLICT DO NOTHING`
		if _, err := s.pool.Exec(ctx, cq, carrierArgs...); err != nil {
			return 0, fmt.Errorf("set proxy key carriers: %w", err)
		}
	}
	return len(ids), nil
}

// SetProxyKeyState records what the node found when it last used a key.
// SetKeyRoomState records what a node found for a key that makes its own rooms.
//
// A list replaces the one the key had: the rooms of a board that has closed are dead, and
// the node creates new ones, so the client has to be given the new list or it opens dead
// rooms forever. A report that is older than the list the key already has is ignored, so a
// late report from a run that ended cannot point the key back at rooms that are gone.
//
// A failure does not touch the list: the client can still open the rooms the key had, and
// the reason the new ones could not be made is what the panel shows.
func (s *Store) SetKeyRoomState(ctx context.Context, keyID, state, failure, list string, reportedAt time.Time) (bool, error) {
	if list != "" {
		tag, err := s.pool.Exec(ctx, `
			UPDATE keys SET doc_url = $2, room_state = 'ready', room_state_at = now(),
			       room_state_error = NULL, room_list_at = $3, updated_at = now()
			WHERE id = $1 AND deleted_at IS NULL
			  AND (doc_url = '' OR doc_url = $2 OR room_list_at IS NULL OR room_list_at <= $3)
		`, keyID, list, reportedAt)
		if err != nil {
			return false, fmt.Errorf("set key doc url from node: %w", err)
		}
		return tag.RowsAffected() > 0, nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE keys SET room_state = $2, room_state_at = now(), room_state_error = NULLIF($3, ''),
		       updated_at = now()
		WHERE id = $1 AND deleted_at IS NULL
	`, keyID, state, failure)
	if err != nil {
		return false, fmt.Errorf("set key room state: %w", err)
	}
	return true, nil
}

func (s *Store) SetProxyKeyState(ctx context.Context, id, state string, latencyMS *int, failure string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE proxy_keys SET state = $2, state_checked_at = now(), state_latency_ms = $3,
		       state_error = NULLIF($4, ''), updated_at = now()
		WHERE id = $1
	`, id, state, latencyMS, failure)
	if err != nil {
		return fmt.Errorf("set proxy key state: %w", err)
	}
	return nil
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
		       (SELECT count(*) FROM proxy_assignments a WHERE a.proxy_key_id = p.id),
		       p.state, p.state_checked_at, p.state_latency_ms, COALESCE(p.state_error, '')
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
			&p.CreatedAt, &p.Carriers, &p.AssignmentCount,
			&p.State, &p.StateCheckedAt, &p.StateLatencyMS, &p.StateError); err != nil {
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

// proxyKeyRotationWindow is how long the order of the unproven keys lasts before it shifts.
// A node materializes a handful of them at a time, so a window that is too long leaves most
// of the list untried for hours and one that is too short never finishes a check.
const proxyKeyRotationWindow = 5 * time.Minute

// ProxyKeyState is what a node found when it last checked one key.
type ProxyKeyState struct {
	ID        string
	State     string
	LatencyMS *int
	Error     string
}

// SetProxyKeyStates records what the node found for a batch of keys in one round trip.
//
// A node checks every key it materialized, which is hundreds of them, and one update per key
// would be hundreds of round trips for a report that is a few kilobytes.
func (s *Store) SetProxyKeyStates(ctx context.Context, states []ProxyKeyState) error {
	if len(states) == 0 {
		return nil
	}
	ids := make([]string, 0, len(states))
	values := make([]string, 0, len(states))
	latencies := make([]*int, 0, len(states))
	errors := make([]string, 0, len(states))
	for _, st := range states {
		ids = append(ids, st.ID)
		values = append(values, st.State)
		latencies = append(latencies, st.LatencyMS)
		errors = append(errors, st.Error)
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE proxy_keys p
		SET state = s.state, state_checked_at = now(), state_latency_ms = s.latency,
		    state_error = NULLIF(s.failure, ''), updated_at = now()
		FROM unnest($1::uuid[], $2::text[], $3::int[], $4::text[])
		     AS s(id, state, latency, failure)
		WHERE p.id = s.id
	`, ids, values, latencies, errors)
	if err != nil {
		return fmt.Errorf("set proxy key states: %w", err)
	}
	return nil
}

// ListProxyKeysForNode hands a node the enabled keys, each with the carriers it balances for.
// A key with no carrier is not sent: the panel saves links before they are put to work, and a
// key nobody claims is not yet routing.
//
// The list a node receives is bounded, and it rotates. A public proxy list runs to thousands of
// links, almost all of them dead, and a node can only try the handful it materializes. The keys
// a node proved to work come first and stay there; the rest are handed out in an order that
// changes every few minutes, so every candidate is eventually the one that gets tried and
// checked, instead of the same first few links being tested forever.
func (s *Store) ListProxyKeysForNode(ctx context.Context, perCarrier int) ([]NodeProxyKey, error) {
	if perCarrier <= 0 {
		perCarrier = 256
	}
	window := int(time.Now().Unix() / int64(proxyKeyRotationWindow.Seconds()))
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, link_enc, kind, priority, carriers FROM (
			SELECT p.id, p.name, p.link_enc, p.kind, p.priority,
			       array_agg(c.carrier ORDER BY c.carrier) AS carriers,
			       row_number() OVER (
			           PARTITION BY c.carrier
			           ORDER BY (p.state = 'ok') DESC, p.priority DESC,
			                    mod(abs((hashtext(p.id::text || ':' || c.carrier)::bigint % 1000003)
			                            * (1 + ($2::bigint * 7919) % 1000003)), 1000003),
			                    p.created_at) AS rn
			FROM proxy_keys p
			JOIN proxy_key_carriers c ON c.proxy_key_id = p.id
			WHERE p.enabled = true
			GROUP BY p.id, c.carrier
		) ranked
		WHERE rn <= $1
	`, perCarrier, window)
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
