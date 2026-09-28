package store

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"openflux-control/internal/model"
)

type CreateKeyParams struct {
	TokenHash         string
	TokenEnc          []byte
	Label             string
	Transport         string
	DocURL            string
	DocURLs           []string
	E2EEncryption     bool
	TrafficLimitBytes *int64
	OwnerRef          string
	ExpiresAt         *time.Time
}

func scanKey(row pgx.Row) (model.Key, error) {
	var k model.Key
	err := row.Scan(
		&k.ID, &k.Label, &k.Transport, &k.DocURL, &k.DocURLs, &k.E2EEncryption, &k.AssignedNodeID, &k.Enabled,
		&k.TrafficLimitBytes, &k.BytesSentTotal, &k.BytesReceivedTotal, &k.OwnerRef,
		&k.ExpiresAt, &k.CreatedAt, &k.UpdatedAt, &k.LastSeenAt, &k.FinalExitNodeID, &k.RelayPort,
	)
	return k, err
}

const keyColumns = `id, label, transport, doc_url, doc_urls, e2e_encryption, assigned_node_id, enabled,
	traffic_limit_bytes, bytes_sent_total, bytes_received_total, owner_ref,
	expires_at, created_at, updated_at, last_seen_at, final_exit_node_id, relay_port`

// CreateKey best-effort assigns the new key to whichever active node currently carries the fewest enabled keys, keeping the fleet roughly balanced without a separate scheduler.
func (s *Store) CreateKey(ctx context.Context, p CreateKeyParams) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT n.id
			FROM nodes n
			LEFT JOIN keys k ON k.assigned_node_id = n.id AND k.enabled = true
			WHERE n.status = 'active'
			GROUP BY n.id
			HAVING count(k.id) < n.max_keys
			ORDER BY count(k.id) ASC
			LIMIT 1
		)
		INSERT INTO keys (token_hash, token_enc, label, transport, doc_url, doc_urls, e2e_encryption, traffic_limit_bytes, owner_ref, expires_at, assigned_node_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, (SELECT id FROM candidate))
		RETURNING `+keyColumns,
		p.TokenHash, p.TokenEnc, p.Label, p.Transport, p.DocURL, p.DocURLs, p.E2EEncryption, p.TrafficLimitBytes, p.OwnerRef, p.ExpiresAt)

	return scanKey(row)
}

func (s *Store) GetKeyByTokenHash(ctx context.Context, tokenHash string) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM keys WHERE token_hash = $1 AND deleted_at IS NULL`, tokenHash)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("get key by token: %w", err)
	}
	return k, nil
}

func (s *Store) GetKeyByID(ctx context.Context, id string) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+keyColumns+` FROM keys WHERE id = $1 AND deleted_at IS NULL`, id)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("get key: %w", err)
	}
	return k, nil
}

// NodeKey is kept separate from model.Key so nothing outside the exit-node path (the admin API, the web panel) has any access to TokenEnc, the encrypted raw token.
type NodeKey struct {
	ID                 string
	DocURL             string
	DocURLs            []string
	Transport          string
	TrafficLimitBytes  *int64
	BytesSentTotal     int64
	BytesReceivedTotal int64
	TokenEnc           []byte
	E2EEncryption      bool
	// RelayPort/RelayHost are set only for a cascaded key: the entry node relays to RelayHost:RelayPort instead of dialing the real internet itself.
	RelayPort *int
	RelayHost *string
}

func (s *Store) ListActiveKeysForNode(ctx context.Context, nodeID string) ([]NodeKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT k.id, k.doc_url, k.doc_urls, k.transport, k.traffic_limit_bytes, k.bytes_sent_total, k.bytes_received_total,
		       k.token_enc, k.e2e_encryption, k.relay_port, fx.public_address
		FROM keys k
		LEFT JOIN nodes fx ON fx.id = k.final_exit_node_id
		WHERE k.assigned_node_id = $1 AND k.enabled = true AND k.deleted_at IS NULL
		ORDER BY k.created_at
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list node keys: %w", err)
	}
	defer rows.Close()

	var out []NodeKey
	for rows.Next() {
		var k NodeKey
		if err := rows.Scan(&k.ID, &k.DocURL, &k.DocURLs, &k.Transport, &k.TrafficLimitBytes,
			&k.BytesSentTotal, &k.BytesReceivedTotal, &k.TokenEnc, &k.E2EEncryption, &k.RelayPort, &k.RelayHost); err != nil {
			return nil, fmt.Errorf("scan node key: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RelayExitKey is what a final-exit node needs: just enough to derive the relay link's key and open its own listener - never doc_url/doc_urls, since it never talks to Yandex at all.
type RelayExitKey struct {
	ID        string
	TokenEnc  []byte
	RelayPort int
}

// ListActiveRelayExitKeysForNode is the other half of a cascade: keys whose final_exit_node_id is this node, filtered the same way as ListActiveKeysForNode so a key disabled (e.g. over quota, reported by the entry node) drops out here too on the next poll.
func (s *Store) ListActiveRelayExitKeysForNode(ctx context.Context, nodeID string) ([]RelayExitKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, token_enc, relay_port
		FROM keys
		WHERE final_exit_node_id = $1 AND enabled = true AND deleted_at IS NULL AND relay_port IS NOT NULL
		ORDER BY created_at
	`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list relay exit keys: %w", err)
	}
	defer rows.Close()

	var out []RelayExitKey
	for rows.Next() {
		var k RelayExitKey
		if err := rows.Scan(&k.ID, &k.TokenEnc, &k.RelayPort); err != nil {
			return nil, fmt.Errorf("scan relay exit key: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// ErrNoFinalExitAddress means the target node has no public_address set yet - can't build a cascade to a node the admin hasn't told us how to reach.
var ErrNoFinalExitAddress = errors.New("final exit node has no public_address set")

// SetKeyFinalExit sets or clears a key's cascade target. Setting it allocates the next free relay_port among keys already routed to that same final-exit node (a fresh, small, per-node-pair range - not the same numbering as any single node's own raw-mode port allocator); clearing (nodeID == nil) frees the port back for reuse implicitly, since a future SetKeyFinalExit call just recomputes the max.
func (s *Store) SetKeyFinalExit(ctx context.Context, id string, finalExitNodeID *string) (model.Key, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Key{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if finalExitNodeID == nil {
		row := tx.QueryRow(ctx, `
			UPDATE keys SET final_exit_node_id = NULL, relay_port = NULL, updated_at = now()
			WHERE id = $1 AND deleted_at IS NULL
			RETURNING `+keyColumns, id)
		k, err := scanKey(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Key{}, ErrNotFound
		}
		if err != nil {
			return model.Key{}, fmt.Errorf("clear key final exit: %w", err)
		}
		return k, tx.Commit(ctx)
	}

	var publicAddress *string
	if err := tx.QueryRow(ctx, `SELECT public_address FROM nodes WHERE id = $1`, *finalExitNodeID).Scan(&publicAddress); errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	} else if err != nil {
		return model.Key{}, fmt.Errorf("check final exit node: %w", err)
	}
	if publicAddress == nil || *publicAddress == "" {
		return model.Key{}, ErrNoFinalExitAddress
	}

	// relayPortBase/Max: a small dedicated range distinct from raw mode's own 1025-65535 ephemeral allocation - this port is for the entry<->final-exit UDP link itself, one per cascaded key sharing that final-exit node.
	const relayPortBase, relayPortMax = 41000, 41999
	var nextPort int
	if err := tx.QueryRow(ctx, `
		SELECT coalesce(max(relay_port), $2 - 1) + 1
		FROM keys WHERE final_exit_node_id = $1
	`, *finalExitNodeID, relayPortBase).Scan(&nextPort); err != nil {
		return model.Key{}, fmt.Errorf("allocate relay port: %w", err)
	}
	if nextPort > relayPortMax {
		return model.Key{}, fmt.Errorf("no relay port capacity left for that final-exit node (range %d-%d exhausted)", relayPortBase, relayPortMax)
	}

	row := tx.QueryRow(ctx, `
		UPDATE keys SET final_exit_node_id = $1, relay_port = $2, updated_at = now()
		WHERE id = $3 AND deleted_at IS NULL
		RETURNING `+keyColumns, *finalExitNodeID, nextPort, id)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("set key final exit: %w", err)
	}
	return k, tx.Commit(ctx)
}

type ListKeysFilter struct {
	OwnerRef string
	Limit    int
	Offset   int
}

func (s *Store) ListKeys(ctx context.Context, f ListKeysFilter) ([]model.Key, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+keyColumns+` FROM keys
		WHERE ($1 = '' OR owner_ref = $1) AND deleted_at IS NULL
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, f.OwnerRef, limit, f.Offset)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer rows.Close()

	var out []model.Key
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// HasEnabledKeyWithAnyDocURL guards against two enabled keys sharing one Yandex document: Yandex broadcasts every event to every participant, so a shared doc means each key's traffic gets reinjected into the other's.
func (s *Store) HasEnabledKeyWithAnyDocURL(ctx context.Context, urls []string, excludeID string) (bool, error) {
	var exists bool
	// excludeID is "" on creation; $2='' short-circuits before the ::uuid cast, which otherwise errors outright on an empty string ("invalid input syntax for type uuid").
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM keys
			WHERE enabled = true
				AND deleted_at IS NULL
				AND ($2 = '' OR id != $2::uuid)
				AND (doc_url = ANY($1::text[]) OR doc_urls && $1::text[])
		)
	`, urls, excludeID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check doc_url in use: %w", err)
	}
	return exists, nil
}

// ErrDocURLInUse is returned when another enabled key already claims a doc URL, discovered inside the same transaction as the write so it can't lose a race against a concurrent check.
var ErrDocURLInUse = errors.New("doc_url already in use by another enabled key")

// docURLLockKeys sorts URLs so two calls with overlapping sets always acquire their shared locks in the same order and can never deadlock waiting on each other.
func docURLLockKeys(urls []string) []int64 {
	seen := make(map[int64]bool, len(urls))
	keys := make([]int64, 0, len(urls))
	for _, u := range urls {
		h := fnv.New64a()
		h.Write([]byte(u))
		k := int64(h.Sum64())
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// lockDocURLs closes the check-then-write race: two concurrent requests touching overlapping URL sets serialize here before either reaches its EXISTS check.
func lockDocURLs(ctx context.Context, tx pgx.Tx, urls []string) error {
	for _, key := range docURLLockKeys(urls) {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
			return fmt.Errorf("lock doc_url: %w", err)
		}
	}
	return nil
}

func docURLInUseTx(ctx context.Context, tx pgx.Tx, urls []string, excludeID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM keys
			WHERE enabled = true
				AND deleted_at IS NULL
				AND ($2 = '' OR id != $2::uuid)
				AND (doc_url = ANY($1::text[]) OR doc_urls && $1::text[])
		)
	`, urls, excludeID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check doc_url in use: %w", err)
	}
	return exists, nil
}

// CreateKeyGuarded holds an advisory lock across the doc_url check and insert in one transaction, since two concurrent creates could otherwise both pass the check before either committed.
func (s *Store) CreateKeyGuarded(ctx context.Context, p CreateKeyParams, urls []string) (model.Key, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Key{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := lockDocURLs(ctx, tx, urls); err != nil {
		return model.Key{}, err
	}

	inUse, err := docURLInUseTx(ctx, tx, urls, "")
	if err != nil {
		return model.Key{}, err
	}
	if inUse {
		return model.Key{}, ErrDocURLInUse
	}

	row := tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT n.id
			FROM nodes n
			LEFT JOIN keys k ON k.assigned_node_id = n.id AND k.enabled = true
			WHERE n.status = 'active'
			GROUP BY n.id
			HAVING count(k.id) < n.max_keys
			ORDER BY count(k.id) ASC
			LIMIT 1
		)
		INSERT INTO keys (token_hash, token_enc, label, transport, doc_url, doc_urls, e2e_encryption, traffic_limit_bytes, owner_ref, expires_at, assigned_node_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, (SELECT id FROM candidate))
		RETURNING `+keyColumns,
		p.TokenHash, p.TokenEnc, p.Label, p.Transport, p.DocURL, p.DocURLs, p.E2EEncryption, p.TrafficLimitBytes, p.OwnerRef, p.ExpiresAt)
	k, err := scanKey(row)
	if err != nil {
		return model.Key{}, fmt.Errorf("create key: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return model.Key{}, fmt.Errorf("commit: %w", err)
	}
	return k, nil
}

func (s *Store) SetKeyEnabledGuarded(ctx context.Context, id string, urls []string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := lockDocURLs(ctx, tx, urls); err != nil {
		return err
	}

	inUse, err := docURLInUseTx(ctx, tx, urls, id)
	if err != nil {
		return err
	}
	if inUse {
		return ErrDocURLInUse
	}

	tag, err := tx.Exec(ctx, `UPDATE keys SET enabled = true, updated_at = now() WHERE id = $1 AND deleted_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("set key enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return tx.Commit(ctx)
}

func (s *Store) SetKeyEnabled(ctx context.Context, id string, enabled bool) error {
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET enabled = $1, updated_at = now() WHERE id = $2 AND deleted_at IS NULL`, enabled, id)
	if err != nil {
		return fmt.Errorf("set key enabled: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RotateKeyToken(ctx context.Context, id, newTokenHash string, newTokenEnc []byte) (model.Key, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE keys SET token_hash = $1, token_enc = $2, updated_at = now() WHERE id = $3 AND deleted_at IS NULL
		RETURNING `+keyColumns, newTokenHash, newTokenEnc, id)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("rotate key token: %w", err)
	}
	return k, nil
}

func (s *Store) SetKeyTrafficLimit(ctx context.Context, id string, limit *int64) error {
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET traffic_limit_bytes = $1, updated_at = now() WHERE id = $2 AND deleted_at IS NULL`, limit, id)
	if err != nil {
		return fmt.Errorf("set key traffic limit: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateKeyParams is a partial update: a nil field is left alone, and the doc-link fields are
// only re-checked against other keys when they actually change, so editing a label or a limit
// never trips the one-doc-per-key rule on the key's own current link.
type UpdateKeyParams struct {
	Label             *string
	Transport         *string
	DocURL            *string
	DocURLs           *[]string
	E2EEncryption     *bool
	TrafficLimitBytes *OptionalInt64
	OwnerRef          *string
	ExpiresAt         **time.Time
	FinalExitNodeID   **string
	Enabled           *bool
}

type OptionalInt64 struct {
	Set   bool
	Value *int64
}

// docURLsTouched reports whether this update moves the key to a different document.
func (p UpdateKeyParams) docURLsTouched() bool {
	return p.DocURL != nil || p.DocURLs != nil
}

func (p UpdateKeyParams) urlSet() []string {
	set := make([]string, 0, 2)
	if p.DocURL != nil && *p.DocURL != "" {
		set = append(set, *p.DocURL)
	}
	if p.DocURLs != nil {
		set = append(set, *p.DocURLs...)
	}
	return set
}

// assignments builds the SET clause and its arguments. Kept separate from the SQL execution so
// the "only what was asked for" behaviour is unit-testable without a database.
func (p UpdateKeyParams) assignments() (string, []any) {
	cols := make([]string, 0, 9)
	args := make([]any, 0, 9)
	add := func(col string, v any) {
		args = append(args, v)
		cols = append(cols, fmt.Sprintf("%s = $%d", col, len(args)))
	}
	if p.Label != nil {
		add("label", *p.Label)
	}
	if p.Transport != nil {
		add("transport", *p.Transport)
	}
	if p.DocURL != nil {
		add("doc_url", *p.DocURL)
	}
	if p.DocURLs != nil {
		add("doc_urls", *p.DocURLs)
	}
	if p.E2EEncryption != nil {
		add("e2e_encryption", *p.E2EEncryption)
	}
	if p.TrafficLimitBytes != nil {
		add("traffic_limit_bytes", p.TrafficLimitBytes.Value)
	}
	if p.OwnerRef != nil {
		add("owner_ref", *p.OwnerRef)
	}
	if p.ExpiresAt != nil {
		add("expires_at", *p.ExpiresAt)
	}
	if p.FinalExitNodeID != nil {
		add("final_exit_node_id", *p.FinalExitNodeID)
	}
	if p.Enabled != nil {
		add("enabled", *p.Enabled)
	}
	if len(cols) == 0 {
		return "", nil
	}
	cols = append(cols, "updated_at = now()")
	return strings.Join(cols, ", "), args
}

// UpdateKeyGuarded applies a partial update in one transaction, holding the advisory locks for
// both the old and the new link set so two keys cannot be swapped onto the same document at once.
func (s *Store) UpdateKeyGuarded(ctx context.Context, id string, p UpdateKeyParams) (model.Key, error) {
	set, args := p.assignments()
	if set == "" {
		k, err := s.GetKeyByID(ctx, id)
		return k, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Key{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	current, err := getKeyByIDTx(ctx, tx, id)
	if err != nil {
		return model.Key{}, err
	}

	lockSet := p.urlSet()
	if p.docURLsTouched() {
		lockSet = append(lockSet, current.DocURL)
		lockSet = append(lockSet, current.DocURLs...)
	}
	if err := lockDocURLs(ctx, tx, lockSet); err != nil {
		return model.Key{}, err
	}

	if p.docURLsTouched() {
		inUse, err := docURLInUseTx(ctx, tx, lockSet, id)
		if err != nil {
			return model.Key{}, err
		}
		if inUse {
			return model.Key{}, ErrDocURLInUse
		}
	}

	args = append(args, id)
	row := tx.QueryRow(ctx, `UPDATE keys SET `+set+fmt.Sprintf(" WHERE id = $%d AND deleted_at IS NULL RETURNING ", len(args))+keyColumns, args...)
	k, err := scanKey(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("update key: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Key{}, fmt.Errorf("commit: %w", err)
	}
	return k, nil
}

func getKeyByIDTx(ctx context.Context, tx pgx.Tx, id string) (model.Key, error) {
	k, err := scanKey(tx.QueryRow(ctx, `SELECT `+keyColumns+` FROM keys WHERE id = $1 AND deleted_at IS NULL`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Key{}, ErrNotFound
	}
	if err != nil {
		return model.Key{}, fmt.Errorf("get key: %w", err)
	}
	return k, nil
}

// DeleteKey soft-deletes: the row (and its lifetime usage totals) stays for the panel's charts, since every other Store method already treats deleted_at IS NOT NULL as "doesn't exist".
func (s *Store) DeleteKey(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE keys SET deleted_at = now(), updated_at = now() WHERE id = $1 AND deleted_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
