package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"runbox/internal/audit"
	"runbox/internal/auth"
	"runbox/internal/ids"
)

// Key is an API key record (never includes the plaintext).
type Key struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Prefix        string     `json:"prefix"`
	MaxConcurrent int        `json:"max_concurrent"`
	RateRPS       *float64   `json:"rate_rps,omitempty"`
	RateBurst     *int       `json:"rate_burst,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	RevokedAt     *time.Time `json:"revoked_at,omitempty"`
}

// NewKeyParams configures CreateKey.
type NewKeyParams struct {
	Name          string
	MaxConcurrent int
	RateRPS       *float64
	RateBurst     *int
}

const keyCols = `id::text, name, key_prefix, max_concurrent, rate_rps, rate_burst, created_at, revoked_at`

func scanKey(row pgx.Row) (*Key, error) {
	var k Key
	err := row.Scan(&k.ID, &k.Name, &k.Prefix, &k.MaxConcurrent, &k.RateRPS, &k.RateBurst, &k.CreatedAt, &k.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &k, err
}

// CreateKey creates a key and returns the record plus the plaintext key,
// which is not stored and cannot be recovered later.
func (s *Store) CreateKey(ctx context.Context, p NewKeyParams, actor string) (*Key, string, error) {
	if p.MaxConcurrent <= 0 {
		p.MaxConcurrent = 4
	}
	plain := auth.NewKey()
	var k *Key
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		k, err = scanKey(tx.QueryRow(ctx,
			`INSERT INTO api_keys (id, name, key_hash, key_prefix, max_concurrent, rate_rps, rate_burst)
			 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING `+keyCols,
			ids.NewUUID(), p.Name, auth.Hash(plain), auth.DisplayPrefix(plain), p.MaxConcurrent, p.RateRPS, p.RateBurst))
		if err != nil {
			return err
		}
		_, err = audit.Append(ctx, tx, actor, audit.ActionKeyCreated, "", map[string]any{
			"key_id": k.ID, "name": k.Name, "prefix": k.Prefix, "max_concurrent": k.MaxConcurrent,
		})
		return err
	})
	if err != nil {
		return nil, "", err
	}
	return k, plain, nil
}

// KeyByPlaintext looks up an active (non-revoked) key.
func (s *Store) KeyByPlaintext(ctx context.Context, plain string) (*Key, error) {
	return scanKey(s.Pool.QueryRow(ctx,
		`SELECT `+keyCols+` FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, auth.Hash(plain)))
}

// ListKeys returns all keys, newest first.
func (s *Store) ListKeys(ctx context.Context) ([]Key, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+keyCols+` FROM api_keys ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *k)
	}
	return out, rows.Err()
}

// RevokeKey revokes a key. Revoking twice is a conflict.
func (s *Store) RevokeKey(ctx context.Context, id, actor string) (*Key, error) {
	if !ids.ValidUUID(id) {
		return nil, ErrNotFound
	}
	var k *Key
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		k, err = scanKey(tx.QueryRow(ctx,
			`UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL RETURNING `+keyCols, id))
		if errors.Is(err, ErrNotFound) {
			var exists bool
			if e := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM api_keys WHERE id = $1)`, id).Scan(&exists); e != nil {
				return e
			}
			if exists {
				return ErrConflict
			}
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		_, err = audit.Append(ctx, tx, actor, audit.ActionKeyRevoked, "", map[string]any{"key_id": k.ID, "name": k.Name})
		return err
	})
	return k, err
}
