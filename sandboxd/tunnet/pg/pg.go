// Package pg keeps a cell's overlay addresses in one shared PostgreSQL table, so two nodes never hand out the same one.
package pg

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/sandbox/sandboxd/metastore"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
)

const (
	table = "sandboxd_tun_addrs"
	// assignTries bounds the candidates one Assign tries before it reports the subnet full.
	assignTries = 64

	assignSQL = `WITH held AS (
		SELECT address FROM sandboxd_tun_addrs WHERE cell = $1 AND claim = $2 AND released_at IS NULL
	), taken AS (
		INSERT INTO sandboxd_tun_addrs (cell, address, claim)
		SELECT $1, host($3::inet + (1 + nextval('sandboxd_tun_addrs_cursor') % $4)), $2 WHERE NOT EXISTS (SELECT 1 FROM held)
		ON CONFLICT (cell, address) DO UPDATE SET claim = EXCLUDED.claim, allocated_at = now(), seen_at = now(), released_at = NULL
		WHERE sandboxd_tun_addrs.released_at < now() - make_interval(secs => $5)
		RETURNING address
	)
	SELECT address FROM held UNION ALL SELECT address FROM taken`
)

//go:embed schema.sql
var schema string

var _ tunnet.Addresses = (*Store)(nil)

// Store is the cell's address table as one node sees it.
type Store struct {
	pool   *pgxpool.Pool
	cell   string
	subnet netip.Prefix
	reuse  time.Duration
}

// Open connects to cell's addresses and creates the table when it is missing.
func Open(ctx context.Context, dsn, cell string, subnet netip.Prefix, reuse time.Duration) (*Store, error) {
	pool, _, err := metastore.Open(ctx, dsn, table, schema)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool, cell: cell, subnet: subnet.Masked(), reuse: reuse}, nil
}

// Assign returns claimID's active address, or claims the next candidate from the shared cursor that is free and past its reuse window, one round trip per candidate.
func (s *Store) Assign(ctx context.Context, claimID string) (netip.Addr, error) {
	hosts := int64(1)<<(32-s.subnet.Bits()) - 2
	for range assignTries {
		var addr string
		qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
		err := s.pool.QueryRow(qctx, assignSQL, s.cell, claimID, s.subnet.Addr().String(), hosts, s.reuse.Seconds()).Scan(&addr)
		cancel()
		switch {
		case err == nil:
			return netip.ParseAddr(addr)
		case !errors.Is(err, pgx.ErrNoRows):
			return netip.Addr{}, down(err)
		}
	}
	return netip.Addr{}, tunnet.ErrNoAddress
}

// Release stamps addr released; the stamp starts its reuse window.
func (s *Store) Release(ctx context.Context, claimID string, addr netip.Addr) error {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	if _, err := s.pool.Exec(qctx, `UPDATE sandboxd_tun_addrs SET released_at = now()
		WHERE cell = $1 AND address = $2 AND claim = $3 AND released_at IS NULL`, s.cell, addr.String(), claimID); err != nil {
		return down(err)
	}
	return nil
}

// Confirm stamps the active addresses of claims as seen now.
func (s *Store) Confirm(ctx context.Context, claims []string) error {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	if _, err := s.pool.Exec(qctx, `UPDATE sandboxd_tun_addrs SET seen_at = now()
		WHERE cell = $1 AND released_at IS NULL AND claim = ANY($2)`, s.cell, claims); err != nil {
		return down(err)
	}
	return nil
}

// Unconfirmed returns the active addresses no node has confirmed for age, by claim.
func (s *Store) Unconfirmed(ctx context.Context, age time.Duration) (map[string]netip.Addr, error) {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	rows, err := s.pool.Query(qctx, `SELECT claim, address FROM sandboxd_tun_addrs
		WHERE cell = $1 AND released_at IS NULL AND seen_at < now() - make_interval(secs => $2)`, s.cell, age.Seconds())
	if err != nil {
		return nil, down(err)
	}
	defer rows.Close()
	stale := map[string]netip.Addr{}
	for rows.Next() {
		var claim, addr string
		if err := rows.Scan(&claim, &addr); err != nil {
			return nil, down(err)
		}
		if a, parseErr := netip.ParseAddr(addr); parseErr == nil {
			stale[claim] = a
		}
	}
	if err := rows.Err(); err != nil {
		return nil, down(err)
	}
	return stale, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func down(err error) error {
	return fmt.Errorf("tun address store: %w", err)
}
