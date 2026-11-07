// Package pg keeps the archive registry in one shared PostgreSQL table.
package pg

import (
	"context"
	_ "embed"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
	"github.com/cocoonstack/sandbox/sandboxd/metastore"
)

const (
	table = "sandboxd_archives"

	leaseExpired = `NOT EXISTS (SELECT 1 FROM sandboxd_nodes n WHERE n.cell = sandboxd_archives.cell AND n.node = sandboxd_archives.owner AND n.lease_until > now())`
)

var (
	//go:embed schema.sql
	schema string

	cas = metastore.CAS{Lost: archives.ErrLost, Unavailable: archives.ErrUnavailable}
)

var _ archives.Registry = (*Registry)(nil)

// Registry is the shared archive table.
type Registry struct {
	pool *pgxpool.Pool
	cell string
}

// Open connects to cell's rows and creates the tables when they are missing.
func Open(ctx context.Context, dsn, cell string) (*Registry, error) {
	pool, _, err := metastore.Open(ctx, dsn, table, schema)
	if err != nil {
		return nil, err
	}
	return &Registry{pool: pool, cell: cell}, nil
}

func (r *Registry) Put(ctx context.Context, rec archives.Record) (int64, error) {
	return cas.Advance(ctx, r.pool, `INSERT INTO sandboxd_archives (id, tenant, token_sha256, owner, epoch, checkpoint, deadline, claim, cell)
		VALUES ($1, $2, $3, $4, nextval('sandboxd_archives_epoch'), $5, $6, $7, $8)
		ON CONFLICT (cell, id) DO UPDATE SET tenant = EXCLUDED.tenant, token_sha256 = EXCLUDED.token_sha256,
			epoch = EXCLUDED.epoch, consumed = false, checkpoint = EXCLUDED.checkpoint,
			deadline = EXCLUDED.deadline, claim = EXCLUDED.claim, updated_at = now()
		WHERE sandboxd_archives.owner = EXCLUDED.owner
		RETURNING epoch`,
		rec.ID, rec.Tenant, rec.TokenSHA256, rec.Owner, rec.Checkpoint, pgtype.Timestamptz{Time: rec.Deadline, Valid: !rec.Deadline.IsZero()}, []byte(rec.Claim), r.cell)
}

func (r *Registry) Replace(ctx context.Context, rec archives.Record, expect int64) (int64, error) {
	if expect == 0 {
		return r.Put(ctx, rec)
	}
	return cas.Advance(ctx, r.pool, `UPDATE sandboxd_archives SET tenant = $5, token_sha256 = $6, epoch = nextval('sandboxd_archives_epoch'),
			checkpoint = $7, deadline = $8, claim = $9, updated_at = now()
		WHERE cell = $1 AND id = $2 AND epoch = $3 AND owner = $4 AND NOT consumed
		RETURNING epoch`, r.cell, rec.ID, expect, rec.Owner, rec.Tenant, rec.TokenSHA256, rec.Checkpoint, pgtype.Timestamptz{Time: rec.Deadline, Valid: !rec.Deadline.IsZero()}, []byte(rec.Claim))
}

func (r *Registry) Take(ctx context.Context, id string, expect int64, owner string) (int64, error) {
	return cas.Advance(ctx, r.pool, `UPDATE sandboxd_archives SET owner = $4, epoch = nextval('sandboxd_archives_epoch'), updated_at = now()
		WHERE cell = $1 AND id = $2 AND epoch = $3 AND NOT consumed AND (owner = $4 OR `+leaseExpired+`)
		RETURNING epoch`, r.cell, id, expect, owner)
}

func (r *Registry) Release(ctx context.Context, id string, expect int64, owner string) (int64, error) {
	return cas.Advance(ctx, r.pool, `UPDATE sandboxd_archives SET owner = '', epoch = nextval('sandboxd_archives_epoch'), updated_at = now()
		WHERE cell = $1 AND id = $2 AND epoch = $3 AND owner = $4 AND NOT consumed
		RETURNING epoch`, r.cell, id, expect, owner)
}

func (r *Registry) TakeOwned(ctx context.Context, owner string, expect map[string]int64) (map[string]int64, error) {
	ids, epochs := make([]string, 0, len(expect)), make([]int64, 0, len(expect))
	for id, epoch := range expect {
		ids, epochs = append(ids, id), append(epochs, epoch)
	}
	recs, err := cas.All(ctx, r.pool, scan, `UPDATE sandboxd_archives a SET epoch = nextval('sandboxd_archives_epoch'), updated_at = now()
		FROM unnest($2::text[], $3::bigint[]) AS t(id, epoch)
		WHERE a.cell = $1 AND a.id = t.id AND a.epoch = t.epoch AND a.owner = $4 AND NOT a.consumed
		RETURNING a.id, a.tenant, a.token_sha256, a.owner, a.epoch, a.consumed, a.checkpoint, a.deadline, NULL::jsonb`, r.cell, ids, epochs, owner)
	if err != nil {
		return nil, err
	}
	taken := make(map[string]int64, len(recs))
	for _, rec := range recs {
		taken[rec.ID] = rec.Epoch
	}
	return taken, nil
}

func (r *Registry) Consume(ctx context.Context, id string, expect int64) (int64, error) {
	return cas.Advance(ctx, r.pool, `UPDATE sandboxd_archives SET consumed = true, epoch = nextval('sandboxd_archives_epoch'), updated_at = now()
		WHERE cell = $1 AND id = $2 AND epoch = $3 AND NOT consumed
		RETURNING epoch`, r.cell, id, expect)
}

func (r *Registry) Delete(ctx context.Context, id string, epoch int64) error {
	return cas.Delete(ctx, r.pool, `DELETE FROM sandboxd_archives WHERE cell = $3 AND id = $1 AND epoch = $2`, id, epoch, r.cell)
}

func (r *Registry) Get(ctx context.Context, id string) (archives.Record, bool, error) {
	return cas.One(ctx, r.pool, scan, `SELECT id, tenant, token_sha256, owner, epoch, consumed, checkpoint, deadline, claim
		FROM sandboxd_archives WHERE cell = $2 AND id = $1`, id, r.cell)
}

func (r *Registry) Sweepable(ctx context.Context, now time.Time, owner string) ([]archives.Record, error) {
	return cas.All(ctx, r.pool, scan, `SELECT id, tenant, token_sha256, owner, epoch, consumed, checkpoint, deadline, NULL::jsonb
		FROM sandboxd_archives WHERE cell = $2 AND (consumed OR owner = $3 OR deadline < $1 AND `+leaseExpired+`)`, now, r.cell, owner)
}

func (r *Registry) Rows(ctx context.Context, ids []string) (map[string]archives.Record, error) {
	recs, err := cas.All(ctx, r.pool, scan, `SELECT id, tenant, token_sha256, owner, epoch, consumed, checkpoint, deadline, NULL::jsonb
		FROM sandboxd_archives WHERE cell = $1 AND id = ANY($2)`, r.cell, ids)
	if err != nil {
		return nil, err
	}
	rows := make(map[string]archives.Record, len(recs))
	for _, rec := range recs {
		rows[rec.ID] = rec
	}
	return rows, nil
}

func (r *Registry) Renew(ctx context.Context, node string, ttl time.Duration) error {
	qctx, cancel := context.WithTimeout(ctx, metastore.QueryTimeout)
	defer cancel()
	_, err := r.pool.Exec(qctx, `INSERT INTO sandboxd_nodes (cell, node, lease_until)
		VALUES ($1, $2, now() + $3::float8 * interval '1 second')
		ON CONFLICT (cell, node) DO UPDATE SET lease_until = EXCLUDED.lease_until`, r.cell, node, ttl.Seconds())
	if err != nil {
		return cas.Down(err)
	}
	return nil
}

// Close releases the connection pool.
func (r *Registry) Close() {
	r.pool.Close()
}

func scan(row pgx.Row) (archives.Record, error) {
	var (
		rec      archives.Record
		deadline pgtype.Timestamptz
		claim    []byte
	)
	if err := row.Scan(&rec.ID, &rec.Tenant, &rec.TokenSHA256, &rec.Owner, &rec.Epoch, &rec.Consumed, &rec.Checkpoint, &deadline, &claim); err != nil {
		return archives.Record{}, err
	}
	rec.Deadline, rec.Claim = deadline.Time, claim
	return rec, nil
}
