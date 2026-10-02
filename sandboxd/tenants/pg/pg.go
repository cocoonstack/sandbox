// Package pg reads the tenant set from one PostgreSQL table every node shares,
// through a per-node cache of the tenants that node has seen. A write commits
// with a NOTIFY that evicts the name on every node; a node whose listener
// reconnects drops its whole cache, so it never serves a change it missed.
// While the database is down a cached tenant keeps working and an uncached
// one fails closed.
package pg

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/projecteru2/core/log"
	"golang.org/x/time/rate"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
)

const (
	channel  = "sandboxd_tenants"
	flushAll = "*"

	queryTimeout = 3 * time.Second
	negativeTTL  = 10 * time.Second
	// maxCached bounds the per-node cache; a node serves far fewer distinct tenants.
	maxCached = 100_000
	// coldLookups caps backend queries per second for tokens and names the cache lacks, so random tokens cannot load the database.
	coldLookups = 200

	listenRetryFirst = time.Second
	listenRetryCap   = 30 * time.Second

	uniqueViolation = "23505"
)

var (
	//go:embed schema.sql
	schema string

	_ tenants.Source = (*Source)(nil)
)

// Source is the shared tenant table behind this node's cache.
type Source struct {
	pool *pgxpool.Pool
	dsn  string
	cold *rate.Limiter

	mu       sync.RWMutex
	byToken  map[[sha256.Size]byte]string
	byName   map[string]entry
	misses   map[[sha256.Size]byte]time.Time
	inflight map[string]struct{}
	// gen moves on every eviction, so a lookup that raced one does not cache what it read.
	gen uint64

	stop context.CancelFunc
	done chan struct{}
}

// Open connects, creates the table when it is missing, and starts the invalidation listener.
func Open(ctx context.Context, dsn string) (*Source, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open tenant database: %w", err)
	}
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	if _, err = pool.Exec(qctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("create tenant table: %w", err)
	}
	lctx, stop := context.WithCancel(context.WithoutCancel(ctx))
	s := &Source{
		pool:     pool,
		dsn:      dsn,
		cold:     rate.NewLimiter(coldLookups, coldLookups),
		byToken:  map[[sha256.Size]byte]string{},
		byName:   map[string]entry{},
		misses:   map[[sha256.Size]byte]time.Time{},
		inflight: map[string]struct{}{},
		stop:     stop,
		done:     make(chan struct{}),
	}
	go s.listen(lctx)
	return s, nil
}

func (s *Source) Resolve(ctx context.Context, sum [sha256.Size]byte) (*config.TenantRecord, error) {
	s.mu.RLock()
	e := s.byName[s.byToken[sum]]
	until, negative := s.misses[sum]
	gen := s.gen
	s.mu.RUnlock()
	switch {
	case e.present && e.sum == sum:
		return e.rec, nil
	case negative && time.Now().Before(until):
		return nil, tenants.ErrUnknown
	case !s.cold.Allow():
		return nil, fmt.Errorf("%w: cold lookups over %d/s", tenants.ErrUnavailable, coldLookups)
	}
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	r, err := scanRecord(s.pool.QueryRow(qctx, `SELECT name, token_sha256, max_claims, egress_class FROM sandboxd_tenants WHERE token_sha256 = $1`, sum[:]))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		s.remember(gen, func() {
			if len(s.misses) >= maxCached {
				clear(s.misses)
			}
			s.misses[sum] = time.Now().Add(negativeTTL)
		})
		return nil, tenants.ErrUnknown
	case err != nil:
		return nil, fmt.Errorf("%w: %w", tenants.ErrUnavailable, err)
	}
	s.remember(gen, func() { s.cache(&r, sum) })
	return &r, nil
}

func (s *Source) Peek(name string) (config.TenantRecord, tenants.Presence) {
	s.mu.RLock()
	e, ok := s.byName[name]
	s.mu.RUnlock()
	switch {
	case !ok:
		s.fetch(name)
		return config.TenantRecord{}, tenants.Unknown
	case e.present:
		return *e.rec, tenants.Present
	default:
		return config.TenantRecord{}, tenants.Absent
	}
}

func (s *Source) List(ctx context.Context, after string, limit int) ([]config.TenantRecord, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, max_claims, egress_class FROM sandboxd_tenants WHERE name > $1 ORDER BY name LIMIT $2`, after, limit)
	if err != nil {
		return nil, unavailable(err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (config.TenantRecord, error) {
		var r config.TenantRecord
		return r, row.Scan(&r.Name, &r.MaxClaims, &r.EgressClass)
	})
	return out, unavailable(err)
}

func (s *Source) Put(ctx context.Context, r config.TenantRecord) (tenants.Change, error) {
	var c tenants.Change
	err := s.write(ctx, r.Name, func(tx pgx.Tx) error {
		if r.TokenSHA256 == "" {
			return keepTokenUpdate(ctx, tx, r, &c)
		}
		sum, err := hex.DecodeString(r.TokenSHA256)
		if err != nil {
			return fmt.Errorf("%w: tenant %q token_sha256 is not hex", tenants.ErrInvalid, r.Name)
		}
		var inserted bool
		err = tx.QueryRow(ctx, `INSERT INTO sandboxd_tenants AS t (name, token_sha256, max_claims, egress_class) VALUES ($1, $2, $3, $4)
			ON CONFLICT (name) DO UPDATE SET token_sha256 = EXCLUDED.token_sha256, max_claims = EXCLUDED.max_claims, egress_class = EXCLUDED.egress_class, updated_at = now()
			WHERE (t.token_sha256, t.max_claims, t.egress_class) IS DISTINCT FROM (EXCLUDED.token_sha256, EXCLUDED.max_claims, EXCLUDED.egress_class)
			RETURNING xmax = 0`, r.Name, sum, r.MaxClaims, r.EgressClass).Scan(&inserted)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		case inserted:
			c.Added = []string{r.Name}
		default:
			c.Changed = []string{r.Name}
		}
		return nil
	})
	return c, err
}

func (s *Source) Delete(ctx context.Context, name string) (tenants.Change, error) {
	err := s.write(ctx, name, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM sandboxd_tenants WHERE name = $1`, name)
		if err == nil && tag.RowsAffected() == 0 {
			return tenants.ErrUnknown
		}
		return err
	})
	if err != nil {
		return tenants.Change{}, err
	}
	return tenants.Change{Removed: []string{name}}, nil
}

func (s *Source) Replace(ctx context.Context, records []config.TenantRecord) (tenants.Change, error) {
	var c tenants.Change
	err := s.write(ctx, flushAll, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `LOCK TABLE sandboxd_tenants IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT name, token_sha256, max_claims, egress_class FROM sandboxd_tenants`)
		if err != nil {
			return err
		}
		cur, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (config.TenantRecord, error) { return scanRecord(row) })
		if err != nil {
			return err
		}
		next, err := keepTokens(indexByName(cur), records)
		if err != nil {
			return err
		}
		c = diff(indexByName(cur), next)
		if _, err = tx.Exec(ctx, `DELETE FROM sandboxd_tenants WHERE NOT (name = ANY($1))`, slices.AppendSeq([]string{}, maps.Keys(next))); err != nil {
			return err
		}
		var batch pgx.Batch
		for _, name := range slices.Concat(c.Added, c.Changed) {
			r := next[name]
			sum, _ := hex.DecodeString(r.TokenSHA256) // keepTokens validated it
			batch.Queue(`INSERT INTO sandboxd_tenants (name, token_sha256, max_claims, egress_class) VALUES ($1, $2, $3, $4)
				ON CONFLICT (name) DO UPDATE SET token_sha256 = EXCLUDED.token_sha256, max_claims = EXCLUDED.max_claims, egress_class = EXCLUDED.egress_class, updated_at = now()`,
				r.Name, sum, r.MaxClaims, r.EgressClass)
		}
		return tx.SendBatch(ctx, &batch).Close()
	})
	if err != nil {
		return tenants.Change{}, err
	}
	return c, nil
}

func (s *Source) Classes(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT egress_class, count(*) FROM sandboxd_tenants WHERE egress_class <> '' GROUP BY egress_class`)
	if err != nil {
		return nil, unavailable(err)
	}
	out := map[string]int{}
	var class string
	var n int
	if _, err = pgx.ForEachRow(rows, []any{&class, &n}, func() error { out[class] = n; return nil }); err != nil {
		return nil, unavailable(err)
	}
	return out, nil
}

func (s *Source) Records() []config.TenantRecord {
	return nil
}

func (s *Source) Digest() string {
	return ""
}

func (s *Source) Close() error {
	s.stop()
	<-s.done
	s.pool.Close()
	return nil
}

// write runs f in one transaction that notifies every node to evict key, then evicts it here at once.
func (s *Source) write(ctx context.Context, key string, f func(tx pgx.Tx) error) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := f(tx); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, key)
		return err
	})
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == uniqueViolation {
		return fmt.Errorf("%w: a tenant token is already held by another tenant", tenants.ErrInvalid)
	}
	if err != nil {
		if errors.Is(err, tenants.ErrInvalid) || errors.Is(err, tenants.ErrUnknown) {
			return err
		}
		return unavailable(err)
	}
	s.evict(key)
	return nil
}

func (s *Source) remember(gen uint64, f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen == gen {
		f()
	}
}

// cache stores a present record; callers hold s.mu.
func (s *Source) cache(r *config.TenantRecord, sum [sha256.Size]byte) {
	if len(s.byName) >= maxCached {
		s.clear()
	}
	s.byName[r.Name] = entry{rec: r, sum: sum, present: true}
	s.byToken[sum] = r.Name
}

// fetch loads one name in the background for Peek, once per name at a time.
func (s *Source) fetch(name string) {
	s.mu.Lock()
	if _, busy := s.inflight[name]; busy || !s.cold.Allow() {
		s.mu.Unlock()
		return
	}
	s.inflight[name] = struct{}{}
	gen := s.gen
	s.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
		defer cancel()
		r, err := scanRecord(s.pool.QueryRow(ctx, `SELECT name, token_sha256, max_claims, egress_class FROM sandboxd_tenants WHERE name = $1`, name))
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.inflight, name)
		switch {
		case s.gen != gen:
		case errors.Is(err, pgx.ErrNoRows):
			s.byName[name] = entry{}
		case err == nil:
			sum, _ := hex.DecodeString(r.TokenSHA256)
			s.cache(&r, [sha256.Size]byte(sum))
		}
	}()
}

func (s *Source) evict(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen++
	if key == flushAll {
		s.clear()
		return
	}
	if e := s.byName[key]; e.present {
		delete(s.byToken, e.sum)
	}
	delete(s.byName, key)
	clear(s.misses)
}

// clear drops the whole cache; callers hold s.mu.
func (s *Source) clear() {
	s.gen++
	clear(s.byToken)
	clear(s.byName)
	clear(s.misses)
}

func (s *Source) listen(ctx context.Context) {
	defer close(s.done)
	logger := log.WithFunc("pg.listen")
	backoff := listenRetryFirst
	for {
		started := time.Now()
		err := s.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > listenRetryCap {
			backoff = listenRetryFirst
		}
		logger.Warnf(ctx, "tenant invalidation listener lost (%v); cached tenants keep serving, retrying in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, listenRetryCap)
	}
}

func (s *Source) listenOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if _, err = conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}
	s.evict(flushAll)
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		s.evict(n.Payload)
	}
}

type entry struct {
	rec     *config.TenantRecord
	sum     [sha256.Size]byte
	present bool
}

func keepTokenUpdate(ctx context.Context, tx pgx.Tx, r config.TenantRecord, c *tenants.Change) error {
	var changed bool
	err := tx.QueryRow(ctx, `WITH cur AS (SELECT max_claims, egress_class FROM sandboxd_tenants WHERE name = $1 FOR UPDATE),
		upd AS (UPDATE sandboxd_tenants t SET max_claims = $2, egress_class = $3, updated_at = now() FROM cur
			WHERE t.name = $1 AND (cur.max_claims, cur.egress_class) IS DISTINCT FROM ($2, $3) RETURNING 1)
		SELECT EXISTS (SELECT 1 FROM upd) FROM cur`, r.Name, r.MaxClaims, r.EgressClass).Scan(&changed)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: tenant %q needs a token", tenants.ErrInvalid, r.Name)
	}
	if changed {
		c.Changed = []string{r.Name}
	}
	return err
}

func keepTokens(cur map[string]config.TenantRecord, records []config.TenantRecord) (map[string]config.TenantRecord, error) {
	next := make(map[string]config.TenantRecord, len(records))
	for _, r := range records {
		if _, dup := next[r.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate tenant name %q", tenants.ErrInvalid, r.Name)
		}
		if r.TokenSHA256 == "" {
			kept, ok := cur[r.Name]
			if !ok {
				return nil, fmt.Errorf("%w: tenant %q needs a token", tenants.ErrInvalid, r.Name)
			}
			r.TokenSHA256 = kept.TokenSHA256
		}
		if sum, err := hex.DecodeString(r.TokenSHA256); err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("%w: tenant %q token_sha256 must be %d hex bytes", tenants.ErrInvalid, r.Name, sha256.Size)
		}
		next[r.Name] = r
	}
	return next, nil
}

func diff(cur, next map[string]config.TenantRecord) tenants.Change {
	var c tenants.Change
	for _, name := range slices.Sorted(maps.Keys(next)) {
		switch old, ok := cur[name]; {
		case !ok:
			c.Added = append(c.Added, name)
		case old != next[name]:
			c.Changed = append(c.Changed, name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(cur)) {
		if _, ok := next[name]; !ok {
			c.Removed = append(c.Removed, name)
		}
	}
	return c
}

func indexByName(records []config.TenantRecord) map[string]config.TenantRecord {
	out := make(map[string]config.TenantRecord, len(records))
	for _, r := range records {
		out[r.Name] = r
	}
	return out
}

func scanRecord(row pgx.Row) (config.TenantRecord, error) {
	var r config.TenantRecord
	var sum []byte
	if err := row.Scan(&r.Name, &sum, &r.MaxClaims, &r.EgressClass); err != nil {
		return config.TenantRecord{}, err
	}
	r.TokenSHA256 = hex.EncodeToString(sum)
	return r, nil
}

func unavailable(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", tenants.ErrUnavailable, err)
}
