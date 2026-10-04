// Package metastore connects to the shared PostgreSQL meta store and follows its NOTIFY channels.
package metastore

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/projecteru2/core/log"
)

const (
	// QueryTimeout bounds one meta store query.
	QueryTimeout = 3 * time.Second

	dialTimeout = 10 * time.Second

	listenRetryFirst = time.Second
	listenRetryCap   = 30 * time.Second
)

// keepAlive finds a dead connection in about 25 s, so a listener on a vanished primary reconnects and re-syncs.
var keepAlive = net.KeepAliveConfig{Enable: true, Idle: 10 * time.Second, Interval: 5 * time.Second, Count: 3}

// Open connects and runs schema only when table is missing, so a role without CREATE serves a pre-created table.
func Open(ctx context.Context, dsn, table, schema string) (*pgxpool.Pool, *pgx.ConnConfig, error) {
	dialer := &net.Dialer{Timeout: dialTimeout, KeepAliveConfig: keepAlive}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("parse meta store dsn: %w", err)
	}
	poolCfg.ConnConfig.DialFunc = dialer.DialContext
	listenCfg := poolCfg.ConnConfig.Copy()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("open meta store: %w", err)
	}
	if err = ensureTable(ctx, pool, table, schema); err != nil {
		pool.Close()
		return nil, nil, err
	}
	return pool, listenCfg, nil
}

// Listen calls synced on each listener connection and notified on each notify, reconnecting with backoff until ctx ends.
func Listen(ctx context.Context, cfg *pgx.ConnConfig, channel string, synced func(), notified func(payload string)) {
	logger := log.WithFunc("metastore.Listen")
	listenOnce := func() error {
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
		if _, err = conn.Exec(ctx, "LISTEN "+channel); err != nil {
			return err
		}
		synced()
		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				return err
			}
			notified(n.Payload)
		}
	}
	backoff := listenRetryFirst
	for {
		started := time.Now()
		err := listenOnce()
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > listenRetryCap {
			backoff = listenRetryFirst
		}
		logger.Warnf(ctx, "%s listener lost (%v); the last synced state keeps serving, retrying in %s", channel, err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, listenRetryCap)
	}
}

func ensureTable(ctx context.Context, pool *pgxpool.Pool, table, schema string) error {
	qctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	var exists bool
	if err := pool.QueryRow(qctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
		return fmt.Errorf("find table %s: %w", table, err)
	}
	if exists {
		return nil
	}
	if _, err := pool.Exec(qctx, schema); err != nil {
		return fmt.Errorf("create table %s: %w", table, err)
	}
	return nil
}
