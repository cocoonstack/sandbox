package main

import (
	"cmp"
	"context"
	"os"
	"path/filepath"
	"regexp"

	archivespg "github.com/cocoonstack/sandbox/sandboxd/archives/pg"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	poolpg "github.com/cocoonstack/sandbox/sandboxd/poolset/pg"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/store/s3"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/file"
	tenantspg "github.com/cocoonstack/sandbox/sandboxd/tenants/pg"
)

func openBackends(ctx context.Context, cfg *config.Config) (pool.Backends, error) {
	var b pool.Backends
	var err error
	// Checkpoints and templates are two id-namespaced views (ck_*, tp_*) over one store root
	if b.Checkpoints, err = newStoreView(ctx, cfg, "checkpoint-staging", store.CheckpointIDRe); err != nil {
		return pool.Backends{}, err
	}
	b.SharedCheckpoints = cfg.CheckpointStore != nil && cfg.CheckpointStore.Kind == "s3"
	if b.Templates, err = newStoreView(ctx, cfg, "template-staging", store.TemplateIDRe); err != nil {
		return pool.Backends{}, err
	}
	if b.Tenants, err = openTenants(ctx, cfg); err != nil {
		return pool.Backends{}, err
	}
	ms := cfg.MetaStore
	if ms != nil && ms.Cell != "" {
		if b.CellPools, err = poolpg.Open(ctx, os.Getenv(ms.DSNEnv), ms.Cell); err != nil {
			return pool.Backends{}, err
		}
	}
	if cfg.ArchiveFailover {
		if b.Archives, err = archivespg.Open(ctx, os.Getenv(ms.DSNEnv), ms.Cell); err != nil {
			return pool.Backends{}, err
		}
	}
	return b, nil
}

func newStoreView(ctx context.Context, cfg *config.Config, staging string, idRe *regexp.Regexp) (store.Store, error) {
	if cs := cfg.CheckpointStore; cs != nil && cs.Kind == "s3" {
		return s3.New(ctx, *cs.S3, filepath.Join(cfg.DataDir, staging), idRe)
	}
	return dir.New(cmp.Or(cfg.CheckpointDir, filepath.Join(cfg.DataDir, "checkpoints")), idRe)
}

func openTenants(ctx context.Context, cfg *config.Config) (tenants.Source, error) {
	if ms := cfg.MetaStore; ms != nil {
		return tenantspg.Open(ctx, os.Getenv(ms.DSNEnv))
	}
	var rootSum string
	if cfg.APIToken != "" {
		rootSum = config.TokenSHA256(cfg.APIToken)
	}
	return file.Open(ctx, cfg.DataDir, rootSum)
}
