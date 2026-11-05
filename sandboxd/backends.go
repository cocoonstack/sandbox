package main

import (
	"cmp"
	"context"
	"os"
	"path/filepath"
	"regexp"

	"github.com/projecteru2/core/log"

	archivespg "github.com/cocoonstack/sandbox/sandboxd/archives/pg"
	"github.com/cocoonstack/sandbox/sandboxd/config"
	movespg "github.com/cocoonstack/sandbox/sandboxd/moves/pg"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	poolpg "github.com/cocoonstack/sandbox/sandboxd/poolset/pg"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/dir"
	"github.com/cocoonstack/sandbox/sandboxd/store/s3"
	"github.com/cocoonstack/sandbox/sandboxd/tenants"
	"github.com/cocoonstack/sandbox/sandboxd/tenants/file"
	tenantspg "github.com/cocoonstack/sandbox/sandboxd/tenants/pg"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet"
	tunpg "github.com/cocoonstack/sandbox/sandboxd/tunnet/pg"
	"github.com/cocoonstack/sandbox/sandboxd/tunnet/udp"
	usagepg "github.com/cocoonstack/sandbox/sandboxd/usage/pg"
)

func openBackends(ctx context.Context, cfg *config.Config) (pool.Backends, func(context.Context), error) {
	var b pool.Backends
	var err error
	if b.Checkpoints, err = newStoreView(ctx, cfg, "checkpoint-staging", store.CheckpointIDRe, false); err != nil {
		return pool.Backends{}, nil, err
	}
	b.SharedCheckpoints = cfg.S3Checkpoints()
	if b.Templates, err = newStoreView(ctx, cfg, "template-staging", store.TemplateIDRe, true); err != nil {
		return pool.Backends{}, nil, err
	}
	if b.Tenants, err = openTenants(ctx, cfg); err != nil {
		return pool.Backends{}, nil, err
	}
	ms := cfg.MetaStore
	if ms != nil && ms.Cell != "" {
		if b.CellPools, err = poolpg.Open(ctx, os.Getenv(ms.DSNEnv), ms.Cell); err != nil {
			return pool.Backends{}, nil, err
		}
	}
	if cfg.ArchiveFailover {
		if b.Archives, err = archivespg.Open(ctx, os.Getenv(ms.DSNEnv), ms.Cell); err != nil {
			return pool.Backends{}, nil, err
		}
	}
	if cfg.Movable() {
		if b.Moves, err = movespg.Open(ctx, os.Getenv(ms.DSNEnv), ms.Cell); err != nil {
			return pool.Backends{}, nil, err
		}
	}
	runTun := func(context.Context) {}
	if cfg.Tun != nil {
		if runTun, err = openTun(ctx, cfg, &b); err != nil {
			return pool.Backends{}, nil, err
		}
	}
	return b, runTun, nil
}

// openTun returns run, which receives VXLAN until its context ends.
func openTun(ctx context.Context, cfg *config.Config, b *pool.Backends) (func(context.Context), error) {
	t := cfg.Tun
	under, err := udp.Listen(t.ListenAddr(), t.Senders)
	if err != nil {
		return nil, err
	}
	var addrs tunnet.Addresses
	if cfg.Mesh != nil {
		ms := cfg.MetaStore
		addrs, err = tunpg.Open(ctx, os.Getenv(ms.DSNEnv), ms.Cell, t.Prefix(), t.ReuseAfter())
	} else {
		addrs, err = tunnet.NewLocal(cfg.DataDir, t.Prefix(), t.ReuseAfter())
	}
	if err != nil {
		under.Close()
		return nil, err
	}
	router := tunnet.New(under, t.Overlay())
	b.TunRouter, b.TunAddrs = router, addrs
	return func(ctx context.Context) {
		// closing the underlay is what ends the blocked receive
		context.AfterFunc(ctx, under.Close)
		log.WithFunc("main.openTun").Error(ctx, router.Run(ctx), "tun router stopped; the advanced network mode is down")
	}, nil
}

func openUsage(ctx context.Context, cfg *config.Config) (*usagepg.Store, error) {
	ms := cfg.MetaStore
	if ms == nil || !ms.Usage {
		return nil, nil
	}
	return usagepg.Open(ctx, os.Getenv(ms.DSNEnv), ms.Cell)
}

func usageNode(cfg *config.Config) (string, error) {
	if cfg.Mesh != nil {
		return cfg.Mesh.ID(), nil
	}
	return os.Hostname()
}

func newStoreView(ctx context.Context, cfg *config.Config, staging string, idRe *regexp.Regexp, labeled bool) (store.Store, error) {
	if cfg.S3Checkpoints() {
		return s3.New(ctx, *cfg.CheckpointStore.S3, filepath.Join(cfg.DataDir, staging), idRe, labeled)
	}
	return dir.New(cmp.Or(cfg.CheckpointDir, filepath.Join(cfg.DataDir, "checkpoints")), idRe)
}

func openTenants(ctx context.Context, cfg *config.Config) (tenants.Source, error) {
	if ms := cfg.MetaStore; ms != nil {
		return tenantspg.Open(ctx, os.Getenv(ms.DSNEnv))
	}
	var rootSum string
	if cfg.APIToken != "" {
		rootSum = tenants.TokenSHA256(cfg.APIToken)
	}
	return file.Open(ctx, cfg.DataDir, rootSum)
}
