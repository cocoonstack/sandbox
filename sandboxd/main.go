// sandboxd is the per-node sandbox control plane: it keeps warm pools of
// claim-ready microVMs, serves claims over HTTP, and relays the silkd data
// plane between clients and guests.
package main

import (
	"cmp"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/projecteru2/core/log"
	coretypes "github.com/projecteru2/core/types"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/egress"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/mesh"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/server"
)

const (
	shutdownGrace  = 5 * time.Second
	gossipInterval = time.Second
	// Slowloris protection; ReadTimeout/WriteTimeout must stay zero — cold
	// claims block up to the cold probe timeout and relays stream forever.
	readHeaderTimeout = 5 * time.Second
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "ca" {
		if err := runCA(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "sandboxd ca:", err)
			os.Exit(1)
		}
		return
	}
	configPath := flag.String("config", "/etc/sandboxd/config.json", "node config file")
	flag.Parse()

	ctx := context.Background()
	logLevel := cmp.Or(os.Getenv("SANDBOXD_LOG_LEVEL"), "info")
	logger := log.WithFunc("main")
	if err := log.SetupLog(ctx, &coretypes.ServerLogConfig{Level: logLevel}, ""); err != nil {
		logger.Fatalf(ctx, err, "setup log")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatalf(ctx, err, "load config")
	}
	eng := engine.New(cfg.CocoonBin, cfg.Bridge, cfg.Network)
	secrets, err := egress.NewSecretStore(cfg.Secrets)
	if err != nil {
		logger.Fatalf(ctx, err, "init egress secrets")
	}
	mgr, err := pool.NewManager(ctx, cfg, eng, secrets)
	if err != nil {
		logger.Fatalf(ctx, err, "init pool manager")
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A node that cannot reconcile cannot trust its view of local VMs.
	if err := mgr.Reconcile(ctx); err != nil {
		logger.Fatalf(ctx, err, "reconcile")
	}
	go mgr.Run(ctx)

	var placer server.Placer
	if cfg.Mesh != nil {
		msh, err := startMesh(cfg, mgr)
		if err != nil {
			logger.Fatalf(ctx, err, "start mesh")
		}
		defer func() { _ = msh.Shutdown() }()
		placer = msh
		mgr.SetTemplateNotifier(func() { msh.UpdateSelf(mgr.WarmCounts(), mgr.TemplateHashes()) })
		go gossipNodeState(ctx, msh, mgr)
		logger.Infof(ctx, "mesh %s joined (%d seeds)", cmp.Or(cfg.Mesh.NodeID, cfg.Mesh.Bind), len(cfg.Mesh.Join))
	}

	var preview *server.PreviewServer
	if cfg.PreviewListen != "" {
		preview = server.NewPreviewServer(cfg.PreviewSecret, cfg.PreviewAdvertise, mgr)
	}
	srv := server.New(cfg.APIToken, cfg.Tenants, cfg.AdvertiseAddr, mgr, eng, placer, preview)
	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	if preview != nil {
		previewSrv := &http.Server{Addr: cfg.PreviewListen, Handler: preview.Handler(), ReadHeaderTimeout: readHeaderTimeout}
		go func() {
			if err := previewSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				logger.Error(ctx, err, "preview server")
			}
		}()
		context.AfterFunc(ctx, func() {
			sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
			defer cancel()
			_ = previewSrv.Shutdown(sctx)
		})
		logger.Infof(ctx, "preview server on %s", cfg.PreviewListen)
	}

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		// Must outlive the canceled signal ctx to bound the drain.
		sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
		srv.CloseRelays()
	}()

	logger.Infof(ctx, "sandboxd listening on %s", cfg.Listen)
	if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf(ctx, err, "serve")
	}
	<-drained
	// A detached recommit may not have converged; leave disk matching memory.
	if err := mgr.FlushClaims(); err != nil {
		logger.Error(ctx, err, "flush claims")
	}
	logger.Info(ctx, "sandboxd stopped; VMs stay alive for the next reconcile")
}

func startMesh(cfg *config.Config, mgr *pool.Manager) (*mesh.Mesh, error) {
	mc := cfg.Mesh
	mlCfg := memberlist.DefaultLANConfig()
	host, portStr, err := net.SplitHostPort(mc.Bind)
	if err != nil {
		return nil, fmt.Errorf("mesh bind %q: %w", mc.Bind, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("mesh bind port %q: %w", portStr, err)
	}
	if host == "" {
		return nil, fmt.Errorf("mesh bind needs an explicit host (got %q); a wildcard advertises an unroutable address", mc.Bind)
	}
	mlCfg.BindAddr, mlCfg.BindPort = host, port
	mlCfg.AdvertiseAddr, mlCfg.AdvertisePort = host, port
	mlCfg.PushPullInterval = gossipInterval
	mlCfg.LogOutput = io.Discard // memberlist's own logs bypass core/log

	nodeID := cmp.Or(mc.NodeID, mc.Bind)
	var key []byte
	if mc.ClusterKey != "" {
		if key, err = base64.StdEncoding.DecodeString(mc.ClusterKey); err != nil {
			return nil, fmt.Errorf("mesh cluster key: %w", err)
		}
	}
	msh, err := mesh.New(mlCfg, nodeID, cfg.AdvertiseAddr, key, cfg.DataDir)
	if err != nil {
		return nil, err
	}
	// Publish the config digest before Join, so the first gossip carries it.
	msh.SetSelfDigest(cfg.ClusterDigest(mgr.EgressCAFingerprint()))
	if err := msh.Join(mc.Join); err != nil {
		_ = msh.Shutdown()
		return nil, err
	}
	return msh, nil
}

// gossipNodeState republishes this node's warm-pool counts and template set
// every tick so the mesh's placement view tracks refill and promotes.
func gossipNodeState(ctx context.Context, msh *mesh.Mesh, mgr *pool.Manager) {
	t := time.NewTicker(gossipInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			msh.UpdateSelf(mgr.WarmCounts(), mgr.TemplateHashes())
		}
	}
}
