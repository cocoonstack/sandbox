// sandboxd is the per-node sandbox control plane: it keeps warm pools of
// claim-ready microVMs, serves claims over HTTP, and relays the silkd data
// plane between clients and guests.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/projecteru2/core/log"
	coretypes "github.com/projecteru2/core/types"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/pool"
	"github.com/cocoonstack/sandbox/sandboxd/server"
)

const (
	shutdownGrace = 5 * time.Second
	// Slowloris protection; ReadTimeout/WriteTimeout must stay zero — cold
	// claims block up to the cold probe timeout and relays stream forever.
	readHeaderTimeout = 5 * time.Second
)

func main() {
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
	mgr, err := pool.NewManager(cfg, eng)
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

	srv := server.New(cfg.APIToken, mgr, eng)
	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
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
	logger.Info(ctx, "sandboxd stopped; VMs stay alive for the next reconcile")
}
