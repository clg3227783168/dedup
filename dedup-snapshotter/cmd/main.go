package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	"github.com/containerd/log"
	"github.com/opencloudos/dedup-snapshotter/pkg/api"
	"github.com/opencloudos/dedup-snapshotter/pkg/audit"
	"github.com/opencloudos/dedup-snapshotter/pkg/config"
	"github.com/opencloudos/dedup-snapshotter/pkg/metrics"
	"github.com/opencloudos/dedup-snapshotter/pkg/snapshotter"
	"google.golang.org/grpc"
)

const (
	defaultAddress    = "/run/containerd/dedup-snapshotter.sock"
	defaultRoot       = "/var/lib/containerd/io.containerd.snapshotter.v1.dedup"
	defaultConfigPath = "/etc/dedup-snapshotter/config.json"
	defaultAPIAddress = ":8080"
)

var globalMetrics = metrics.NewMetrics()

func main() {
	if err := run(); err != nil {
		log.L.WithError(err).Fatal("failed to run snapshotter")
	}
}

func run() error {
	address := os.Getenv("ADDRESS")
	if address == "" {
		address = defaultAddress
	}

	root := os.Getenv("ROOT")
	if root == "" {
		root = defaultRoot
	}

	configPath := os.Getenv("CONFIG")
	if configPath == "" {
		configPath = defaultConfigPath
	}

	apiAddress := os.Getenv("API_ADDRESS")
	if apiAddress == "" {
		apiAddress = defaultAPIAddress
	}

	cfg := config.DefaultConfig(root)
	if _, err := os.Stat(configPath); err == nil {
		loadedCfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.L.WithError(err).Warnf("failed to load config from %s, using defaults", configPath)
		} else {
			cfg = loadedCfg
			log.L.Infof("loaded config from %s", configPath)
		}
	} else {
		log.L.Infof("no config file found at %s, using defaults", configPath)
	}

	auditLogger, err := audit.NewAuditLogger(filepath.Join(root, "audit.db"))
	if err != nil {
		return fmt.Errorf("failed to create audit logger: %w", err)
	}
	defer auditLogger.Close()

	configWatcher, err := config.NewConfigWatcher(configPath, cfg)
	if err != nil {
		log.L.WithError(err).Warn("failed to create config watcher")
	} else {
		ctx := context.Background()
		configWatcher.Start(ctx)
		defer configWatcher.Stop()

		configWatcher.AddCallback(func(oldConfig, newConfig *config.Config) error {
			log.L.Info("config updated via file watcher")

			ctx := audit.StartAudit(context.Background(), "config_reload", "config", "system", os.Getpid(), nil)
			audit.FinishAudit(ctx, auditLogger, "success", nil)

			if err := newConfig.ApplyKSMSettings(); err != nil {
				log.L.WithError(err).Warn("failed to apply new KSM settings")
			}
			return nil
		})
	}

	if err := setupLogging(cfg.LogLevel); err != nil {
		return fmt.Errorf("failed to setup logging: %w", err)
	}

	if cfg.KSM.Enabled {
		if err := cfg.ApplyKSMSettings(); err != nil {
			log.L.WithError(err).Warn("failed to apply KSM settings")
		}
	}

	if err := os.RemoveAll(address); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove socket: %w", err)
	}

	if err := os.MkdirAll(root, 0700); err != nil {
		return fmt.Errorf("failed to create root directory: %w", err)
	}

	log.L.Infof("starting dedup-snapshotter with config: %s", cfg)

	sn, err := snapshotter.NewSnapshotterWithAudit(root, auditLogger)
	if err != nil {
		return fmt.Errorf("failed to create snapshotter: %w", err)
	}

	go startMetricsReporter()
	go startAuditCleanup(auditLogger)

	apiServer := api.NewAPIServer(apiAddress, auditLogger, cfg, configPath)
	go func() {
		if err := apiServer.Start(); err != nil {
			log.L.WithError(err).Error("API server failed")
		}
	}()

	rpc := grpc.NewServer()
	service := snapshotservice.FromSnapshotter(sn)
	snapshotsapi.RegisterSnapshotsServer(rpc, service)

	l, err := net.Listen("unix", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	log.L.Infof("snapshotter listening on %s", address)
	log.L.Infof("erofs-based dedup snapshotter started successfully")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- rpc.Serve(l)
	}()

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		log.L.Info("received signal, shutting down")
		printMetrics()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		go func() {
			if err := apiServer.Stop(ctx); err != nil {
				log.L.WithError(err).Error("failed to stop API server")
			}
		}()

		rpc.GracefulStop()
	}

	return nil
}

func startAuditCleanup(auditLogger *audit.AuditLogger) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		ctx := context.Background()
		if err := auditLogger.Cleanup(ctx, 30); err != nil {
			log.L.WithError(err).Error("failed to cleanup audit logs")
		}
	}
}

func setupLogging(level string) error {
	switch level {
	case "debug":
		log.L.Logger.SetLevel(log.DebugLevel)
	case "info":
		log.L.Logger.SetLevel(log.InfoLevel)
	case "warn":
		log.L.Logger.SetLevel(log.WarnLevel)
	case "error":
		log.L.Logger.SetLevel(log.ErrorLevel)
	default:
		log.L.Logger.SetLevel(log.InfoLevel)
	}
	return nil
}

func startMetricsReporter() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		printMetrics()
	}
}

func printMetrics() {
	snapshot := globalMetrics.GetSnapshot()
	log.L.Infof("\n%s", snapshot.String())
}
