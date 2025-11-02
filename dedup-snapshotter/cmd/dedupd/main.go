package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/containerd/log"
	"github.com/opencloudos/dedup-snapshotter/pkg/fscache"
)

var (
	rootDir     = flag.String("root", "/var/lib/dedup-snapshotter", "root directory for dedup snapshotter")
	registry    = flag.String("registry", "https://registry-1.docker.io", "container registry URL")
	workers     = flag.Int("workers", 4, "number of download workers")
	logLevel    = flag.String("log-level", "info", "log level (debug, info, warn, error)")
	showStats   = flag.Bool("stats", false, "show stats and exit")
	showVersion = flag.Bool("version", false, "show version and exit")
)

const (
	version = "1.0.0"
)

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("dedupd version %s\n", version)
		os.Exit(0)
	}

	setupLogging(*logLevel)

	log.L.Infof("starting dedupd daemon (version=%s)", version)
	log.L.Infof("config: root=%s, registry=%s, workers=%d", *rootDir, *registry, *workers)

	daemon, err := fscache.NewDedupDaemon(*rootDir, *registry, *workers)
	if err != nil {
		log.L.Fatalf("failed to create dedupd daemon: %v", err)
	}

	if *showStats {
		printStats(daemon)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.L.Info("received shutdown signal")
		cancel()
	}()

	go statsReporter(ctx, daemon)

	log.L.Info("dedupd daemon is running")

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := daemon.Shutdown(shutdownCtx); err != nil {
		log.L.Errorf("error during shutdown: %v", err)
		os.Exit(1)
	}

	log.L.Info("dedupd daemon stopped")
}

func setupLogging(level string) {
	var logrusLevel log.Level
	switch level {
	case "debug":
		logrusLevel = log.DebugLevel
	case "info":
		logrusLevel = log.InfoLevel
	case "warn":
		logrusLevel = log.WarnLevel
	case "error":
		logrusLevel = log.ErrorLevel
	default:
		logrusLevel = log.InfoLevel
	}

	log.L.Logger.SetLevel(logrusLevel)
}

func printStats(daemon *fscache.DedupDaemon) {
	stats := daemon.GetStats()

	fmt.Println("=== Dedupd Daemon Statistics ===")
	fmt.Printf("Registered Images: %d\n", stats.Images)
	fmt.Printf("Download Queue Depth: %d\n", stats.QueueDepth)

	if stats.BackendStats != nil {
		fmt.Println("\n=== Fscache Backend Statistics ===")
		fmt.Printf("Volumes: %d\n", stats.BackendStats.Volumes)
		fmt.Printf("Objects: %d\n", stats.BackendStats.Objects)
		fmt.Printf("Complete Objects: %d\n", stats.BackendStats.CompleteObjects)
		fmt.Printf("Total Size: %d bytes (%.2f MB)\n",
			stats.BackendStats.TotalSize,
			float64(stats.BackendStats.TotalSize)/(1024*1024))
	}
}

func statsReporter(ctx context.Context, daemon *fscache.DedupDaemon) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := daemon.GetStats()
			log.L.Infof("stats: images=%d, queue_depth=%d, objects=%d, complete=%d",
				stats.Images,
				stats.QueueDepth,
				stats.BackendStats.Objects,
				stats.BackendStats.CompleteObjects)
		}
	}
}
