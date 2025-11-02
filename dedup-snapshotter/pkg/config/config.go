package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/containerd/log"
)

type Config struct {
	Root          string        `json:"root"`
	EnableErofs   bool          `json:"enable_erofs"`
	EnableFscache bool          `json:"enable_fscache"`
	EnableLazy    bool          `json:"enable_lazy"`
	EnableMemDedup bool         `json:"enable_mem_dedup"`
	Registry      string        `json:"registry"`
	ChunkSize     int64         `json:"chunk_size"`
	LogLevel      string        `json:"log_level"`
	Prefetch      PrefetchConfig `json:"prefetch"`
	KSM           KSMConfig     `json:"ksm"`
	Dedupd        DedupdConfig  `json:"dedupd"`
}

type PrefetchConfig struct {
	Enabled     bool   `json:"enabled"`
	Workers     int    `json:"workers"`
	QueueSize   int    `json:"queue_size"`
	TraceDir    string `json:"trace_dir"`
}

type KSMConfig struct {
	Enabled       bool `json:"enabled"`
	ScanInterval  int  `json:"scan_interval"`
	PagesToScan   int  `json:"pages_to_scan"`
	MergeAcrossNodes bool `json:"merge_across_nodes"`
}

type DedupdConfig struct {
	Enabled       bool   `json:"enabled"`
	Workers       int    `json:"workers"`
	Registry      string `json:"registry"`
	FscacheDomain string `json:"fscache_domain"`
}

func DefaultConfig(root string) *Config {
	return &Config{
		Root:          root,
		EnableErofs:   true,
		EnableFscache: true,
		EnableLazy:    true,
		EnableMemDedup: true,
		Registry:      "",
		ChunkSize:     4 * 1024 * 1024,
		LogLevel:      "info",
		Prefetch: PrefetchConfig{
			Enabled:   true,
			Workers:   4,
			QueueSize: 1000,
			TraceDir:  filepath.Join(root, "traces"),
		},
		KSM: KSMConfig{
			Enabled:       true,
			ScanInterval:  100,
			PagesToScan:   100,
			MergeAcrossNodes: false,
		},
		Dedupd: DedupdConfig{
			Enabled:       true,
			Workers:       4,
			Registry:      "https://registry-1.docker.io",
			FscacheDomain: "dedup-snapshotter",
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("config file not found: %s", path)
		}
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Root == "" {
		return fmt.Errorf("root path is required")
	}

	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk_size must be positive")
	}

	if c.Prefetch.Workers <= 0 {
		c.Prefetch.Workers = 4
	}

	if c.Prefetch.QueueSize <= 0 {
		c.Prefetch.QueueSize = 1000
	}

	return nil
}

func (c *Config) Save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func (c *Config) ApplyKSMSettings() error {
	if !c.KSM.Enabled {
		log.L.Info("KSM disabled in config")
		return nil
	}

	ksmPath := "/sys/kernel/mm/ksm"
	if _, err := os.Stat(ksmPath); os.IsNotExist(err) {
		return fmt.Errorf("KSM not available in kernel")
	}

	if err := os.WriteFile(filepath.Join(ksmPath, "run"), []byte("1"), 0644); err != nil {
		return fmt.Errorf("failed to enable KSM: %w", err)
	}

	if c.KSM.ScanInterval > 0 {
		val := fmt.Sprintf("%d", c.KSM.ScanInterval)
		if err := os.WriteFile(filepath.Join(ksmPath, "sleep_millisecs"), []byte(val), 0644); err != nil {
			log.L.Warnf("failed to set KSM scan interval: %v", err)
		}
	}

	if c.KSM.PagesToScan > 0 {
		val := fmt.Sprintf("%d", c.KSM.PagesToScan)
		if err := os.WriteFile(filepath.Join(ksmPath, "pages_to_scan"), []byte(val), 0644); err != nil {
			log.L.Warnf("failed to set KSM pages to scan: %v", err)
		}
	}

	mergeAcrossNodes := "0"
	if c.KSM.MergeAcrossNodes {
		mergeAcrossNodes = "1"
	}
	if err := os.WriteFile(filepath.Join(ksmPath, "merge_across_nodes"), []byte(mergeAcrossNodes), 0644); err != nil {
		log.L.Warnf("failed to set KSM merge_across_nodes: %v", err)
	}

	log.L.Info("KSM settings applied successfully")
	return nil
}

func (c *Config) String() string {
	data, _ := json.MarshalIndent(c, "", "  ")
	return string(data)
}
