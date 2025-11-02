package config

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/containerd/log"
	"github.com/fsnotify/fsnotify"
)

type ConfigWatcher struct {
	path        string
	config      *Config
	watcher     *fsnotify.Watcher
	callbacks   []ConfigCallback
	mu          sync.RWMutex
	lastModTime time.Time
}

type ConfigCallback func(oldConfig, newConfig *Config) error

func NewConfigWatcher(configPath string, initialConfig *Config) (*ConfigWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(configPath)
	if err != nil {
		return nil, err
	}

	cw := &ConfigWatcher{
		path:        configPath,
		config:      initialConfig,
		watcher:     watcher,
		lastModTime: stat.ModTime(),
	}

	if err := watcher.Add(configPath); err != nil {
		watcher.Close()
		return nil, err
	}

	return cw, nil
}

func (cw *ConfigWatcher) AddCallback(callback ConfigCallback) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.callbacks = append(cw.callbacks, callback)
}

func (cw *ConfigWatcher) Start(ctx context.Context) {
	go cw.watchLoop(ctx)
}

func (cw *ConfigWatcher) Stop() error {
	return cw.watcher.Close()
}

func (cw *ConfigWatcher) GetConfig() *Config {
	cw.mu.RLock()
	defer cw.mu.RUnlock()
	return cw.config
}

func (cw *ConfigWatcher) watchLoop(ctx context.Context) {
	debounceTimer := time.NewTimer(0)
	if !debounceTimer.Stop() {
		<-debounceTimer.C
	}

	for {
		select {
		case event, ok := <-cw.watcher.Events:
			if !ok {
				return
			}

			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				stat, err := os.Stat(cw.path)
				if err != nil {
					log.L.WithError(err).Warn("failed to stat config file")
					continue
				}

				if stat.ModTime().After(cw.lastModTime) {
					cw.lastModTime = stat.ModTime()
					debounceTimer.Reset(100 * time.Millisecond)
				}
			}

		case <-debounceTimer.C:
			cw.reloadConfig()

		case err, ok := <-cw.watcher.Errors:
			if !ok {
				return
			}
			log.L.WithError(err).Error("config watcher error")

		case <-ctx.Done():
			return
		}
	}
}

func (cw *ConfigWatcher) reloadConfig() {
	log.L.Info("config file changed, reloading...")

	newConfig, err := LoadConfig(cw.path)
	if err != nil {
		log.L.WithError(err).Error("failed to reload config")
		return
	}

	cw.mu.Lock()
	oldConfig := cw.config
	cw.config = newConfig
	callbacks := append([]ConfigCallback{}, cw.callbacks...)
	cw.mu.Unlock()

	for _, callback := range callbacks {
		if err := callback(oldConfig, newConfig); err != nil {
			log.L.WithError(err).Error("config callback failed")
		}
	}

	log.L.Info("config reloaded successfully")
}

func (cw *ConfigWatcher) UpdateConfig(newConfig *Config) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if err := newConfig.Save(cw.path); err != nil {
		return err
	}

	oldConfig := cw.config
	cw.config = newConfig

	for _, callback := range cw.callbacks {
		if err := callback(oldConfig, newConfig); err != nil {
			log.L.WithError(err).Error("config callback failed")
		}
	}

	return nil
}