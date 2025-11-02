package fscache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/containerd/log"
)

const (
	FsCachePath     = "/sys/fs/fscache"
	CachefilesPath  = "/dev/cachefiles"
	CacheDirDefault = "/var/cache/fscache"
)

type Backend struct {
	root      string
	cacheDir  string
	volumeDir string
	fd        int
	mu        sync.RWMutex
	volumes   map[string]*Volume
}

type Volume struct {
	Name      string
	Path      string
	CookieFd  int
	Objects   map[string]*CacheObject
	mu        sync.RWMutex
}

type CacheObject struct {
	Key       string
	Size      int64
	Fd        int
	Complete  bool
	mu        sync.Mutex
}

func NewBackend(root string) (*Backend, error) {
	cacheDir := filepath.Join(root, "fscache")
	volumeDir := filepath.Join(cacheDir, "volumes")

	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create cache dir: %w", err)
	}
	if err := os.MkdirAll(volumeDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create volume dir: %w", err)
	}

	if err := ensureFsCacheAvailable(); err != nil {
		return nil, err
	}

	fd, err := syscall.Open(CachefilesPath, syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open cachefiles device: %w", err)
	}

	backend := &Backend{
		root:      root,
		cacheDir:  cacheDir,
		volumeDir: volumeDir,
		fd:        fd,
		volumes:   make(map[string]*Volume),
	}

	if err := backend.bindCache(); err != nil {
		syscall.Close(fd)
		return nil, err
	}

	log.L.Info("fscache backend initialized")
	return backend, nil
}

func ensureFsCacheAvailable() error {
	if _, err := os.Stat(FsCachePath); os.IsNotExist(err) {
		return fmt.Errorf("fscache not available in kernel")
	}

	if _, err := os.Stat(CachefilesPath); os.IsNotExist(err) {
		if err := loadCachefilesModule(); err != nil {
			return fmt.Errorf("cachefiles module not loaded: %w", err)
		}
	}

	return nil
}

func loadCachefilesModule() error {
	cmd := fmt.Sprintf("modprobe cachefiles")
	_, err := os.ReadFile("/proc/modules")
	if err != nil {
		return err
	}

	file, err := os.OpenFile("/proc/sys/kernel/modules_disabled", os.O_RDONLY, 0)
	if err == nil {
		defer file.Close()
	}

	log.L.Info("cachefiles module may need to be loaded manually: modprobe cachefiles")
	return fmt.Errorf("please load cachefiles module: %s", cmd)
}

func (b *Backend) bindCache() error {
	bindCmd := fmt.Sprintf("bind %s", b.cacheDir)
	_, err := syscall.Write(b.fd, []byte(bindCmd))
	if err != nil {
		return fmt.Errorf("failed to bind cache: %w", err)
	}

	log.L.Infof("bound fscache to %s", b.cacheDir)
	return nil
}

func (b *Backend) CreateVolume(ctx context.Context, volumeName string) (*Volume, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if vol, exists := b.volumes[volumeName]; exists {
		return vol, nil
	}

	volumePath := filepath.Join(b.volumeDir, volumeName)
	if err := os.MkdirAll(volumePath, 0700); err != nil {
		return nil, err
	}

	cookieCmd := fmt.Sprintf("volume %s", volumeName)
	cookieFd, err := syscall.Open(CachefilesPath, syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open volume fd: %w", err)
	}

	_, err = syscall.Write(cookieFd, []byte(cookieCmd))
	if err != nil {
		syscall.Close(cookieFd)
		return nil, fmt.Errorf("failed to create volume: %w", err)
	}

	volume := &Volume{
		Name:     volumeName,
		Path:     volumePath,
		CookieFd: cookieFd,
		Objects:  make(map[string]*CacheObject),
	}

	b.volumes[volumeName] = volume
	log.L.Infof("created fscache volume: %s", volumeName)

	return volume, nil
}

func (b *Backend) GetVolume(volumeName string) (*Volume, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	vol, exists := b.volumes[volumeName]
	if !exists {
		return nil, fmt.Errorf("volume not found: %s", volumeName)
	}
	return vol, nil
}

func (v *Volume) CreateObject(ctx context.Context, key string, size int64) (*CacheObject, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if obj, exists := v.Objects[key]; exists {
		return obj, nil
	}

	objectCmd := fmt.Sprintf("open %s", key)
	objFd, err := syscall.Open(CachefilesPath, syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open object fd: %w", err)
	}

	_, err = syscall.Write(objFd, []byte(objectCmd))
	if err != nil {
		syscall.Close(objFd)
		return nil, fmt.Errorf("failed to create cache object: %w", err)
	}

	obj := &CacheObject{
		Key:      key,
		Size:     size,
		Fd:       objFd,
		Complete: false,
	}

	v.Objects[key] = obj
	log.L.Debugf("created cache object: %s (size=%d)", key, size)

	return obj, nil
}

func (v *Volume) GetObject(key string) (*CacheObject, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	obj, exists := v.Objects[key]
	return obj, exists
}

func (o *CacheObject) Write(offset int64, data []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	n, err := syscall.Pwrite(o.Fd, data, offset)
	if err != nil {
		return 0, fmt.Errorf("failed to write to cache object: %w", err)
	}

	return n, nil
}

func (o *CacheObject) MarkComplete() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.Complete {
		return nil
	}

	completeCmd := []byte("complete")
	_, err := syscall.Write(o.Fd, completeCmd)
	if err != nil {
		return fmt.Errorf("failed to mark object complete: %w", err)
	}

	o.Complete = true
	log.L.Debugf("marked cache object complete: %s", o.Key)
	return nil
}

func (o *CacheObject) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.Fd > 0 {
		return syscall.Close(o.Fd)
	}
	return nil
}

func (v *Volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	for _, obj := range v.Objects {
		obj.Close()
	}

	if v.CookieFd > 0 {
		return syscall.Close(v.CookieFd)
	}
	return nil
}

func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, vol := range b.volumes {
		vol.Close()
	}

	if b.fd > 0 {
		return syscall.Close(b.fd)
	}
	return nil
}

func (b *Backend) GetStats() *BackendStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := &BackendStats{
		Volumes: len(b.volumes),
	}

	for _, vol := range b.volumes {
		vol.mu.RLock()
		stats.Objects += len(vol.Objects)
		for _, obj := range vol.Objects {
			stats.TotalSize += obj.Size
			if obj.Complete {
				stats.CompleteObjects++
			}
		}
		vol.mu.RUnlock()
	}

	return stats
}

type BackendStats struct {
	Volumes         int
	Objects         int
	CompleteObjects int
	TotalSize       int64
}
