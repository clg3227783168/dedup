package fscache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/containerd/log"
)

type DedupDaemon struct {
	backend       *Backend
	root          string
	registry      string
	client        *http.Client
	prefetcher    *Prefetcher
	downloadQueue chan *DownloadTask
	workers       int
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	images        map[string]*ImageInfo
}

type ImageInfo struct {
	ImageID   string
	Volume    *Volume
	Manifest  *ImageManifest
	mu        sync.RWMutex
}

type ImageManifest struct {
	Layers    []*LayerInfo
	TotalSize int64
}

type LayerInfo struct {
	Digest      string
	Size        int64
	Offset      int64
	ChunkHashes []string
}

type DownloadTask struct {
	ImageID     string
	LayerDigest string
	ChunkHash   string
	Offset      int64
	Size        int64
	Priority    int
	Volume      *Volume
}

func NewDedupDaemon(root, registry string, workers int) (*DedupDaemon, error) {
	backend, err := NewBackend(root)
	if err != nil {
		return nil, fmt.Errorf("failed to create fscache backend: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	daemon := &DedupDaemon{
		backend:       backend,
		root:          root,
		registry:      registry,
		client:        &http.Client{Timeout: 30 * time.Second},
		downloadQueue: make(chan *DownloadTask, 10000),
		workers:       workers,
		ctx:           ctx,
		cancel:        cancel,
		images:        make(map[string]*ImageInfo),
	}

	prefetcher, err := NewPrefetcher(daemon)
	if err != nil {
		backend.Close()
		return nil, err
	}
	daemon.prefetcher = prefetcher

	daemon.startWorkers()

	log.L.Infof("dedupd daemon started with %d workers", workers)
	return daemon, nil
}

func (d *DedupDaemon) startWorkers() {
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go d.downloadWorker(i)
	}
}

func (d *DedupDaemon) downloadWorker(id int) {
	defer d.wg.Done()

	log.L.Infof("download worker %d started", id)

	for {
		select {
		case <-d.ctx.Done():
			log.L.Infof("download worker %d stopped", id)
			return

		case task := <-d.downloadQueue:
			if task == nil {
				return
			}

			if err := d.processDownloadTask(task); err != nil {
				log.L.WithError(err).Warnf("worker %d failed to process task: %s", id, task.ChunkHash)
			} else {
				log.L.Debugf("worker %d completed task: %s", id, task.ChunkHash)
			}
		}
	}
}

func (d *DedupDaemon) processDownloadTask(task *DownloadTask) error {
	obj, exists := task.Volume.GetObject(task.ChunkHash)
	if exists && obj.Complete {
		log.L.Debugf("chunk already cached: %s", task.ChunkHash)
		return nil
	}

	if !exists {
		var err error
		obj, err = task.Volume.CreateObject(d.ctx, task.ChunkHash, task.Size)
		if err != nil {
			return fmt.Errorf("failed to create cache object: %w", err)
		}
	}

	data, err := d.fetchChunkData(task.ImageID, task.LayerDigest, task.Offset, task.Size)
	if err != nil {
		return fmt.Errorf("failed to fetch chunk: %w", err)
	}

	if _, err := obj.Write(0, data); err != nil {
		return fmt.Errorf("failed to write to cache: %w", err)
	}

	if err := obj.MarkComplete(); err != nil {
		return fmt.Errorf("failed to mark complete: %w", err)
	}

	log.L.Debugf("downloaded and cached chunk: %s (size=%d)", task.ChunkHash, len(data))
	return nil
}

func (d *DedupDaemon) fetchChunkData(imageID, layerDigest string, offset, size int64) ([]byte, error) {
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", d.registry, imageID, layerDigest)

	req, err := http.NewRequestWithContext(d.ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
	req.Header.Set("Range", rangeHeader)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return data, nil
}

func (d *DedupDaemon) RegisterImage(ctx context.Context, imageID string, manifestPath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.images[imageID]; exists {
		return nil
	}

	volume, err := d.backend.CreateVolume(ctx, imageID)
	if err != nil {
		return fmt.Errorf("failed to create volume for image: %w", err)
	}

	manifest, err := d.loadManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	imageInfo := &ImageInfo{
		ImageID:  imageID,
		Volume:   volume,
		Manifest: manifest,
	}

	d.images[imageID] = imageInfo

	log.L.Infof("registered image %s with %d layers", imageID, len(manifest.Layers))
	return nil
}

func (d *DedupDaemon) loadManifest(manifestPath string) (*ImageManifest, error) {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}

	manifest := &ImageManifest{
		Layers: make([]*LayerInfo, 0),
	}

	var currentOffset int64
	lines := string(data)
	for _, line := range splitLines(lines) {
		if line == "" {
			continue
		}

		layer := &LayerInfo{
			Digest: line,
			Offset: currentOffset,
			Size:   4 * 1024 * 1024,
		}
		manifest.Layers = append(manifest.Layers, layer)
		currentOffset += layer.Size
	}

	manifest.TotalSize = currentOffset
	return manifest, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func (d *DedupDaemon) StartPrefetch(ctx context.Context, imageID string, traceFile string) error {
	d.mu.RLock()
	imageInfo, exists := d.images[imageID]
	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf("image not registered: %s", imageID)
	}

	return d.prefetcher.StartPrefetch(ctx, imageInfo, traceFile)
}

func (d *DedupDaemon) EnqueueDownload(task *DownloadTask) {
	select {
	case d.downloadQueue <- task:
	case <-d.ctx.Done():
		return
	default:
		log.L.Warnf("download queue full, dropping task: %s", task.ChunkHash)
	}
}

func (d *DedupDaemon) GetImageVolume(imageID string) (*Volume, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	imageInfo, exists := d.images[imageID]
	if !exists {
		return nil, fmt.Errorf("image not found: %s", imageID)
	}

	return imageInfo.Volume, nil
}

func (d *DedupDaemon) ComputeChunkHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (d *DedupDaemon) GetCachePath(imageID, chunkHash string) (string, error) {
	volume, err := d.GetImageVolume(imageID)
	if err != nil {
		return "", err
	}

	cachePath := filepath.Join(volume.Path, chunkHash)
	return cachePath, nil
}

func (d *DedupDaemon) GetStats() *DaemonStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := &DaemonStats{
		Images:       len(d.images),
		QueueDepth:   len(d.downloadQueue),
		BackendStats: d.backend.GetStats(),
	}

	return stats
}

func (d *DedupDaemon) Shutdown(ctx context.Context) error {
	log.L.Info("shutting down dedupd daemon")

	d.cancel()

	d.wg.Wait()

	close(d.downloadQueue)

	if d.prefetcher != nil {
		d.prefetcher.Stop()
	}

	if d.backend != nil {
		return d.backend.Close()
	}

	return nil
}

type DaemonStats struct {
	Images       int
	QueueDepth   int
	BackendStats *BackendStats
}
