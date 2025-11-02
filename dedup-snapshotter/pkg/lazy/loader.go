package lazy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/containerd/log"
)

type LazyLoader struct {
	root          string
	cacheDir      string
	registry      string
	prefetchQueue *PrefetchQueue
	client        *http.Client
	mu            sync.RWMutex
	loadedChunks  map[string]bool
}

type PrefetchQueue struct {
	queue   chan *PrefetchTask
	workers int
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

type PrefetchTask struct {
	ChunkHash string
	Priority  int
	ImageID   string
}

func NewLazyLoader(root, registry string) (*LazyLoader, error) {
	cacheDir := filepath.Join(root, "lazy-cache")
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	prefetchQueue := &PrefetchQueue{
		queue:   make(chan *PrefetchTask, 1000),
		workers: 4,
		ctx:     ctx,
		cancel:  cancel,
	}

	loader := &LazyLoader{
		root:          root,
		cacheDir:      cacheDir,
		registry:      registry,
		prefetchQueue: prefetchQueue,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		loadedChunks: make(map[string]bool),
	}

	loader.startPrefetchWorkers()
	return loader, nil
}

func (l *LazyLoader) startPrefetchWorkers() {
	for i := 0; i < l.prefetchQueue.workers; i++ {
		l.prefetchQueue.wg.Add(1)
		go l.prefetchWorker()
	}
}

func (l *LazyLoader) prefetchWorker() {
	defer l.prefetchQueue.wg.Done()

	for {
		select {
		case <-l.prefetchQueue.ctx.Done():
			return
		case task := <-l.prefetchQueue.queue:
			if task == nil {
				return
			}

			if err := l.loadChunk(l.prefetchQueue.ctx, task.ChunkHash, task.ImageID); err != nil {
				log.L.WithError(err).Warnf("prefetch failed for chunk %s", task.ChunkHash)
			} else {
				log.L.Debugf("prefetched chunk %s for image %s", task.ChunkHash, task.ImageID)
			}
		}
	}
}

func (l *LazyLoader) LoadChunk(ctx context.Context, chunkHash, imageID string) (string, error) {
	chunkPath := filepath.Join(l.cacheDir, chunkHash)

	l.mu.RLock()
	if l.loadedChunks[chunkHash] {
		l.mu.RUnlock()
		if _, err := os.Stat(chunkPath); err == nil {
			return chunkPath, nil
		}
	}
	l.mu.RUnlock()

	return chunkPath, l.loadChunk(ctx, chunkHash, imageID)
}

func (l *LazyLoader) loadChunk(ctx context.Context, chunkHash, imageID string) error {
	chunkPath := filepath.Join(l.cacheDir, chunkHash)

	if _, err := os.Stat(chunkPath); err == nil {
		l.mu.Lock()
		l.loadedChunks[chunkHash] = true
		l.mu.Unlock()
		return nil
	}

	tmpPath := chunkPath + ".tmp"
	if err := l.fetchChunk(ctx, chunkHash, imageID, tmpPath); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, chunkPath); err != nil {
		os.Remove(tmpPath)
		return err
	}

	l.mu.Lock()
	l.loadedChunks[chunkHash] = true
	l.mu.Unlock()

	return nil
}

func (l *LazyLoader) fetchChunk(ctx context.Context, chunkHash, imageID, targetPath string) error {
	url := fmt.Sprintf("%s/v2/%s/blobs/sha256:%s", l.registry, imageID, chunkHash)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := l.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch chunk: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	file, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return err
	}

	return nil
}

func (l *LazyLoader) Prefetch(imageID string, chunkHashes []string) {
	for i, hash := range chunkHashes {
		task := &PrefetchTask{
			ChunkHash: hash,
			Priority:  i,
			ImageID:   imageID,
		}

		select {
		case l.prefetchQueue.queue <- task:
		default:
			log.L.Warnf("prefetch queue full, skipping chunk %s", hash)
		}
	}
}

func (l *LazyLoader) PrefetchWithTrace(imageID string, traceFile string) error {
	traces, err := l.loadTraceFile(traceFile)
	if err != nil {
		return err
	}

	log.L.Infof("loaded %d trace entries for prefetch", len(traces))

	for _, trace := range traces {
		l.Prefetch(imageID, []string{trace.ChunkHash})
	}

	return nil
}

func (l *LazyLoader) loadTraceFile(traceFile string) ([]*TraceEntry, error) {
	data, err := os.ReadFile(traceFile)
	if err != nil {
		return nil, err
	}

	var traces []*TraceEntry
	lines := string(data)
	for _, line := range splitLines(lines) {
		if line == "" {
			continue
		}
		traces = append(traces, &TraceEntry{ChunkHash: line})
	}

	return traces, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func (l *LazyLoader) GetCacheStats() *CacheStats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var totalSize int64
	var fileCount int64

	filepath.Walk(l.cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})

	return &CacheStats{
		LoadedChunks: int64(len(l.loadedChunks)),
		CachedFiles:  fileCount,
		TotalSize:    totalSize,
	}
}

func (l *LazyLoader) Cleanup() error {
	l.prefetchQueue.cancel()
	l.prefetchQueue.wg.Wait()
	close(l.prefetchQueue.queue)
	return nil
}

type TraceEntry struct {
	ChunkHash string
	Timestamp int64
}

type CacheStats struct {
	LoadedChunks int64
	CachedFiles  int64
	TotalSize    int64
}
