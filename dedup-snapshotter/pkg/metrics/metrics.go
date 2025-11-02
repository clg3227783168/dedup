package metrics

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Metrics struct {
	mu              sync.RWMutex
	startTime       time.Time
	snapshotCount   int64
	imageCount      int64
	totalChunks     int64
	uniqueChunks    int64
	dedupRatio      float64
	memoryDeduped   int64
	lazyLoadHits    int64
	lazyLoadMisses  int64
	mountCount      int64
	unmountCount    int64
	buildTime       time.Duration
	mountTime       time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

func (m *Metrics) IncSnapshotCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotCount++
}

func (m *Metrics) IncImageCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.imageCount++
}

func (m *Metrics) UpdateChunkStats(total, unique int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalChunks = total
	m.uniqueChunks = unique
	if total > 0 {
		m.dedupRatio = float64(total-unique) / float64(total) * 100
	}
}

func (m *Metrics) UpdateMemoryDeduped(bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.memoryDeduped = bytes
}

func (m *Metrics) IncLazyLoadHit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lazyLoadHits++
}

func (m *Metrics) IncLazyLoadMiss() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lazyLoadMisses++
}

func (m *Metrics) IncMountCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mountCount++
}

func (m *Metrics) IncUnmountCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unmountCount++
}

func (m *Metrics) AddBuildTime(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buildTime += duration
}

func (m *Metrics) AddMountTime(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mountTime += duration
}

func (m *Metrics) GetSnapshot() *MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	uptime := time.Since(m.startTime)
	cacheHitRate := 0.0
	if m.lazyLoadHits+m.lazyLoadMisses > 0 {
		cacheHitRate = float64(m.lazyLoadHits) / float64(m.lazyLoadHits+m.lazyLoadMisses) * 100
	}

	return &MetricsSnapshot{
		Uptime:         uptime,
		SnapshotCount:  m.snapshotCount,
		ImageCount:     m.imageCount,
		TotalChunks:    m.totalChunks,
		UniqueChunks:   m.uniqueChunks,
		DedupRatio:     m.dedupRatio,
		MemoryDeduped:  m.memoryDeduped,
		LazyLoadHits:   m.lazyLoadHits,
		LazyLoadMisses: m.lazyLoadMisses,
		CacheHitRate:   cacheHitRate,
		MountCount:     m.mountCount,
		UnmountCount:   m.unmountCount,
		AvgBuildTime:   m.avgBuildTime(),
		AvgMountTime:   m.avgMountTime(),
	}
}

func (m *Metrics) avgBuildTime() time.Duration {
	if m.imageCount == 0 {
		return 0
	}
	return m.buildTime / time.Duration(m.imageCount)
}

func (m *Metrics) avgMountTime() time.Duration {
	if m.mountCount == 0 {
		return 0
	}
	return m.mountTime / time.Duration(m.mountCount)
}

func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startTime = time.Now()
	m.snapshotCount = 0
	m.imageCount = 0
	m.totalChunks = 0
	m.uniqueChunks = 0
	m.dedupRatio = 0
	m.memoryDeduped = 0
	m.lazyLoadHits = 0
	m.lazyLoadMisses = 0
	m.mountCount = 0
	m.unmountCount = 0
	m.buildTime = 0
	m.mountTime = 0
}

type MetricsSnapshot struct {
	Uptime         time.Duration `json:"uptime"`
	SnapshotCount  int64         `json:"snapshot_count"`
	ImageCount     int64         `json:"image_count"`
	TotalChunks    int64         `json:"total_chunks"`
	UniqueChunks   int64         `json:"unique_chunks"`
	DedupRatio     float64       `json:"dedup_ratio"`
	MemoryDeduped  int64         `json:"memory_deduped_bytes"`
	LazyLoadHits   int64         `json:"lazy_load_hits"`
	LazyLoadMisses int64         `json:"lazy_load_misses"`
	CacheHitRate   float64       `json:"cache_hit_rate"`
	MountCount     int64         `json:"mount_count"`
	UnmountCount   int64         `json:"unmount_count"`
	AvgBuildTime   time.Duration `json:"avg_build_time"`
	AvgMountTime   time.Duration `json:"avg_mount_time"`
}

func (s *MetricsSnapshot) String() string {
	return fmt.Sprintf(`Metrics:
  Uptime: %v
  Snapshots: %d
  Images: %d
  Total Chunks: %d
  Unique Chunks: %d
  Dedup Ratio: %.2f%%
  Memory Deduped: %s
  Lazy Load Hits: %d
  Lazy Load Misses: %d
  Cache Hit Rate: %.2f%%
  Mounts: %d
  Unmounts: %d
  Avg Build Time: %v
  Avg Mount Time: %v`,
		s.Uptime,
		s.SnapshotCount,
		s.ImageCount,
		s.TotalChunks,
		s.UniqueChunks,
		s.DedupRatio,
		formatBytes(s.MemoryDeduped),
		s.LazyLoadHits,
		s.LazyLoadMisses,
		s.CacheHitRate,
		s.MountCount,
		s.UnmountCount,
		s.AvgBuildTime,
		s.AvgMountTime,
	)
}

func (s *MetricsSnapshot) JSON() (string, error) {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
