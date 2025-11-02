package fscache

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/containerd/log"
)

type Prefetcher struct {
	daemon         *DedupDaemon
	activeJobs     map[string]*PrefetchJob
	mu             sync.RWMutex
	maxConcurrent  int
	predictorCache *PredictorCache
}

type PrefetchJob struct {
	ImageID      string
	ImageInfo    *ImageInfo
	TraceEntries []*TraceEntry
	Index        int
	StartTime    time.Time
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
}

type TraceEntry struct {
	Offset    int64
	Size      int64
	Timestamp int64
	ChunkHash string
}

type PredictorCache struct {
	predictions map[string]*AccessPattern
	mu          sync.RWMutex
}

type AccessPattern struct {
	NextChunks  []string
	Probability float64
	LastUpdate  time.Time
}

func NewPrefetcher(daemon *DedupDaemon) (*Prefetcher, error) {
	return &Prefetcher{
		daemon:        daemon,
		activeJobs:    make(map[string]*PrefetchJob),
		maxConcurrent: 8,
		predictorCache: &PredictorCache{
			predictions: make(map[string]*AccessPattern),
		},
	}, nil
}

func (p *Prefetcher) StartPrefetch(ctx context.Context, imageInfo *ImageInfo, traceFile string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.activeJobs[imageInfo.ImageID]; exists {
		return fmt.Errorf("prefetch already active for image: %s", imageInfo.ImageID)
	}

	traces, err := p.loadTraceFile(traceFile)
	if err != nil {
		return fmt.Errorf("failed to load trace file: %w", err)
	}

	jobCtx, cancel := context.WithCancel(ctx)
	job := &PrefetchJob{
		ImageID:      imageInfo.ImageID,
		ImageInfo:    imageInfo,
		TraceEntries: traces,
		Index:        0,
		StartTime:    time.Now(),
		ctx:          jobCtx,
		cancel:       cancel,
	}

	p.activeJobs[imageInfo.ImageID] = job

	go p.runPrefetchJob(job)

	log.L.Infof("started prefetch for image %s with %d trace entries", imageInfo.ImageID, len(traces))
	return nil
}

func (p *Prefetcher) loadTraceFile(traceFile string) ([]*TraceEntry, error) {
	data, err := os.ReadFile(traceFile)
	if err != nil {
		return nil, err
	}

	var traces []*TraceEntry
	lines := splitLines(string(data))

	var offset int64
	const defaultChunkSize = 4 * 1024 * 1024

	for _, line := range lines {
		if line == "" {
			continue
		}

		entry := &TraceEntry{
			Offset:    offset,
			Size:      defaultChunkSize,
			ChunkHash: line,
			Timestamp: time.Now().UnixNano(),
		}

		traces = append(traces, entry)
		offset += defaultChunkSize
	}

	return traces, nil
}

func (p *Prefetcher) runPrefetchJob(job *PrefetchJob) {
	defer func() {
		p.mu.Lock()
		delete(p.activeJobs, job.ImageID)
		p.mu.Unlock()
		log.L.Infof("prefetch job completed for image %s", job.ImageID)
	}()

	semaphore := make(chan struct{}, p.maxConcurrent)
	var wg sync.WaitGroup

	for i, entry := range job.TraceEntries {
		select {
		case <-job.ctx.Done():
			log.L.Infof("prefetch job cancelled for image %s", job.ImageID)
			wg.Wait()
			return
		case semaphore <- struct{}{}:
		}

		wg.Add(1)
		go func(idx int, trace *TraceEntry) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if err := p.prefetchChunk(job, trace); err != nil {
				log.L.WithError(err).Warnf("failed to prefetch chunk %s", trace.ChunkHash)
			}

			job.mu.Lock()
			job.Index = idx + 1
			job.mu.Unlock()

			p.updatePredictor(trace.ChunkHash, job.TraceEntries, idx)
		}(i, entry)

		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
}

func (p *Prefetcher) prefetchChunk(job *PrefetchJob, trace *TraceEntry) error {
	obj, exists := job.ImageInfo.Volume.GetObject(trace.ChunkHash)
	if exists && obj.Complete {
		log.L.Debugf("chunk already prefetched: %s", trace.ChunkHash)
		return nil
	}

	layerDigest := ""
	if len(job.ImageInfo.Manifest.Layers) > 0 {
		layerDigest = job.ImageInfo.Manifest.Layers[0].Digest
	}

	task := &DownloadTask{
		ImageID:     job.ImageID,
		LayerDigest: layerDigest,
		ChunkHash:   trace.ChunkHash,
		Offset:      trace.Offset,
		Size:        trace.Size,
		Priority:    100,
		Volume:      job.ImageInfo.Volume,
	}

	p.daemon.EnqueueDownload(task)

	return nil
}

func (p *Prefetcher) updatePredictor(currentChunk string, traces []*TraceEntry, currentIdx int) {
	if currentIdx+1 >= len(traces) {
		return
	}

	p.predictorCache.mu.Lock()
	defer p.predictorCache.mu.Unlock()

	nextChunks := make([]string, 0, 5)
	for i := 1; i <= 5 && currentIdx+i < len(traces); i++ {
		nextChunks = append(nextChunks, traces[currentIdx+i].ChunkHash)
	}

	pattern := &AccessPattern{
		NextChunks:  nextChunks,
		Probability: 0.8,
		LastUpdate:  time.Now(),
	}

	p.predictorCache.predictions[currentChunk] = pattern
}

func (p *Prefetcher) PredictNextChunks(currentChunk string) []string {
	p.predictorCache.mu.RLock()
	defer p.predictorCache.mu.RUnlock()

	if pattern, exists := p.predictorCache.predictions[currentChunk]; exists {
		return pattern.NextChunks
	}

	return nil
}

func (p *Prefetcher) StopPrefetch(imageID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	job, exists := p.activeJobs[imageID]
	if !exists {
		return fmt.Errorf("no active prefetch job for image: %s", imageID)
	}

	job.cancel()
	delete(p.activeJobs, imageID)

	log.L.Infof("stopped prefetch for image %s", imageID)
	return nil
}

func (p *Prefetcher) GetJobStatus(imageID string) *PrefetchStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	job, exists := p.activeJobs[imageID]
	if !exists {
		return nil
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	totalEntries := len(job.TraceEntries)
	progress := float64(job.Index) / float64(totalEntries) * 100

	return &PrefetchStatus{
		ImageID:      job.ImageID,
		TotalEntries: totalEntries,
		Completed:    job.Index,
		Progress:     progress,
		StartTime:    job.StartTime,
		Elapsed:      time.Since(job.StartTime),
	}
}

func (p *Prefetcher) GetAllJobStatuses() []*PrefetchStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	statuses := make([]*PrefetchStatus, 0, len(p.activeJobs))
	for _, job := range p.activeJobs {
		if status := p.GetJobStatus(job.ImageID); status != nil {
			statuses = append(statuses, status)
		}
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].StartTime.Before(statuses[j].StartTime)
	})

	return statuses
}

func (p *Prefetcher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, job := range p.activeJobs {
		job.cancel()
	}

	p.activeJobs = make(map[string]*PrefetchJob)
	log.L.Info("prefetcher stopped")
}

type PrefetchStatus struct {
	ImageID      string
	TotalEntries int
	Completed    int
	Progress     float64
	StartTime    time.Time
	Elapsed      time.Duration
}
