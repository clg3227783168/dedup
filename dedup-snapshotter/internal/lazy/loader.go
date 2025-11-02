package lazy

import (
	"context"
	"io"
	"sync"

	"github.com/containerd/log"
)

type Loader struct {
	mu      sync.RWMutex
	cache   map[string][]byte
	fetcher Fetcher
}

type Fetcher interface {
	Fetch(ctx context.Context, id string, offset, size int64) (io.ReadCloser, error)
}

func NewLoader(fetcher Fetcher) *Loader {
	return &Loader{
		cache:   make(map[string][]byte),
		fetcher: fetcher,
	}
}

func (l *Loader) Load(ctx context.Context, id string, offset, size int64) ([]byte, error) {
	cacheKey := getCacheKey(id, offset, size)

	l.mu.RLock()
	if data, ok := l.cache[cacheKey]; ok {
		l.mu.RUnlock()
		log.L.Debugf("cache hit for %s", cacheKey)
		return data, nil
	}
	l.mu.RUnlock()

	rc, err := l.fetcher.Fetch(ctx, id, offset, size)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	l.cache[cacheKey] = data
	l.mu.Unlock()

	return data, nil
}

func (l *Loader) Prefetch(ctx context.Context, ids []string) error {
	for _, id := range ids {
		go func(id string) {
			_, err := l.Load(ctx, id, 0, -1)
			if err != nil {
				log.L.WithError(err).Errorf("failed to prefetch %s", id)
			}
		}(id)
	}
	return nil
}

func getCacheKey(id string, offset, size int64) string {
	return id
}
