package memory

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/containerd/log"
)

type MemoryDeduplicator struct {
	root         string
	pageSize     int
	pageMu       sync.RWMutex
	pageMap      map[string]*PageInfo
	mergedPages  int64
	savedMemory  int64
	ksm          *KSMController
}

type PageInfo struct {
	Hash     string
	Size     int
	RefCount int64
	Addr     uintptr
	FilePath string
}

type KSMController struct {
	enabled    bool
	sysfsPath  string
	mu         sync.Mutex
}

func NewMemoryDeduplicator(root string) (*MemoryDeduplicator, error) {
	pageSize := os.Getpagesize()

	ksm, err := NewKSMController()
	if err != nil {
		log.L.Warnf("KSM not available: %v, memory dedup will use madvise only", err)
	}

	return &MemoryDeduplicator{
		root:     root,
		pageSize: pageSize,
		pageMap:  make(map[string]*PageInfo),
		ksm:      ksm,
	}, nil
}

func NewKSMController() (*KSMController, error) {
	sysfsPath := "/sys/kernel/mm/ksm"
	if _, err := os.Stat(sysfsPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("KSM not available in kernel")
	}

	return &KSMController{
		enabled:   false,
		sysfsPath: sysfsPath,
	}, nil
}

func (k *KSMController) Enable() error {
	if k == nil {
		return fmt.Errorf("KSM controller not initialized")
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	if err := os.WriteFile(k.sysfsPath+"/run", []byte("1"), 0644); err != nil {
		return fmt.Errorf("failed to enable KSM: %w", err)
	}

	k.enabled = true
	log.L.Info("KSM enabled for memory deduplication")
	return nil
}

func (k *KSMController) Disable() error {
	if k == nil {
		return nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	if err := os.WriteFile(k.sysfsPath+"/run", []byte("0"), 0644); err != nil {
		return fmt.Errorf("failed to disable KSM: %w", err)
	}

	k.enabled = false
	return nil
}

func (k *KSMController) GetStats() (*KSMStats, error) {
	if k == nil {
		return nil, fmt.Errorf("KSM controller not initialized")
	}

	stats := &KSMStats{}

	pagesSharing, err := readInt64FromFile(k.sysfsPath + "/pages_sharing")
	if err != nil {
		return nil, err
	}
	stats.PagesSharing = pagesSharing

	pagesShared, err := readInt64FromFile(k.sysfsPath + "/pages_shared")
	if err != nil {
		return nil, err
	}
	stats.PagesShared = pagesShared

	pagesUnshared, err := readInt64FromFile(k.sysfsPath + "/pages_unshared")
	if err != nil {
		return nil, err
	}
	stats.PagesUnshared = pagesUnshared

	pageSize := int64(os.Getpagesize())
	stats.SavedMemory = pagesSharing * pageSize

	return stats, nil
}

func readInt64FromFile(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	var val int64
	_, err = fmt.Sscanf(string(data), "%d", &val)
	return val, err
}

func (m *MemoryDeduplicator) DeduplicateFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	size := int(stat.Size())
	if size == 0 {
		return nil
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}
	defer syscall.Munmap(data)

	return m.processPages(data, filePath)
}

func (m *MemoryDeduplicator) processPages(data []byte, filePath string) error {
	numPages := (len(data) + m.pageSize - 1) / m.pageSize

	for i := 0; i < numPages; i++ {
		start := i * m.pageSize
		end := start + m.pageSize
		if end > len(data) {
			end = len(data)
		}

		page := data[start:end]
		if err := m.deduplicatePage(page, filePath); err != nil {
			log.L.WithError(err).Warnf("failed to deduplicate page %d of %s", i, filePath)
		}
	}

	if err := m.markMergeable(data); err != nil {
		return err
	}

	return nil
}

func (m *MemoryDeduplicator) deduplicatePage(page []byte, filePath string) error {
	hash := sha256.Sum256(page)
	hashStr := hex.EncodeToString(hash[:])

	m.pageMu.Lock()
	defer m.pageMu.Unlock()

	if existing, ok := m.pageMap[hashStr]; ok {
		existing.RefCount++
		m.mergedPages++
		m.savedMemory += int64(len(page))
		log.L.Debugf("deduplicated page %s, refcount=%d", hashStr, existing.RefCount)
		return nil
	}

	m.pageMap[hashStr] = &PageInfo{
		Hash:     hashStr,
		Size:     len(page),
		RefCount: 1,
		Addr:     uintptr(unsafe.Pointer(&page[0])),
		FilePath: filePath,
	}

	return nil
}

func (m *MemoryDeduplicator) markMergeable(data []byte) error {
	err := syscall.Madvise(data, syscall.MADV_MERGEABLE)
	if err != nil {
		return fmt.Errorf("madvise MADV_MERGEABLE failed: %w", err)
	}

	return nil
}

func (m *MemoryDeduplicator) EnableKSM() error {
	if m.ksm == nil {
		return fmt.Errorf("KSM not available")
	}
	return m.ksm.Enable()
}

func (m *MemoryDeduplicator) DisableKSM() error {
	if m.ksm == nil {
		return nil
	}
	return m.ksm.Disable()
}

func (m *MemoryDeduplicator) GetStats() (*DedupStats, error) {
	m.pageMu.RLock()
	uniquePages := int64(len(m.pageMap))
	mergedPages := m.mergedPages
	savedMemory := m.savedMemory
	m.pageMu.RUnlock()

	stats := &DedupStats{
		UniquePages: uniquePages,
		MergedPages: mergedPages,
		SavedMemory: savedMemory,
	}

	if m.ksm != nil && m.ksm.enabled {
		ksmStats, err := m.ksm.GetStats()
		if err == nil {
			stats.KSMStats = ksmStats
			stats.SavedMemory += ksmStats.SavedMemory
		}
	}

	return stats, nil
}

func (m *MemoryDeduplicator) Close() error {
	if m.ksm != nil {
		return m.ksm.Disable()
	}
	return nil
}

type DedupStats struct {
	UniquePages int64
	MergedPages int64
	SavedMemory int64
	KSMStats    *KSMStats
}

type KSMStats struct {
	PagesSharing  int64
	PagesShared   int64
	PagesUnshared int64
	SavedMemory   int64
}
