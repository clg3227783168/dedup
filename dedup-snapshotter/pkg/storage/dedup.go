package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/log"
	"github.com/opencloudos/dedup-snapshotter/pkg/erofs"
	"github.com/opencloudos/dedup-snapshotter/pkg/fscache"
	"github.com/opencloudos/dedup-snapshotter/pkg/lazy"
	"github.com/opencloudos/dedup-snapshotter/pkg/memory"
)

const (
	ChunkSize = 4 * 1024 * 1024
)

type DedupStore struct {
	root         string
	chunksDir    string
	snapsDir     string
	imagesDir    string
	indexDB      *IndexDB
	chunkCache   sync.Map
	erofsBuilder *erofs.Builder
	mountManager *erofs.MountManager
	lazyLoader   *lazy.LazyLoader
	memDedup     *memory.MemoryDeduplicator
	dedupDaemon  *fscache.DedupDaemon
	useErofs     bool
	useFscache   bool
}

type ChunkInfo struct {
	Hash     string
	Size     int64
	RefCount int64
}

func NewDedupStore(root string) (*DedupStore, error) {
	return NewDedupStoreWithOptions(root, true, true)
}

func NewDedupStoreWithErofs(root string, useErofs bool) (*DedupStore, error) {
	return NewDedupStoreWithOptions(root, useErofs, false)
}

func NewDedupStoreWithOptions(root string, useErofs bool, useFscache bool) (*DedupStore, error) {
	chunksDir := filepath.Join(root, "chunks")
	snapsDir := filepath.Join(root, "snapshots")
	imagesDir := filepath.Join(root, "images")

	if err := os.MkdirAll(chunksDir, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(snapsDir, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(imagesDir, 0755); err != nil {
		return nil, err
	}

	indexDB, err := NewIndexDB(filepath.Join(root, "index.db"))
	if err != nil {
		return nil, err
	}

	store := &DedupStore{
		root:       root,
		chunksDir:  chunksDir,
		snapsDir:   snapsDir,
		imagesDir:  imagesDir,
		indexDB:    indexDB,
		useErofs:   useErofs,
		useFscache: useFscache,
	}

	if useErofs {
		builder, err := erofs.NewBuilder(root)
		if err != nil {
			return nil, fmt.Errorf("failed to create erofs builder: %w", err)
		}
		store.erofsBuilder = builder

		mountManager, err := erofs.NewMountManager(root)
		if err != nil {
			return nil, fmt.Errorf("failed to create mount manager: %w", err)
		}
		store.mountManager = mountManager

		if useFscache {
			dedupDaemon, err := fscache.NewDedupDaemon(root, "", 4)
			if err != nil {
				log.L.Warnf("failed to create dedupd daemon: %v", err)
			} else {
				store.dedupDaemon = dedupDaemon
				log.L.Info("dedupd daemon initialized for fscache support")
			}
		} else {
			lazyLoader, err := lazy.NewLazyLoader(root, "")
			if err != nil {
				return nil, fmt.Errorf("failed to create lazy loader: %w", err)
			}
			store.lazyLoader = lazyLoader
		}

		memDedup, err := memory.NewMemoryDeduplicator(root)
		if err != nil {
			return nil, fmt.Errorf("failed to create memory deduplicator: %w", err)
		}
		store.memDedup = memDedup

		if err := memDedup.EnableKSM(); err != nil {
			log.L.Warnf("failed to enable KSM: %v", err)
		}
	}

	return store, nil
}

func (d *DedupStore) DiskUsage(ctx context.Context, id string) (UsageInfo, error) {
	snapPath := filepath.Join(d.snapsDir, id)

	var size int64
	err := filepath.Walk(snapPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return UsageInfo{
		Inodes: 1,
		Size:   size,
	}, err
}

func (d *DedupStore) Prepare(ctx context.Context, id string, parents []string) error {
	snapPath := filepath.Join(d.snapsDir, id)
	if err := os.MkdirAll(snapPath, 0755); err != nil {
		return err
	}

	metadataPath := filepath.Join(snapPath, ".metadata")
	metadata := map[string]interface{}{
		"id":         id,
		"parents":    parents,
		"created_at": time.Now().Unix(),
		"status":     "active",
	}

	if err := d.writeMetadata(metadataPath, metadata); err != nil {
		log.L.WithError(err).Warnf("failed to write metadata for snapshot %s", id)
	}

	log.L.Debugf("prepared snapshot %s with parents %v", id, parents)
	return nil
}

func (d *DedupStore) Mounts(id string, parents []string) ([]mount.Mount, error) {
	if d.useErofs && d.mountManager != nil {
		return d.mountsWithErofs(id, parents)
	}
	return d.mountsWithOverlay(id, parents)
}

func (d *DedupStore) mountsWithErofs(id string, parents []string) ([]mount.Mount, error) {
	var lowerDirs []string

	for _, parent := range parents {
		imagePath := filepath.Join(d.imagesDir, parent+erofs.ErofsImageExt)
		if _, err := os.Stat(imagePath); err == nil {
			var mountPath string
			var err error

			if d.useFscache && d.dedupDaemon != nil {
				fsid := parent
				domain := "dedup-snapshotter"
				mountPath, err = d.mountManager.MountErofsWithFscache(parent, fsid, domain)
				if err != nil {
					log.L.Warnf("fscache mount failed, falling back to loop mount: %v", err)
					mountPath, err = d.mountManager.MountErofs(parent, imagePath)
				}
			} else {
				mountPath, err = d.mountManager.MountErofs(parent, imagePath)
			}

			if err != nil {
				return nil, fmt.Errorf("failed to mount erofs image %s: %w", parent, err)
			}
			lowerDirs = append(lowerDirs, mountPath)

			if d.memDedup != nil {
				go func(path string) {
					filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
						if err == nil && info.Mode().IsRegular() && info.Size() > 0 {
							d.memDedup.DeduplicateFile(p)
						}
						return nil
					})
				}(mountPath)
			}
		} else {
			parentPath := filepath.Join(d.snapsDir, parent, "fs")
			lowerDirs = append(lowerDirs, parentPath)
		}
	}

	snapPath := filepath.Join(d.snapsDir, id)
	workDir := filepath.Join(snapPath, "work")
	upperDir := filepath.Join(snapPath, "fs")

	return d.mountManager.CreateOverlayMounts(id, lowerDirs, upperDir, workDir)
}

func (d *DedupStore) mountsWithOverlay(id string, parents []string) ([]mount.Mount, error) {
	snapPath := filepath.Join(d.snapsDir, id)
	workDir := filepath.Join(snapPath, "work")
	upperDir := filepath.Join(snapPath, "fs")

	if err := os.MkdirAll(workDir, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		return nil, err
	}

	var lowerDirs []string
	for _, parent := range parents {
		parentPath := filepath.Join(d.snapsDir, parent, "fs")
		lowerDirs = append(lowerDirs, parentPath)
	}

	options := []string{
		fmt.Sprintf("workdir=%s", workDir),
		fmt.Sprintf("upperdir=%s", upperDir),
	}

	if len(lowerDirs) > 0 {
		lowerDir := ""
		for i := len(lowerDirs) - 1; i >= 0; i-- {
			if lowerDir == "" {
				lowerDir = lowerDirs[i]
			} else {
				lowerDir = lowerDirs[i] + ":" + lowerDir
			}
		}
		options = append(options, fmt.Sprintf("lowerdir=%s", lowerDir))
	}

	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}, nil
}

func (d *DedupStore) Remove(ctx context.Context, id string) error {
	if d.useErofs && d.mountManager != nil {
		if err := d.mountManager.Unmount(id); err != nil {
			log.L.WithError(err).Warnf("failed to unmount %s", id)
		}
	}

	snapPath := filepath.Join(d.snapsDir, id)
	return os.RemoveAll(snapPath)
}

func (d *DedupStore) BuildErofsImage(ctx context.Context, sourceDir, imageID string) error {
	if !d.useErofs || d.erofsBuilder == nil {
		return fmt.Errorf("erofs not enabled")
	}

	imagePath, err := d.erofsBuilder.BuildImage(ctx, sourceDir, imageID)
	if err != nil {
		return err
	}

	log.L.Infof("built erofs image for %s at %s", imageID, imagePath)
	return nil
}

func (d *DedupStore) Close() error {
	var errs []error

	if d.erofsBuilder != nil {
		if err := d.erofsBuilder.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if d.mountManager != nil {
		if err := d.mountManager.UnmountAll(); err != nil {
			errs = append(errs, err)
		}
	}

	if d.lazyLoader != nil {
		if err := d.lazyLoader.Cleanup(); err != nil {
			errs = append(errs, err)
		}
	}

	if d.dedupDaemon != nil {
		if err := d.dedupDaemon.Shutdown(context.Background()); err != nil {
			errs = append(errs, err)
		}
	}

	if d.memDedup != nil {
		if err := d.memDedup.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}

	return nil
}

func (d *DedupStore) StartPrefetch(ctx context.Context, imageID string, traceFile string) error {
	if !d.useFscache || d.dedupDaemon == nil {
		return fmt.Errorf("fscache not enabled")
	}

	return d.dedupDaemon.StartPrefetch(ctx, imageID, traceFile)
}

func (d *DedupStore) RegisterImageForFscache(ctx context.Context, imageID string, manifestPath string) error {
	if !d.useFscache || d.dedupDaemon == nil {
		return fmt.Errorf("fscache not enabled")
	}

	return d.dedupDaemon.RegisterImage(ctx, imageID, manifestPath)
}

func (d *DedupStore) WriteFile(ctx context.Context, path string, data io.Reader) error {
	chunks, err := d.chunkData(data)
	if err != nil {
		return err
	}

	for _, chunk := range chunks {
		if err := d.storeChunk(ctx, chunk); err != nil {
			return err
		}
	}

	return d.indexDB.IndexFile(path, chunks)
}

func (d *DedupStore) chunkData(data io.Reader) ([]ChunkInfo, error) {
	var chunks []ChunkInfo
	buf := make([]byte, ChunkSize)

	for {
		n, err := io.ReadFull(data, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		if n == 0 {
			break
		}

		hash := sha256.Sum256(buf[:n])
		hashStr := hex.EncodeToString(hash[:])

		chunks = append(chunks, ChunkInfo{
			Hash: hashStr,
			Size: int64(n),
		})

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	return chunks, nil
}

func (d *DedupStore) storeChunk(ctx context.Context, chunk ChunkInfo) error {
	chunkPath := filepath.Join(d.chunksDir, chunk.Hash)

	if _, err := os.Stat(chunkPath); err == nil {
		return d.indexDB.IncrementRefCount(chunk.Hash)
	}

	return nil
}

type UsageInfo struct {
	Inodes int64
	Size   int64
}

func (d *DedupStore) writeMetadata(path string, metadata map[string]interface{}) error {
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (d *DedupStore) readMetadata(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (d *DedupStore) VerifySnapshot(id string) error {
	snapPath := filepath.Join(d.snapsDir, id)
	metadataPath := filepath.Join(snapPath, ".metadata")

	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return fmt.Errorf("metadata missing for snapshot %s", id)
	}

	metadata, err := d.readMetadata(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	if metadata["id"] != id {
		return fmt.Errorf("metadata id mismatch: expected %s, got %v", id, metadata["id"])
	}

	fsPath := filepath.Join(snapPath, "fs")
	if _, err := os.Stat(fsPath); err != nil {
		return fmt.Errorf("fs directory missing: %w", err)
	}

	log.L.Debugf("verified snapshot %s metadata and filesystem", id)
	return nil
}

func (d *DedupStore) RecoverSnapshots(ctx context.Context) error {
	log.L.Info("starting snapshot recovery")

	entries, err := os.ReadDir(d.snapsDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshots directory: %w", err)
	}

	recoveredCount := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		id := entry.Name()
		if err := d.VerifySnapshot(id); err != nil {
			log.L.WithError(err).Warnf("snapshot %s verification failed, skipping", id)
			continue
		}

		recoveredCount++
	}

	log.L.Infof("recovered %d snapshots", recoveredCount)
	return nil
}

func (d *DedupStore) VerifyChunks(ctx context.Context) error {
	log.L.Info("verifying chunk files")

	entries, err := os.ReadDir(d.chunksDir)
	if err != nil {
		return fmt.Errorf("failed to read chunks directory: %w", err)
	}

	verifiedCount := 0
	missingCount := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		chunkHash := entry.Name()
		chunkPath := filepath.Join(d.chunksDir, chunkHash)

		info, err := os.Stat(chunkPath)
		if err != nil {
			missingCount++
			log.L.WithError(err).Warnf("chunk file %s missing or inaccessible", chunkHash)
			continue
		}

		if info.Size() == 0 {
			missingCount++
			log.L.Warnf("chunk file %s is empty", chunkHash)
			continue
		}

		verifiedCount++
	}

	log.L.Infof("chunk verification: %d verified, %d missing or invalid", verifiedCount, missingCount)
	return nil
}
